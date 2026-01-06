use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::listener::ListenerId;
use crate::node::{LocalNode, generate_object_id};
use crate::object::ObjectId;
use crate::sql::catalog::{Catalog, TableDescriptor};
use crate::sql::index::RefIndex;
use crate::sql::parser::{self, Condition, ConditionValue, Join, Projection, Select, SelectExpr, Statement};
use crate::sql::policy::{
    Policy, PolicyAction, PolicyError, PolicyExpr, PolicyValue, TablePolicies,
};
use crate::sql::query_graph::registry::{GraphRegistry, OutputCallback};
use crate::sql::query_graph::{
    DeltaBatch, GraphId, JoinGraphBuilder, Predicate, PriorState, QueryGraphBuilder, RowDelta,
};
use crate::sql::row::{Row, RowError, Value, decode_row, encode_row};
use crate::sql::schema::{ColumnType, SchemaError, TableSchema};
use crate::sql::table_rows::TableRows;
use crate::sql::types::{IndexKey, SchemaId};
use crate::storage::Environment;

/// Coerce a Value to match the expected ColumnType.
/// This is primarily used to convert String values to Ref(ObjectId) when
/// the column type is Ref, since the SQL parser parses all string literals
/// as Value::String (to avoid ambiguity with regular strings).
fn coerce_value(value: Value, ty: &ColumnType) -> Value {
    match (&value, ty) {
        // String to Ref coercion: parse the string as ObjectId
        (Value::String(s), ColumnType::Ref(_)) => {
            if let Ok(id) = s.parse::<ObjectId>() {
                Value::Ref(id)
            } else {
                value // Keep as string if not a valid ObjectId
            }
        }
        // No coercion needed
        _ => value,
    }
}

#[cfg(test)]
mod tests;

// ========== JOIN Support ==========

/// A row resulting from a JOIN, containing data from multiple tables.
#[derive(Clone)]
struct JoinedRow {
    /// Primary table name (for output row ID).
    primary_table: String,
    /// Table name → (schema, row_id, values)
    tables: HashMap<String, (TableSchema, ObjectId, Vec<Value>)>,
}

impl JoinedRow {
    /// Create a JoinedRow from a single table's row.
    fn from_single(table: &str, schema: &TableSchema, row: Row) -> Self {
        let mut tables = HashMap::new();
        tables.insert(table.to_string(), (schema.clone(), row.id, row.values));
        JoinedRow {
            primary_table: table.to_string(),
            tables,
        }
    }

    /// Add another table's row to this joined row.
    fn add_table(&mut self, table: &str, schema: &TableSchema, row: Row) {
        self.tables
            .insert(table.to_string(), (schema.clone(), row.id, row.values));
    }

    /// Get a column value by optional table qualifier and column name.
    /// If table is None, searches all tables (returns first match).
    fn get_column(&self, table: Option<&str>, column: &str) -> Option<&Value> {
        if column == "id" {
            // Special case: id is the row's object ID
            // Can't return reference to temporary Value, this is handled specially in matches_condition
            return None;
        }

        if let Some(table_name) = table {
            // Qualified column: look in specific table
            if let Some((schema, _, values)) = self.tables.get(table_name) {
                if let Some(idx) = schema.column_index(column) {
                    return values.get(idx);
                }
            }
            None
        } else {
            // Unqualified column: search all tables
            for (schema, _, values) in self.tables.values() {
                if let Some(idx) = schema.column_index(column) {
                    return values.get(idx);
                }
            }
            None
        }
    }

    /// Get the row ID for a specific table.
    fn get_row_id(&self, table: &str) -> Option<ObjectId> {
        self.tables.get(table).map(|(_, id, _)| *id)
    }

    /// Check if this joined row matches a WHERE condition.
    fn matches_condition(&self, cond: &Condition) -> bool {
        use crate::sql::parser::ConditionValue;

        let table = cond.column.table.as_deref();
        let column = &cond.column.column;

        // Get the right-hand side value (either literal or from another column)
        let rhs_value: Option<Value> = match &cond.right {
            ConditionValue::Literal(v) => Some(v.clone()),
            ConditionValue::Column(rhs_col) => {
                // Get value from referenced column
                if rhs_col.column == "id" {
                    let rhs_table = rhs_col.table.as_deref().unwrap_or(&self.primary_table);
                    self.get_row_id(rhs_table).map(Value::Ref)
                } else {
                    self.get_column(rhs_col.table.as_deref(), &rhs_col.column)
                        .cloned()
                }
            }
        };

        let rhs_value = match rhs_value {
            Some(v) => v,
            None => return false,
        };

        // Handle special "id" column on left side
        if column == "id" {
            // Coerce String to Ref for id comparison
            let coerced = coerce_value(rhs_value, &ColumnType::Ref("".to_string()));
            if let Value::Ref(target_id) = coerced {
                if let Some(table_name) = table {
                    if let Some(row_id) = self.get_row_id(table_name) {
                        return row_id == target_id;
                    }
                } else {
                    // Unqualified id - check primary table
                    if let Some(row_id) = self.get_row_id(&self.primary_table) {
                        return row_id == target_id;
                    }
                }
            }
            return false;
        }

        // Regular column - find the column type and coerce
        if let Some(table_name) = table {
            if let Some((schema, _, values)) = self.tables.get(table_name) {
                if let Some(idx) = schema.column_index(column) {
                    let coerced = coerce_value(rhs_value, &schema.columns[idx].ty);
                    return values.get(idx) == Some(&coerced);
                }
            }
        } else {
            // Unqualified column: search all tables
            for (schema, _, values) in self.tables.values() {
                if let Some(idx) = schema.column_index(column) {
                    let coerced = coerce_value(rhs_value.clone(), &schema.columns[idx].ty);
                    return values.get(idx) == Some(&coerced);
                }
            }
        }
        false
    }

    /// Convert to output Row with combined values from all tables.
    /// Uses primary table's row ID as the output row ID.
    fn to_output_row(self, projection: &Projection) -> Row {
        let row_id = self
            .tables
            .get(&self.primary_table)
            .map(|(_, id, _)| *id)
            .unwrap_or(ObjectId::default());

        let values = match projection {
            Projection::All => {
                // Combine all values from all tables (primary first, then joins in insertion order)
                let mut all_values = Vec::new();
                if let Some((_, _, values)) = self.tables.get(&self.primary_table) {
                    all_values.extend(values.iter().cloned());
                }
                for (table_name, (_, _, values)) in &self.tables {
                    if table_name != &self.primary_table {
                        all_values.extend(values.iter().cloned());
                    }
                }
                all_values
            }
            Projection::TableAll(table_name) => {
                // Only values from specified table
                if let Some((_, _, values)) = self.tables.get(table_name) {
                    values.clone()
                } else {
                    vec![]
                }
            }
            Projection::Columns(cols) => {
                // Specific columns
                cols.iter()
                    .filter_map(|qc| self.get_column(qc.table.as_deref(), &qc.column).cloned())
                    .collect()
            }
            Projection::Expressions(_) => {
                // Expressions are handled in execute_select, not here
                // This path should not be reached for JoinedRow
                panic!("Projection::Expressions should be handled in execute_select");
            }
        };

        Row::new(row_id, values)
    }
}

// ========== Incremental Query (Query Graph based) ==========

/// A handle to an incremental query graph.
///
/// Uses incremental computation - only processing the delta from each
/// change and propagating it through the computation graph.
///
/// The query is automatically cleaned up when this handle is dropped.
#[derive(Clone)]
pub struct IncrementalQuery {
    /// The graph ID in the registry.
    graph_id: GraphId,
    /// Reference to database state for output retrieval.
    db_state: Arc<DatabaseState>,
}

impl std::fmt::Debug for IncrementalQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncrementalQuery")
            .field("graph_id", &self.graph_id)
            .finish()
    }
}

impl IncrementalQuery {
    /// Get the current query output rows.
    pub fn rows(&self) -> Vec<Row> {
        self.db_state
            .graph_registry
            .get_output(self.graph_id, &self.db_state)
            .unwrap_or_default()
    }

    /// Subscribe to query output changes with a delta callback.
    ///
    /// The callback receives a `DeltaBatch` describing which rows were
    /// added, removed, or updated since the last notification.
    ///
    /// **Important**: The callback is immediately called with the current state
    /// as a batch of "Added" deltas, so subscribers always see the initial data.
    ///
    /// Returns a `ListenerId` that can be used to unsubscribe.
    pub fn subscribe_delta(&self, callback: OutputCallback) -> Option<ListenerId> {
        // Get current state and send as initial "Added" deltas
        // Always call the callback, even if empty, so subscribers know initial load completed
        let initial_rows = self.rows();
        let initial_deltas: DeltaBatch = initial_rows
            .into_iter()
            .map(RowDelta::Added)
            .collect();
        callback(&initial_deltas);

        // Subscribe for future changes
        self.db_state
            .graph_registry
            .subscribe(self.graph_id, callback)
    }

    /// Subscribe to query output changes with a full rows callback.
    ///
    /// This is a convenience wrapper that provides the full current row set
    /// on each change, rather than the delta. Less efficient but simpler.
    #[cfg(not(feature = "wasm"))]
    pub fn subscribe(
        &self,
        callback: impl Fn(Vec<Row>) + Send + Sync + 'static,
    ) -> Option<ListenerId> {
        let db_state = self.db_state.clone();
        let graph_id = self.graph_id;

        self.db_state.graph_registry.subscribe(
            self.graph_id,
            Box::new(move |_delta| {
                let rows = db_state
                    .graph_registry
                    .get_output(graph_id, &db_state)
                    .unwrap_or_default();
                callback(rows);
            }),
        )
    }

    /// Subscribe to query output changes with a full rows callback (WASM version).
    ///
    /// This is a convenience wrapper that provides the full current row set
    /// on each change, rather than the delta. Less efficient but simpler.
    #[cfg(feature = "wasm")]
    pub fn subscribe(&self, callback: impl Fn(Vec<Row>) + 'static) -> Option<ListenerId> {
        let db_state = self.db_state.clone();
        let graph_id = self.graph_id;

        self.db_state.graph_registry.subscribe(
            self.graph_id,
            Box::new(move |_delta| {
                let rows = db_state
                    .graph_registry
                    .get_output(graph_id, &db_state)
                    .unwrap_or_default();
                callback(rows);
            }),
        )
    }

    /// Unsubscribe a callback.
    pub fn unsubscribe(&self, listener_id: ListenerId) -> bool {
        self.db_state
            .graph_registry
            .unsubscribe(self.graph_id, listener_id)
    }

    /// Get the graph ID (for testing/debugging).
    pub fn graph_id(&self) -> GraphId {
        self.graph_id
    }

    /// Get a text diagram of the query graph.
    ///
    /// Returns a human-readable representation of the computation DAG
    /// showing node types, predicates, and cache states.
    pub fn diagram(&self) -> String {
        self.db_state
            .graph_registry
            .get_diagram(self.graph_id)
            .unwrap_or_else(|| "Graph not found".to_string())
    }
}

impl Drop for IncrementalQuery {
    fn drop(&mut self) {
        // Only unregister if this is the last reference
        // Note: Clone creates a new Arc reference, so this is safe
        if Arc::strong_count(&self.db_state) > 1 {
            // Still being used elsewhere, don't unregister
            // This is a simplification - in production we'd want reference counting on the query itself
        }
        // For now, we don't auto-unregister to allow cloning queries
        // TODO: Add proper reference counting for query cleanup
    }
}

/// Shared database state that can be held by queries for re-evaluation.
/// This is the core data that queries need access to.
pub struct DatabaseState {
    node: LocalNode,
    /// Object ID for the database catalog.
    catalog_object_id: ObjectId,
    /// Map from table name to schema object ID.
    tables: RwLock<HashMap<String, SchemaId>>,
    /// Cached schemas by ID.
    schemas: RwLock<HashMap<SchemaId, TableSchema>>,
    /// Map from table name to table rows object ID.
    table_rows_objects: RwLock<HashMap<String, ObjectId>>,
    /// Map from table name to table descriptor object ID.
    descriptor_objects: RwLock<HashMap<String, ObjectId>>,
    /// Map from row object ID to its table name.
    row_table: RwLock<HashMap<ObjectId, String>>,
    /// Reference index objects: (source_table, source_column) -> object ID.
    index_objects: RwLock<HashMap<IndexKey, ObjectId>>,
    /// Policies per table.
    policies: RwLock<HashMap<String, TablePolicies>>,
    /// Registry for incremental QueryGraph instances.
    graph_registry: GraphRegistry,
}

impl std::fmt::Debug for DatabaseState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseState")
            .field(
                "tables",
                &self.tables.read().unwrap().keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl DatabaseState {
    fn new(env: Arc<dyn Environment>) -> Self {
        let node = LocalNode::new(env);

        // Create catalog object
        let catalog_object_id = node.create_object("catalog");

        // Initialize empty catalog
        let empty_catalog = Catalog::new();
        node.write(
            catalog_object_id,
            "main",
            &empty_catalog.to_bytes(),
            "system",
            timestamp_now(),
        )
        .expect("failed to initialize catalog");

        DatabaseState {
            node,
            catalog_object_id,
            tables: RwLock::new(HashMap::new()),
            schemas: RwLock::new(HashMap::new()),
            table_rows_objects: RwLock::new(HashMap::new()),
            descriptor_objects: RwLock::new(HashMap::new()),
            row_table: RwLock::new(HashMap::new()),
            index_objects: RwLock::new(HashMap::new()),
            policies: RwLock::new(HashMap::new()),
            graph_registry: GraphRegistry::new(),
        }
    }

    fn in_memory() -> Self {
        let node = LocalNode::in_memory();

        // Create catalog object
        let catalog_object_id = node.create_object("catalog");

        // Initialize empty catalog
        let empty_catalog = Catalog::new();
        node.write(
            catalog_object_id,
            "main",
            &empty_catalog.to_bytes(),
            "system",
            timestamp_now(),
        )
        .expect("failed to initialize catalog");

        DatabaseState {
            node,
            catalog_object_id,
            tables: RwLock::new(HashMap::new()),
            schemas: RwLock::new(HashMap::new()),
            table_rows_objects: RwLock::new(HashMap::new()),
            descriptor_objects: RwLock::new(HashMap::new()),
            row_table: RwLock::new(HashMap::new()),
            index_objects: RwLock::new(HashMap::new()),
            policies: RwLock::new(HashMap::new()),
            graph_registry: GraphRegistry::new(),
        }
    }

    /// Get a table schema by name.
    fn get_schema(&self, table: &str) -> Option<TableSchema> {
        let tables = self.tables.read().unwrap();
        let schema_id = tables.get(table)?;
        let schemas = self.schemas.read().unwrap();
        schemas.get(schema_id).cloned()
    }

    /// Read all rows from a table.
    pub fn read_all_rows(&self, table: &str) -> Vec<Row> {
        let schema = match self.get_schema(table) {
            Some(s) => s,
            None => return vec![],
        };

        let row_ids: Vec<ObjectId> = {
            let table_rows_objects = self.table_rows_objects.read().unwrap();
            if let Some(rows_id) = table_rows_objects.get(table) {
                if let Ok(Some(data)) = self.node.read(*rows_id, "main") {
                    if !data.is_empty() {
                        if let Ok(table_rows) = TableRows::from_bytes(&data) {
                            table_rows.into_vec()
                        } else {
                            vec![]
                        }
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                }
            } else {
                return vec![];
            }
        };

        let mut rows = Vec::new();
        for row_id in row_ids {
            let data = match self.node.read(row_id, "main") {
                Ok(Some(data)) if !data.is_empty() => data,
                _ => continue,
            };

            let values = match decode_row(&data, &schema) {
                Ok(v) => v,
                Err(_) => continue,
            };

            rows.push(Row::new(row_id, values));
        }

        rows
    }

    /// Get a single row by ID.
    pub fn get_row(&self, table: &str, id: ObjectId) -> Option<Row> {
        let schema = self.get_schema(table)?;

        // Check if row belongs to this table
        {
            let row_table = self.row_table.read().unwrap();
            match row_table.get(&id) {
                Some(t) if t == table => {}
                _ => return None,
            }
        }

        let data = match self.node.read(id, "main") {
            Ok(Some(data)) if !data.is_empty() => data,
            _ => return None,
        };

        let values = match decode_row(&data, &schema) {
            Ok(v) => v,
            Err(_) => return None,
        };

        Some(Row::new(id, values))
    }

    /// Find all rows where `column` references `target_id`.
    ///
    /// This is used by JOIN queries to find all left rows referencing a right row.
    pub fn find_referencing(&self, table: &str, column: &str, target_id: ObjectId) -> Vec<Row> {
        self.select_where(table, column, &Value::Ref(target_id))
    }

    /// Find rows matching a column = value condition.
    fn select_where(&self, table: &str, column: &str, value: &Value) -> Vec<Row> {
        let schema = match self.get_schema(table) {
            Some(s) => s,
            None => return vec![],
        };

        // Special case: id column (implicit Ref type)
        if column == "id" {
            // Coerce String to Ref for id comparison
            let coerced = coerce_value(value.clone(), &ColumnType::Ref("".to_string()));
            if let Value::Ref(id) = coerced {
                return match self.get_row(table, id) {
                    Some(row) => vec![row],
                    None => vec![],
                };
            }
            return vec![];
        }

        let col_idx = match schema.column_index(column) {
            Some(idx) => idx,
            None => return vec![],
        };

        // Coerce the value to match the column type
        let coerced = coerce_value(value.clone(), &schema.columns[col_idx].ty);

        self.read_all_rows(table)
            .into_iter()
            .filter(|row| row.values.get(col_idx) == Some(&coerced))
            .collect()
    }

    /// Execute a SELECT statement and return the resulting rows.
    /// This is the shared implementation used by both reactive queries and direct execution.
    fn execute_select(&self, select: &Select) -> Vec<Row> {
        let primary_table = &select.from.table;
        let table_alias = select.from.alias.as_deref();

        // Get primary table schema
        let primary_schema = match self.get_schema(primary_table) {
            Some(s) => s,
            None => return vec![],
        };

        // Read all rows from primary table
        let primary_rows = self.read_all_rows(primary_table);

        let result = if select.from.joins.is_empty() {
            // Simple case: no JOINs
            let filtered: Vec<Row> = primary_rows
                .into_iter()
                .filter(|row| {
                    Self::matches_where_simple(&select.where_clause, &row.values, &primary_schema)
                })
                .collect();

            // Check if we need to evaluate expressions (for ARRAY subqueries)
            match &select.projection {
                Projection::Expressions(exprs) => {
                    // Evaluate each expression for each row
                    filtered
                        .into_iter()
                        .map(|row| {
                            let values = self.evaluate_projection_exprs(
                                exprs,
                                &row,
                                primary_table,
                                table_alias,
                                &primary_schema,
                            );
                            Row::new(row.id, values)
                        })
                        .collect()
                }
                _ => filtered,
            }
        } else {
            // JOIN case: build joined rows
            let mut joined_rows: Vec<JoinedRow> = primary_rows
                .into_iter()
                .map(|row| JoinedRow::from_single(primary_table, &primary_schema, row))
                .collect();

            // Apply each join
            for join in &select.from.joins {
                joined_rows = self.apply_join(joined_rows, join);
            }

            // Apply WHERE filtering on joined results
            for cond in &select.where_clause {
                joined_rows.retain(|jr| jr.matches_condition(cond));
            }

            // Apply projection and convert to output
            joined_rows
                .into_iter()
                .map(|jr| jr.to_output_row(&select.projection))
                .collect()
        };

        // Apply LIMIT and OFFSET
        Self::apply_limit_offset(result, select.limit, select.offset)
    }

    /// Apply LIMIT and OFFSET to a result set.
    fn apply_limit_offset(rows: Vec<Row>, limit: Option<u64>, offset: Option<u64>) -> Vec<Row> {
        let offset = offset.unwrap_or(0) as usize;
        let rows: Vec<Row> = rows.into_iter().skip(offset).collect();
        match limit {
            Some(n) => rows.into_iter().take(n as usize).collect(),
            None => rows,
        }
    }

    /// Evaluate projection expressions for a single outer row.
    fn evaluate_projection_exprs(
        &self,
        exprs: &[SelectExpr],
        outer_row: &Row,
        outer_table: &str,
        outer_alias: Option<&str>,
        outer_schema: &TableSchema,
    ) -> Vec<Value> {
        exprs
            .iter()
            .map(|expr| {
                self.evaluate_select_expr(expr, outer_row, outer_table, outer_alias, outer_schema)
            })
            .collect()
    }

    /// Evaluate a single SELECT expression.
    fn evaluate_select_expr(
        &self,
        expr: &SelectExpr,
        outer_row: &Row,
        outer_table: &str,
        outer_alias: Option<&str>,
        outer_schema: &TableSchema,
    ) -> Value {
        match expr {
            SelectExpr::Column(qc) => {
                // Check if this is a reference to the outer table alias (composite row)
                if qc.table.is_none() {
                    // Bare identifier - check if it matches outer table alias
                    if let Some(alias) = outer_alias {
                        if qc.column == alias {
                            // Return the whole row as Value::Row
                            return Value::Row(Box::new(outer_row.clone()));
                        }
                    }
                    // Also check if it matches the table name
                    if qc.column == outer_table {
                        return Value::Row(Box::new(outer_row.clone()));
                    }
                }

                // Regular column reference
                if qc.column == "id" {
                    Value::Ref(outer_row.id)
                } else if let Some(idx) = outer_schema.column_index(&qc.column) {
                    outer_row.values.get(idx).cloned().unwrap_or(Value::NullableNone)
                } else {
                    Value::NullableNone
                }
            }

            SelectExpr::TableRow(alias) => {
                // Direct table row reference - return the whole row
                if outer_alias == Some(alias.as_str()) || alias == outer_table {
                    Value::Row(Box::new(outer_row.clone()))
                } else {
                    Value::NullableNone
                }
            }

            SelectExpr::ArraySubquery(subquery) => {
                // Execute subquery with outer row context
                self.execute_array_subquery(subquery, outer_row, outer_table, outer_alias, outer_schema)
            }

            SelectExpr::Aliased { expr, .. } => {
                // Alias doesn't affect the value, just evaluate the inner expression
                self.evaluate_select_expr(expr, outer_row, outer_table, outer_alias, outer_schema)
            }
        }
    }

    /// Execute an ARRAY subquery with outer row context.
    fn execute_array_subquery(
        &self,
        subquery: &Select,
        outer_row: &Row,
        outer_table: &str,
        outer_alias: Option<&str>,
        outer_schema: &TableSchema,
    ) -> Value {
        let inner_table = &subquery.from.table;
        let inner_alias = subquery.from.alias.as_deref();

        let inner_schema = match self.get_schema(inner_table) {
            Some(s) => s,
            None => return Value::Array(vec![]),
        };

        // Read all rows from inner table
        let inner_rows = self.read_all_rows(inner_table);

        // Filter by WHERE clause, resolving outer references
        let filtered: Vec<Row> = inner_rows
            .into_iter()
            .filter(|inner_row| {
                self.matches_where_with_outer(
                    &subquery.where_clause,
                    inner_row,
                    inner_table,
                    inner_alias,
                    &inner_schema,
                    outer_row,
                    outer_table,
                    outer_alias,
                    outer_schema,
                )
            })
            .collect();

        // Apply projection to get the values
        let array_values: Vec<Value> = match &subquery.projection {
            Projection::All => {
                // SELECT * - return all values as Row
                filtered
                    .into_iter()
                    .map(|row| Value::Row(Box::new(row)))
                    .collect()
            }
            Projection::Expressions(exprs) => {
                // Check if it's just the table alias (returns whole row)
                if exprs.len() == 1 {
                    if let SelectExpr::Column(qc) = &exprs[0] {
                        if qc.table.is_none() {
                            let name = &qc.column;
                            if inner_alias == Some(name.as_str()) || name == inner_table {
                                // Table alias - return whole rows
                                return Value::Array(
                                    filtered
                                        .into_iter()
                                        .map(|row| Value::Row(Box::new(row)))
                                        .collect(),
                                );
                            }
                        }
                    }
                }

                // Evaluate expressions for each row
                filtered
                    .into_iter()
                    .map(|inner_row| {
                        let values = self.evaluate_projection_exprs(
                            exprs,
                            &inner_row,
                            inner_table,
                            inner_alias,
                            &inner_schema,
                        );
                        if values.len() == 1 {
                            // Single column - return the value directly
                            values.into_iter().next().unwrap_or(Value::NullableNone)
                        } else {
                            // Multiple columns - wrap in Row
                            Value::Row(Box::new(Row::new(inner_row.id, values)))
                        }
                    })
                    .collect()
            }
            Projection::TableAll(table) => {
                if table == inner_table || inner_alias == Some(table.as_str()) {
                    filtered
                        .into_iter()
                        .map(|row| Value::Row(Box::new(row)))
                        .collect()
                } else {
                    vec![]
                }
            }
            Projection::Columns(cols) => {
                filtered
                    .into_iter()
                    .map(|inner_row| {
                        let values: Vec<Value> = cols
                            .iter()
                            .filter_map(|qc| {
                                if qc.column == "id" {
                                    Some(Value::Ref(inner_row.id))
                                } else {
                                    inner_schema
                                        .column_index(&qc.column)
                                        .and_then(|idx| inner_row.values.get(idx).cloned())
                                }
                            })
                            .collect();
                        if values.len() == 1 {
                            values.into_iter().next().unwrap_or(Value::NullableNone)
                        } else {
                            Value::Row(Box::new(Row::new(inner_row.id, values)))
                        }
                    })
                    .collect()
            }
        };

        Value::Array(array_values)
    }

    /// Check if a row matches WHERE conditions, resolving references to outer row.
    #[allow(clippy::too_many_arguments)]
    fn matches_where_with_outer(
        &self,
        conditions: &[Condition],
        inner_row: &Row,
        inner_table: &str,
        inner_alias: Option<&str>,
        inner_schema: &TableSchema,
        outer_row: &Row,
        outer_table: &str,
        outer_alias: Option<&str>,
        outer_schema: &TableSchema,
    ) -> bool {
        for cond in conditions {
            let lhs_table = cond.column.table.as_deref();
            let lhs_col = &cond.column.column;

            // Resolve left-hand side value
            let lhs_value = self.resolve_column_value(
                lhs_table,
                lhs_col,
                inner_row,
                inner_table,
                inner_alias,
                inner_schema,
                outer_row,
                outer_table,
                outer_alias,
                outer_schema,
            );

            // Resolve right-hand side value
            let rhs_value = match &cond.right {
                ConditionValue::Literal(v) => Some(v.clone()),
                ConditionValue::Column(rhs_col) => self.resolve_column_value(
                    rhs_col.table.as_deref(),
                    &rhs_col.column,
                    inner_row,
                    inner_table,
                    inner_alias,
                    inner_schema,
                    outer_row,
                    outer_table,
                    outer_alias,
                    outer_schema,
                ),
            };

            // Compare values
            match (lhs_value, rhs_value) {
                (Some(lhs), Some(rhs)) => {
                    if lhs != rhs {
                        return false;
                    }
                }
                _ => return false,
            }
        }
        true
    }

    /// Resolve a column reference, looking in both inner and outer contexts.
    #[allow(clippy::too_many_arguments)]
    fn resolve_column_value(
        &self,
        table_ref: Option<&str>,
        column: &str,
        inner_row: &Row,
        inner_table: &str,
        inner_alias: Option<&str>,
        inner_schema: &TableSchema,
        outer_row: &Row,
        outer_table: &str,
        outer_alias: Option<&str>,
        outer_schema: &TableSchema,
    ) -> Option<Value> {
        // Check if this references the inner table
        let is_inner = match table_ref {
            Some(t) => t == inner_table || inner_alias == Some(t),
            None => {
                // Unqualified - check inner first
                column == "id" || inner_schema.column_index(column).is_some()
            }
        };

        if is_inner {
            if column == "id" {
                return Some(Value::Ref(inner_row.id));
            }
            if let Some(idx) = inner_schema.column_index(column) {
                return inner_row.values.get(idx).cloned();
            }
        }

        // Check if this references the outer table
        let is_outer = match table_ref {
            Some(t) => t == outer_table || outer_alias == Some(t),
            None => {
                // Unqualified - check outer if not found in inner
                column == "id" || outer_schema.column_index(column).is_some()
            }
        };

        if is_outer {
            if column == "id" {
                return Some(Value::Ref(outer_row.id));
            }
            if let Some(idx) = outer_schema.column_index(column) {
                return outer_row.values.get(idx).cloned();
            }
        }

        None
    }

    /// Apply a single JOIN to a set of joined rows.
    fn apply_join(&self, rows: Vec<JoinedRow>, join: &Join) -> Vec<JoinedRow> {
        let join_schema = match self.get_schema(&join.table) {
            Some(s) => s,
            None => return vec![],
        };

        let mut result = Vec::new();

        for jr in rows {
            let left_table = join.on.left.table.as_deref();
            let left_column = &join.on.left.column;
            let right_table = join.on.right.table.as_deref();
            let right_column = &join.on.right.column;

            // Determine which side references the join table
            let (lookup_value, join_column) = if right_table == Some(join.table.as_str())
                || (right_table.is_none() && join_schema.column_index(right_column).is_some())
            {
                // Right side is from join table
                let left_val = if left_column == "id" {
                    let table_name = left_table.unwrap_or(&jr.primary_table);
                    jr.get_row_id(table_name).map(Value::Ref)
                } else {
                    jr.get_column(left_table, left_column).cloned()
                };
                (left_val, right_column.as_str())
            } else {
                // Left side is from join table
                let right_val = if right_column == "id" {
                    let table_name = right_table.unwrap_or(&jr.primary_table);
                    jr.get_row_id(table_name).map(Value::Ref)
                } else {
                    jr.get_column(right_table, right_column).cloned()
                };
                (right_val, left_column.as_str())
            };

            let lookup_value = match lookup_value {
                Some(v) => v,
                None => continue,
            };

            // Find matching rows from the join table
            let matching = if join_column == "id" {
                if let Value::Ref(id) = &lookup_value {
                    match self.get_row(&join.table, *id) {
                        Some(row) => vec![row],
                        None => vec![],
                    }
                } else {
                    vec![]
                }
            } else {
                self.select_where(&join.table, join_column, &lookup_value)
            };

            // Produce cartesian product of matches (inner join)
            for matched_row in matching {
                let mut new_jr = jr.clone();
                new_jr.add_table(&join.table, &join_schema, matched_row);
                result.push(new_jr);
            }
        }

        result
    }

    /// Simple WHERE matching for non-JOIN queries.
    fn matches_where_simple(
        where_clause: &[Condition],
        values: &[Value],
        schema: &TableSchema,
    ) -> bool {
        for cond in where_clause {
            let col_idx = match schema.column_index(&cond.column.column) {
                Some(idx) => idx,
                None => return false,
            };
            // Only handle literal values in simple WHERE matching
            // Column references are handled in JoinedRow::matches_condition
            let value = match cond.value() {
                Some(v) => v.clone(),
                None => return false, // Column references not supported in simple matches
            };
            // Coerce the condition value to match the column type
            let coerced = coerce_value(value, &schema.columns[col_idx].ty);
            if values[col_idx] != coerced {
                return false;
            }
        }
        true
    }
}

/// Database providing SQL operations on top of LocalNode.
///
/// The Database uses shared state internally so that reactive queries
/// can hold references to the same data and auto-update when changes occur.
pub struct Database {
    /// Shared database state.
    state: Arc<DatabaseState>,
}

/// Result of executing a SQL statement.
#[derive(Debug, Clone)]
pub enum ExecuteResult {
    /// CREATE TABLE - returns schema ID
    Created(SchemaId),
    /// CREATE POLICY - returns table name and action
    PolicyCreated { table: String, action: PolicyAction },
    /// INSERT - returns new row ID
    Inserted(ObjectId),
    /// UPDATE - returns number of rows affected
    Updated(usize),
    /// DELETE - returns number of rows affected
    Deleted(usize),
    /// SELECT - returns matching rows
    Selected(Vec<Row>),
}

/// Information about an INHERITS clause for JOIN expansion.
///
/// Used internally when flattening INHERITS policies into JOIN predicates
/// for incremental query graphs.
struct InheritsInfo {
    /// The Ref column in the source table (e.g., "folder_id")
    ref_column: String,
    /// The target table being referenced (e.g., "folders")
    target_table: String,
    /// The flattened predicate from the target table's policy
    target_predicate: Option<PolicyExpr>,
    /// Any additional predicates from AND clauses in the source policy
    additional_predicates: Vec<PolicyExpr>,
    /// Whether this is a self-referential INHERITS (target_table == source_table)
    is_self_referential: bool,
    /// The base predicate for self-referential INHERITS (the OR sibling of INHERITS)
    base_predicate: Option<PolicyExpr>,
}

/// A hop in an INHERITS chain.
#[derive(Clone)]
struct ChainHop {
    /// The Ref column in source table
    ref_column: String,
    /// The target table being referenced
    target_table: String,
    /// Optional base predicate at this hop (from OR sibling of INHERITS)
    /// When present, a row matches if it satisfies this predicate OR continues via INHERITS.
    base_predicate: Option<Predicate>,
}

/// A resolved INHERITS chain from source to terminal table.
struct InheritsChain {
    /// The hops in the chain (source→target for each)
    hops: Vec<ChainHop>,
    /// The terminal predicate from the last table (non-INHERITS)
    terminal_predicate: Option<Predicate>,
    /// The table that has the terminal predicate
    terminal_table: String,
}

/// Database errors.
#[derive(Debug, Clone)]
pub enum DatabaseError {
    /// Table already exists.
    TableExists(String),
    /// Table not found.
    TableNotFound(String),
    /// Row not found.
    RowNotFound(ObjectId),
    /// Column not found.
    ColumnNotFound(String),
    /// Schema error.
    Schema(SchemaError),
    /// Row encoding error.
    Row(RowError),
    /// Parse error.
    Parse(parser::ParseError),
    /// Column count mismatch.
    ColumnMismatch { expected: usize, got: usize },
    /// Type mismatch.
    TypeMismatch {
        column: String,
        expected: String,
        got: String,
    },
    /// Missing required column in INSERT.
    MissingColumn(String),
    /// Storage error.
    Storage(String),
    /// Invalid reference: target row doesn't exist.
    InvalidReference {
        column: String,
        target_table: String,
        target_id: ObjectId,
    },
    /// Column is not a reference type.
    NotAReference(String),
    /// Policy error.
    Policy(PolicyError),
    /// Policy denied the operation.
    PolicyDenied {
        action: PolicyAction,
        reason: String,
    },
}

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatabaseError::TableExists(name) => write!(f, "table '{}' already exists", name),
            DatabaseError::TableNotFound(name) => write!(f, "table '{}' not found", name),
            DatabaseError::RowNotFound(id) => write!(f, "row {} not found", id),
            DatabaseError::ColumnNotFound(name) => write!(f, "column '{}' not found", name),
            DatabaseError::Schema(e) => write!(f, "schema error: {}", e),
            DatabaseError::Row(e) => write!(f, "row error: {}", e),
            DatabaseError::Parse(e) => write!(f, "parse error: {}", e),
            DatabaseError::ColumnMismatch { expected, got } => {
                write!(
                    f,
                    "column count mismatch: expected {}, got {}",
                    expected, got
                )
            }
            DatabaseError::TypeMismatch {
                column,
                expected,
                got,
            } => {
                write!(
                    f,
                    "type mismatch for '{}': expected {}, got {}",
                    column, expected, got
                )
            }
            DatabaseError::MissingColumn(name) => write!(f, "missing required column: {}", name),
            DatabaseError::Storage(e) => write!(f, "storage error: {}", e),
            DatabaseError::InvalidReference {
                column,
                target_table,
                target_id,
            } => {
                write!(
                    f,
                    "invalid reference in '{}': row {} not found in table '{}'",
                    column, target_id, target_table
                )
            }
            DatabaseError::NotAReference(name) => write!(f, "column '{}' is not a reference", name),
            DatabaseError::Policy(e) => write!(f, "policy error: {}", e),
            DatabaseError::PolicyDenied { action, reason } => {
                write!(f, "{} denied: {}", action, reason)
            }
        }
    }
}

impl std::error::Error for DatabaseError {}

impl From<SchemaError> for DatabaseError {
    fn from(e: SchemaError) -> Self {
        DatabaseError::Schema(e)
    }
}

impl From<RowError> for DatabaseError {
    fn from(e: RowError) -> Self {
        DatabaseError::Row(e)
    }
}

impl From<PolicyError> for DatabaseError {
    fn from(e: PolicyError) -> Self {
        DatabaseError::Policy(e)
    }
}

impl From<parser::ParseError> for DatabaseError {
    fn from(e: parser::ParseError) -> Self {
        DatabaseError::Parse(e)
    }
}

// ========== Policy Lookup Traits ==========

use crate::sql::policy::{PolicyLookup, RowLookup};

impl RowLookup for Database {
    fn get_row(&self, table: &str, id: ObjectId) -> Option<Row> {
        self.get(table, id).ok().flatten()
    }

    fn get_schema(&self, table: &str) -> Option<TableSchema> {
        self.get_table(table)
    }
}

impl PolicyLookup for Database {
    fn get_policies(&self, table: &str) -> Option<TablePolicies> {
        let policies = self.state.policies.read().unwrap();
        policies.get(table).cloned()
    }
}

/// Result of finding a join column - indicates which table has the Ref
enum JoinDirection {
    /// Left table has Ref column pointing to right table (normal case)
    LeftToRight(String),
    /// Right table has Ref column pointing to left table (reverse join)
    RightToLeft(String),
}

/// Information about how to chain join a new table.
enum ChainJoinInfo {
    /// Forward join: existing table has ref column pointing to new table.
    /// chain_join(source_table.ref_column = target_table.id)
    Forward {
        source_table: String,
        ref_column: String,
    },
    /// Reverse join: new table has ref column pointing to existing table.
    /// reverse_chain_join(target_table.ref_column = existing_table.id)
    Reverse {
        existing_table: String,
        ref_column: String,
    },
}

impl Database {
    /// Create a new database with the given environment.
    pub fn new(env: Arc<dyn Environment>) -> Self {
        Database {
            state: Arc::new(DatabaseState::new(env)),
        }
    }

    /// Create a new in-memory database (for testing).
    pub fn in_memory() -> Self {
        Database {
            state: Arc::new(DatabaseState::in_memory()),
        }
    }

    /// Restore database from an existing environment with a known catalog object ID.
    ///
    /// This method loads the catalog and all table descriptors from the environment,
    /// restoring the database to its previous state.
    pub fn from_env(env: Arc<dyn Environment>, catalog_object_id: ObjectId) -> Result<Self, DatabaseError> {
        let node = LocalNode::new(env);

        // Load catalog object from Environment
        node.load_object(catalog_object_id, "catalog", "main")
            .ok_or_else(|| DatabaseError::Storage("catalog not found in environment".to_string()))?;

        // Read catalog content
        let catalog_bytes = node
            .read(catalog_object_id, "main")
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?
            .ok_or_else(|| DatabaseError::Storage("catalog content not found".to_string()))?;

        let catalog = Catalog::from_bytes(&catalog_bytes)
            .map_err(|e| DatabaseError::Storage(format!("catalog parse error: {}", e)))?;

        // Initialize state maps
        let mut tables = HashMap::new();
        let mut schemas = HashMap::new();
        let mut table_rows_objects = HashMap::new();
        let mut descriptor_objects = HashMap::new();
        let mut row_table = HashMap::new();
        let mut index_objects = HashMap::new();
        let mut policies = HashMap::new();

        // Restore each table from its descriptor
        for (table_name, descriptor_id) in &catalog.tables {
            // Load descriptor object from Environment
            node.load_object(*descriptor_id, format!("descriptor:{}", table_name), "main")
                .ok_or_else(|| {
                    DatabaseError::Storage(format!("descriptor for {} not found in env", table_name))
                })?;

            // Read descriptor content
            let descriptor_bytes = node
                .read(*descriptor_id, "main")
                .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?
                .ok_or_else(|| {
                    DatabaseError::Storage(format!("descriptor for {} not found", table_name))
                })?;

            let descriptor = TableDescriptor::from_bytes(&descriptor_bytes)
                .map_err(|e| DatabaseError::Storage(format!("descriptor parse error: {}", e)))?;

            // Load schema object
            node.load_object(descriptor.schema_object_id, format!("schema:{}", table_name), "main");

            // Load rows object
            node.load_object(descriptor.rows_object_id, format!("rows:{}", table_name), "main");

            // Load index objects
            for (col_name, index_id) in &descriptor.index_object_ids {
                node.load_object(*index_id, format!("index:{}:{}", table_name, col_name), "main");
                let key = IndexKey::new(table_name, col_name);
                index_objects.insert(key, *index_id);
            }

            // Restore table metadata
            tables.insert(table_name.clone(), descriptor.schema_object_id);
            schemas.insert(descriptor.schema_object_id, descriptor.schema.clone());
            table_rows_objects.insert(table_name.clone(), descriptor.rows_object_id);
            descriptor_objects.insert(table_name.clone(), *descriptor_id);

            // Restore policies
            if !descriptor.policies.is_empty() {
                policies.insert(table_name.clone(), descriptor.policies.clone());
            }

            // Restore row_table mapping by reading table_rows
            let rows_bytes = node
                .read(descriptor.rows_object_id, "main")
                .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

            if let Some(bytes) = rows_bytes {
                let table_rows = TableRows::from_bytes(&bytes)
                    .map_err(|e| DatabaseError::Storage(format!("table_rows parse error: {}", e)))?;
                for row_id in table_rows.iter() {
                    // Load row object
                    node.load_object(row_id, format!("row:{}:{}", table_name, row_id), "main");
                    row_table.insert(row_id, table_name.clone());
                }
            }
        }

        let state = DatabaseState {
            node,
            catalog_object_id,
            tables: RwLock::new(tables),
            schemas: RwLock::new(schemas),
            table_rows_objects: RwLock::new(table_rows_objects),
            descriptor_objects: RwLock::new(descriptor_objects),
            row_table: RwLock::new(row_table),
            index_objects: RwLock::new(index_objects),
            policies: RwLock::new(policies),
            graph_registry: GraphRegistry::new(),
        };

        Ok(Database {
            state: Arc::new(state),
        })
    }

    /// Get the catalog object ID (for use with from_env).
    pub fn catalog_object_id(&self) -> ObjectId {
        self.state.catalog_object_id
    }

    /// Get the underlying LocalNode.
    pub fn node(&self) -> &LocalNode {
        &self.state.node
    }

    /// Get the shared database state.
    pub fn state(&self) -> &DatabaseState {
        &self.state
    }

    // ========== Index Object Helpers ==========

    /// Read an index from its object.
    fn read_index(&self, key: &IndexKey) -> Result<RefIndex, DatabaseError> {
        let index_objects = self.state.index_objects.read().unwrap();
        let index_id = index_objects
            .get(key)
            .ok_or_else(|| DatabaseError::ColumnNotFound(key.source_column.clone()))?;

        let data = self
            .state
            .node
            .read(*index_id, "main")
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?
            .unwrap_or_default();

        if data.is_empty() {
            return Ok(RefIndex::new());
        }

        RefIndex::from_bytes(&data)
            .map_err(|e| DatabaseError::Storage(format!("index decode: {}", e)))
    }

    /// Write an index to its object.
    fn write_index(&self, key: &IndexKey, index: &RefIndex) -> Result<(), DatabaseError> {
        let index_objects = self.state.index_objects.read().unwrap();
        let index_id = index_objects
            .get(key)
            .ok_or_else(|| DatabaseError::ColumnNotFound(key.source_column.clone()))?;

        self.state
            .node
            .write(
                *index_id,
                "main",
                &index.to_bytes(),
                "system",
                timestamp_now(),
            )
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

        Ok(())
    }

    /// Get the object ID for an index.
    pub fn index_object_id(&self, key: &IndexKey) -> Option<ObjectId> {
        self.state.index_objects.read().unwrap().get(key).copied()
    }

    // ========== Table Rows Object Helpers ==========

    /// Read table rows from its object.
    fn read_table_rows(&self, table: &str) -> Result<TableRows, DatabaseError> {
        let table_rows_objects = self.state.table_rows_objects.read().unwrap();
        let rows_id = table_rows_objects
            .get(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

        let data = self
            .state
            .node
            .read(*rows_id, "main")
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?
            .unwrap_or_default();

        if data.is_empty() {
            return Ok(TableRows::new());
        }

        TableRows::from_bytes(&data)
            .map_err(|e| DatabaseError::Storage(format!("table rows decode: {}", e)))
    }

    /// Write table rows to its object.
    fn write_table_rows(&self, table: &str, rows: &TableRows) -> Result<(), DatabaseError> {
        let table_rows_objects = self.state.table_rows_objects.read().unwrap();
        let rows_id = table_rows_objects
            .get(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

        self.state
            .node
            .write(
                *rows_id,
                "main",
                &rows.to_bytes(),
                "system",
                timestamp_now(),
            )
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

        Ok(())
    }

    /// Get the object ID for a table's row set.
    pub fn table_rows_object_id(&self, table: &str) -> Option<ObjectId> {
        self.state
            .table_rows_objects
            .read()
            .unwrap()
            .get(table)
            .copied()
    }

    /// Create a new table from schema.
    pub fn create_table(&self, schema: TableSchema) -> Result<SchemaId, DatabaseError> {
        {
            let tables = self.state.tables.read().unwrap();
            if tables.contains_key(&schema.name) {
                return Err(DatabaseError::TableExists(schema.name.clone()));
            }

            // Validate that referenced tables exist (for Ref columns)
            // Allow self-references (table referencing itself, e.g., parent_id)
            for col in &schema.columns {
                if let ColumnType::Ref(target_table) = &col.ty {
                    // Skip validation for self-references
                    if target_table != &schema.name && !tables.contains_key(target_table) {
                        return Err(DatabaseError::TableNotFound(target_table.clone()));
                    }
                }
            }
        }

        // Create object for schema (uses internal mutability)
        let schema_id = self
            .state
            .node
            .create_object(&format!("schema:{}", schema.name));

        // Serialize and store schema
        let schema_bytes = schema.to_bytes();
        self.state
            .node
            .write(schema_id, "main", &schema_bytes, "system", timestamp_now())
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

        // Create table rows object to track row membership
        let rows_id = self
            .state
            .node
            .create_object(&format!("rows:{}", schema.name));
        let empty_rows = TableRows::new();
        self.state
            .node
            .write(
                rows_id,
                "main",
                &empty_rows.to_bytes(),
                "system",
                timestamp_now(),
            )
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;
        self.state
            .table_rows_objects
            .write()
            .unwrap()
            .insert(schema.name.clone(), rows_id);

        // Create index objects for Ref columns
        let mut index_object_ids: HashMap<String, ObjectId> = HashMap::new();
        for col in &schema.columns {
            if matches!(col.ty, ColumnType::Ref(_)) {
                let key = IndexKey::new(&schema.name, &col.name);
                let index_id = self
                    .state
                    .node
                    .create_object(&format!("index:{}:{}", schema.name, col.name));

                // Initialize with empty index
                let empty_index = RefIndex::new();
                self.state
                    .node
                    .write(
                        index_id,
                        "main",
                        &empty_index.to_bytes(),
                        "system",
                        timestamp_now(),
                    )
                    .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

                index_object_ids.insert(col.name.clone(), index_id);

                self.state
                    .index_objects
                    .write()
                    .unwrap()
                    .insert(key, index_id);
            }
        }

        // Create table descriptor object
        let descriptor_id = self
            .state
            .node
            .create_object(&format!("descriptor:{}", schema.name));

        let descriptor = TableDescriptor {
            schema: schema.clone(),
            policies: TablePolicies::default(),
            rows_object_id: rows_id,
            schema_object_id: schema_id,
            index_object_ids,
        };
        self.state
            .node
            .write(
                descriptor_id,
                "main",
                &descriptor.to_bytes(),
                "system",
                timestamp_now(),
            )
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

        self.state
            .descriptor_objects
            .write()
            .unwrap()
            .insert(schema.name.clone(), descriptor_id);

        // Update catalog with the new table
        self.update_catalog_add_table(&schema.name, descriptor_id)?;

        // Cache schema
        self.state
            .tables
            .write()
            .unwrap()
            .insert(schema.name.clone(), schema_id);
        self.state
            .schemas
            .write()
            .unwrap()
            .insert(schema_id, schema);

        Ok(schema_id)
    }

    /// Update the catalog to add a new table.
    fn update_catalog_add_table(
        &self,
        table_name: &str,
        descriptor_id: ObjectId,
    ) -> Result<(), DatabaseError> {
        // Read current catalog
        let catalog_bytes = self
            .state
            .node
            .read(self.state.catalog_object_id, "main")
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?
            .ok_or_else(|| DatabaseError::Storage("catalog not found".to_string()))?;

        let mut catalog = Catalog::from_bytes(&catalog_bytes)
            .map_err(|e| DatabaseError::Storage(format!("catalog parse error: {}", e)))?;

        // Add the new table
        catalog.tables.insert(table_name.to_string(), descriptor_id);

        // Write updated catalog
        self.state
            .node
            .write(
                self.state.catalog_object_id,
                "main",
                &catalog.to_bytes(),
                "system",
                timestamp_now(),
            )
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

        Ok(())
    }

    /// Get table schema by name.
    pub fn get_table(&self, name: &str) -> Option<TableSchema> {
        let tables = self.state.tables.read().unwrap();
        let schema_id = tables.get(name)?;
        let schemas = self.state.schemas.read().unwrap();
        schemas.get(schema_id).cloned()
    }

    /// List all table names.
    pub fn list_tables(&self) -> Vec<String> {
        self.state.tables.read().unwrap().keys().cloned().collect()
    }

    /// Create a policy for a table.
    pub fn create_policy(&self, policy: Policy) -> Result<(), DatabaseError> {
        let table_name = policy.table.clone();

        // Verify table exists
        {
            let tables = self.state.tables.read().unwrap();
            if !tables.contains_key(&table_name) {
                return Err(DatabaseError::TableNotFound(table_name.clone()));
            }
        }

        // Add policy to table's policy collection
        {
            let mut policies = self.state.policies.write().unwrap();
            let table_policies = policies
                .entry(table_name.clone())
                .or_insert_with(TablePolicies::new);

            table_policies.add(policy)?;
        }

        // Update the table descriptor to persist the policy
        self.update_table_descriptor_policies(&table_name)?;

        Ok(())
    }

    /// Update the table descriptor with current policies.
    fn update_table_descriptor_policies(&self, table_name: &str) -> Result<(), DatabaseError> {
        // Get descriptor object ID
        let descriptor_id = self
            .state
            .descriptor_objects
            .read()
            .unwrap()
            .get(table_name)
            .copied()
            .ok_or_else(|| DatabaseError::TableNotFound(table_name.to_string()))?;

        // Read current descriptor
        let descriptor_bytes = self
            .state
            .node
            .read(descriptor_id, "main")
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?
            .ok_or_else(|| DatabaseError::Storage("descriptor not found".to_string()))?;

        let mut descriptor = TableDescriptor::from_bytes(&descriptor_bytes)
            .map_err(|e| DatabaseError::Storage(format!("descriptor parse error: {}", e)))?;

        // Update policies from current in-memory state
        let policies = self.state.policies.read().unwrap();
        descriptor.policies = policies
            .get(table_name)
            .cloned()
            .unwrap_or_default();

        // Write updated descriptor
        self.state
            .node
            .write(
                descriptor_id,
                "main",
                &descriptor.to_bytes(),
                "system",
                timestamp_now(),
            )
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

        Ok(())
    }

    /// Get policies for a table.
    pub fn get_policies(&self, table: &str) -> Option<TablePolicies> {
        let policies = self.state.policies.read().unwrap();
        policies.get(table).cloned()
    }

    /// Insert a new row into a table.
    pub fn insert(
        &self,
        table: &str,
        columns: &[&str],
        values: Vec<Value>,
    ) -> Result<ObjectId, DatabaseError> {
        let schema = self
            .get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

        // Build full row values in schema order
        let mut row_values = vec![Value::NullableNone; schema.columns.len()];

        if columns.len() != values.len() {
            return Err(DatabaseError::ColumnMismatch {
                expected: columns.len(),
                got: values.len(),
            });
        }

        for (col_name, value) in columns.iter().zip(values) {
            let idx = schema
                .column_index(col_name)
                .ok_or_else(|| DatabaseError::ColumnNotFound(col_name.to_string()))?;
            // Coerce the value to match the column type (e.g., String -> Ref)
            row_values[idx] = coerce_value(value, &schema.columns[idx].ty);
        }

        // Check for missing non-nullable columns
        for (i, col) in schema.columns.iter().enumerate() {
            if !col.nullable && row_values[i].is_null() {
                return Err(DatabaseError::MissingColumn(col.name.clone()));
            }
        }

        // Validate references: check that referenced rows exist
        {
            let row_table = self.state.row_table.read().unwrap();
            for (i, col) in schema.columns.iter().enumerate() {
                if let ColumnType::Ref(target_table) = &col.ty {
                    if let Value::Ref(target_id) = &row_values[i] {
                        // Check target row exists
                        if !row_table.contains_key(target_id) {
                            return Err(DatabaseError::InvalidReference {
                                column: col.name.clone(),
                                target_table: target_table.clone(),
                                target_id: *target_id,
                            });
                        }
                        // Also verify target row is in the correct table
                        if row_table.get(target_id) != Some(target_table) {
                            return Err(DatabaseError::InvalidReference {
                                column: col.name.clone(),
                                target_table: target_table.clone(),
                                target_id: *target_id,
                            });
                        }
                    }
                    // Null refs are ok if column is nullable (already validated above)
                }
            }
        }

        // Encode row
        let row_bytes = encode_row(&row_values, &schema)?;

        // Create object for row
        let row_id =
            self.state
                .node
                .create_object(&format!("row:{}:{}", table, generate_object_id()));

        // Store row data
        self.state
            .node
            .write(row_id, "main", &row_bytes, "system", timestamp_now())
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

        // Track row -> table mapping
        self.state
            .row_table
            .write()
            .unwrap()
            .insert(row_id, table.to_string());

        // Add to table rows object (for reactive queries)
        let mut table_rows = self.read_table_rows(table)?;
        table_rows.add(row_id);
        self.write_table_rows(table, &table_rows)?;

        // Update indexes for Ref columns
        for (i, col) in schema.columns.iter().enumerate() {
            if matches!(col.ty, ColumnType::Ref(_)) {
                if let Value::Ref(target_id) = &row_values[i] {
                    let key = IndexKey::new(table, &col.name);
                    if self.state.index_objects.read().unwrap().contains_key(&key) {
                        let mut index = self.read_index(&key)?;
                        index.add(*target_id, row_id);
                        self.write_index(&key, &index)?;
                    }
                }
            }
        }

        // Notify query graphs of the change
        let row = Row::new(row_id, row_values);
        self.state
            .graph_registry
            .notify_row_change(table, RowDelta::Added(row), &*self.state);

        Ok(row_id)
    }

    /// Get a row by ID.
    pub fn get(&self, table: &str, id: ObjectId) -> Result<Option<Row>, DatabaseError> {
        let schema = self
            .get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

        // Check if row belongs to this table
        {
            let row_table = self.state.row_table.read().unwrap();
            match row_table.get(&id) {
                Some(t) if t == table => {}
                Some(_) => return Ok(None), // Row exists but in different table
                None => return Ok(None),    // Row doesn't exist
            }
        }

        // Read row data
        let data = match self.state.node.read(id, "main") {
            Ok(Some(data)) => data,
            Ok(None) => return Ok(None),
            Err(e) => return Err(DatabaseError::Storage(format!("{:?}", e))),
        };

        // Decode row
        let values = decode_row(&data, &schema)?;

        Ok(Some(Row::new(id, values)))
    }

    /// Update a row by ID.
    pub fn update(
        &self,
        table: &str,
        id: ObjectId,
        assignments: &[(&str, Value)],
    ) -> Result<bool, DatabaseError> {
        let schema = self
            .get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

        // Check row exists and belongs to table
        {
            let row_table = self.state.row_table.read().unwrap();
            match row_table.get(&id) {
                Some(t) if t == table => {}
                _ => return Ok(false),
            }
        }

        // Read current row data
        let data = match self.state.node.read(id, "main") {
            Ok(Some(data)) => data,
            Ok(None) => return Ok(false),
            Err(e) => return Err(DatabaseError::Storage(format!("{:?}", e))),
        };

        // Decode current values
        let old_values = decode_row(&data, &schema)?;
        let mut new_values = old_values.clone();

        // Apply assignments
        for (col_name, value) in assignments {
            let idx = schema
                .column_index(col_name)
                .ok_or_else(|| DatabaseError::ColumnNotFound(col_name.to_string()))?;
            // Coerce the value to match the column type (e.g., String -> Ref)
            new_values[idx] = coerce_value(value.clone(), &schema.columns[idx].ty);
        }

        // Validate new references
        {
            let row_table = self.state.row_table.read().unwrap();
            for (i, col) in schema.columns.iter().enumerate() {
                if let ColumnType::Ref(target_table) = &col.ty {
                    if let Value::Ref(target_id) = &new_values[i] {
                        if !row_table.contains_key(target_id) {
                            return Err(DatabaseError::InvalidReference {
                                column: col.name.clone(),
                                target_table: target_table.clone(),
                                target_id: *target_id,
                            });
                        }
                        if row_table.get(target_id) != Some(target_table) {
                            return Err(DatabaseError::InvalidReference {
                                column: col.name.clone(),
                                target_table: target_table.clone(),
                                target_id: *target_id,
                            });
                        }
                    }
                }
            }
        }

        // Re-encode row
        let row_bytes = encode_row(&new_values, &schema)?;

        // Write updated row
        self.state
            .node
            .write(id, "main", &row_bytes, "system", timestamp_now())
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

        // Update indexes for changed Ref columns
        for (i, col) in schema.columns.iter().enumerate() {
            if matches!(col.ty, ColumnType::Ref(_)) {
                let old_ref = old_values[i].as_ref();
                let new_ref = new_values[i].as_ref();

                if old_ref != new_ref {
                    let key = IndexKey::new(table, &col.name);
                    if self.state.index_objects.read().unwrap().contains_key(&key) {
                        let mut index = self.read_index(&key)?;
                        // Remove old reference
                        if let Some(old_target) = old_ref {
                            index.remove(old_target, id);
                        }
                        // Add new reference
                        if let Some(new_target) = new_ref {
                            index.add(new_target, id);
                        }
                        self.write_index(&key, &index)?;
                    }
                }
            }
        }

        // Notify query graphs of the change
        let new_row = Row::new(id, new_values);
        self.state.graph_registry.notify_row_change(
            table,
            RowDelta::Updated {
                id,
                new: new_row,
                prior: PriorState::empty(), // TODO: Get prior commit tips
            },
            &*self.state,
        );

        Ok(true)
    }

    /// Delete a row by ID (soft delete).
    /// Creates a commit with deleted=true metadata marker.
    /// The row remains in the system but is filtered from queries.
    /// Use `delete_hard` to also truncate history.
    pub fn delete(&self, table: &str, id: ObjectId) -> Result<bool, DatabaseError> {
        self.delete_impl(table, id, false)
    }

    /// Delete a row by ID with history truncation (hard delete).
    /// Creates a soft delete commit, then truncates history at that commit.
    /// This is the closest to a true hard delete in a distributed system.
    pub fn delete_hard(&self, table: &str, id: ObjectId) -> Result<bool, DatabaseError> {
        self.delete_impl(table, id, true)
    }

    /// Internal delete implementation.
    fn delete_impl(&self, table: &str, id: ObjectId, hard: bool) -> Result<bool, DatabaseError> {
        let schema = self
            .get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

        // Check row exists and belongs to table
        {
            let row_table = self.state.row_table.read().unwrap();
            match row_table.get(&id) {
                Some(t) if t == table => {}
                _ => return Ok(false),
            }
        }

        // Read current row data to get ref values for index cleanup
        let data = match self.state.node.read(id, "main") {
            Ok(Some(data)) if !data.is_empty() => Some(data),
            _ => None,
        };

        // Remove from indexes
        if let Some(data) = data {
            if let Ok(values) = decode_row(&data, &schema) {
                for (i, col) in schema.columns.iter().enumerate() {
                    if matches!(col.ty, ColumnType::Ref(_)) {
                        if let Value::Ref(target_id) = &values[i] {
                            let key = IndexKey::new(table, &col.name);
                            if self.state.index_objects.read().unwrap().contains_key(&key) {
                                let mut index = self.read_index(&key)?;
                                index.remove(*target_id, id);
                                self.write_index(&key, &index)?;
                            }
                        }
                    }
                }
            }
        }

        // Create delete metadata marker
        let mut meta = std::collections::BTreeMap::new();
        meta.insert("deleted".to_string(), "true".to_string());

        // Write soft delete commit with metadata marker
        let commit_id = self.state
            .node
            .write_with_meta(id, "main", &[], "system", timestamp_now(), Some(meta))
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

        // If hard delete, truncate history at the delete commit
        if hard {
            self.state
                .node
                .truncate_at(id, "main", commit_id)
                .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;
        }

        // Remove from row_table (logically deleted)
        self.state.row_table.write().unwrap().remove(&id);

        // Remove from table rows object
        let mut table_rows = self.read_table_rows(table)?;
        table_rows.remove(id);
        self.write_table_rows(table, &table_rows)?;

        // Notify query graphs of the change
        self.state.graph_registry.notify_row_change(
            table,
            RowDelta::Removed {
                id,
                prior: PriorState::empty(), // TODO: Get prior commit tips
            },
            &*self.state,
        );

        Ok(true)
    }

    /// Simple select - returns all rows from a table.
    /// For now, implements only basic scans.
    pub fn select_all(&self, table: &str) -> Result<Vec<Row>, DatabaseError> {
        let schema = self
            .get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

        let mut rows = Vec::new();

        // Find all rows for this table
        let row_table = self.state.row_table.read().unwrap();
        for (&row_id, row_tbl) in row_table.iter() {
            if row_tbl != table {
                continue;
            }

            // Read row data
            let data = match self.state.node.read(row_id, "main") {
                Ok(Some(data)) if !data.is_empty() => data,
                _ => continue, // Skip deleted or missing rows
            };

            // Decode row
            match decode_row(&data, &schema) {
                Ok(values) => rows.push(Row::new(row_id, values)),
                Err(_) => continue, // Skip malformed rows
            }
        }

        Ok(rows)
    }

    /// Select rows matching a simple where clause (column = value).
    pub fn select_where(
        &self,
        table: &str,
        column: &str,
        value: &Value,
    ) -> Result<Vec<Row>, DatabaseError> {
        let schema = self
            .get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

        // Special case: id column (implicit, not in schema)
        if column == "id" {
            // Coerce String to Ref for id comparison
            let coerced = coerce_value(value.clone(), &ColumnType::Ref("".to_string()));
            if let Value::Ref(id) = coerced {
                return match self.get(table, id)? {
                    Some(row) => Ok(vec![row]),
                    None => Ok(vec![]),
                };
            }
            // id column but non-Ref value and not coercible - no matches
            return Ok(vec![]);
        }

        let col_idx = schema
            .column_index(column)
            .ok_or_else(|| DatabaseError::ColumnNotFound(column.to_string()))?;

        // Coerce the value to match the column type
        let coerced = coerce_value(value.clone(), &schema.columns[col_idx].ty);

        let mut rows = Vec::new();

        // Scan all rows
        let row_table = self.state.row_table.read().unwrap();
        for (&row_id, row_tbl) in row_table.iter() {
            if row_tbl != table {
                continue;
            }

            let data = match self.state.node.read(row_id, "main") {
                Ok(Some(data)) if !data.is_empty() => data,
                _ => continue,
            };

            match decode_row(&data, &schema) {
                Ok(values) => {
                    if values[col_idx] == coerced {
                        rows.push(Row::new(row_id, values));
                    }
                }
                Err(_) => continue,
            }
        }

        Ok(rows)
    }

    // ========== Policy-Filtered Queries ==========

    /// Select all rows from a table, filtered by policy for the given viewer.
    pub fn select_all_as(&self, table: &str, viewer: ObjectId) -> Result<Vec<Row>, DatabaseError> {
        let rows = self.select_all(table)?;
        Ok(self.filter_rows_by_policy(table, rows, viewer))
    }

    /// Select rows matching a condition, filtered by policy for the given viewer.
    pub fn select_where_as(
        &self,
        table: &str,
        column: &str,
        value: &Value,
        viewer: ObjectId,
    ) -> Result<Vec<Row>, DatabaseError> {
        let rows = self.select_where(table, column, value)?;
        Ok(self.filter_rows_by_policy(table, rows, viewer))
    }

    /// Filter a list of rows by SELECT policy for the given viewer.
    fn filter_rows_by_policy(&self, table: &str, rows: Vec<Row>, viewer: ObjectId) -> Vec<Row> {
        use crate::sql::policy::{PolicyConfig, PolicyEvaluator};

        let config = PolicyConfig::default();
        let mut evaluator = PolicyEvaluator::new(self, self, viewer, config);

        rows.into_iter()
            .filter(|row| evaluator.check_select(table, row).is_allowed())
            .collect()
    }

    // ========== Policy-Checked Write Operations ==========

    /// Insert a new row, checking INSERT policy for the given viewer.
    pub fn insert_as(
        &self,
        table: &str,
        columns: &[&str],
        values: Vec<Value>,
        viewer: ObjectId,
    ) -> Result<ObjectId, DatabaseError> {
        use crate::sql::policy::{PolicyConfig, PolicyEvaluator, PolicyResult};

        let schema = self
            .get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

        // Build the row values first (to validate and check policy)
        let mut row_values = vec![Value::NullableNone; schema.columns.len()];

        if columns.len() != values.len() {
            return Err(DatabaseError::ColumnMismatch {
                expected: columns.len(),
                got: values.len(),
            });
        }

        for (col_name, value) in columns.iter().zip(values.clone()) {
            let idx = schema
                .column_index(col_name)
                .ok_or_else(|| DatabaseError::ColumnNotFound(col_name.to_string()))?;
            row_values[idx] = coerce_value(value, &schema.columns[idx].ty);
        }

        // Check for missing non-nullable columns
        for (i, col) in schema.columns.iter().enumerate() {
            if !col.nullable && row_values[i].is_null() {
                return Err(DatabaseError::MissingColumn(col.name.clone()));
            }
        }

        // Create a temporary row for policy evaluation
        let temp_row = Row::new(ObjectId::default(), row_values);

        // Check INSERT policy
        let config = PolicyConfig::default();
        let mut evaluator = PolicyEvaluator::new(self, self, viewer, config);
        let result = evaluator.check_insert(table, &temp_row);

        match result {
            PolicyResult::Denied { reason } => {
                return Err(DatabaseError::PolicyDenied {
                    action: PolicyAction::Insert,
                    reason,
                });
            }
            PolicyResult::Allowed { .. } => {}
        }

        // Policy passed, perform the actual insert
        self.insert(table, columns, values)
    }

    /// Update a row, checking UPDATE policy for the given viewer.
    pub fn update_as(
        &self,
        table: &str,
        id: ObjectId,
        assignments: &[(&str, Value)],
        viewer: ObjectId,
    ) -> Result<bool, DatabaseError> {
        use crate::sql::policy::{PolicyConfig, PolicyEvaluator, PolicyResult};

        let schema = self
            .get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

        // Get the existing row
        let old_row = match self.get(table, id)? {
            Some(row) => row,
            None => return Ok(false),
        };

        // Build the new row values
        let mut new_values = old_row.values.clone();
        for (col_name, value) in assignments {
            let idx = schema
                .column_index(col_name)
                .ok_or_else(|| DatabaseError::ColumnNotFound(col_name.to_string()))?;
            new_values[idx] = coerce_value(value.clone(), &schema.columns[idx].ty);
        }

        let new_row = Row::new(id, new_values);

        // Check UPDATE policy
        let config = PolicyConfig::default();
        let mut evaluator = PolicyEvaluator::new(self, self, viewer, config);
        let result = evaluator.check_update(table, &old_row, &new_row);

        match result {
            PolicyResult::Denied { reason } => {
                return Err(DatabaseError::PolicyDenied {
                    action: PolicyAction::Update,
                    reason,
                });
            }
            PolicyResult::Allowed { .. } => {}
        }

        // Policy passed, perform the actual update
        self.update(table, id, assignments)
    }

    /// Delete a row, checking DELETE policy for the given viewer.
    pub fn delete_as(
        &self,
        table: &str,
        id: ObjectId,
        viewer: ObjectId,
    ) -> Result<bool, DatabaseError> {
        use crate::sql::policy::{PolicyConfig, PolicyEvaluator, PolicyResult};

        // Get the existing row
        let row = match self.get(table, id)? {
            Some(row) => row,
            None => return Ok(false),
        };

        // Check DELETE policy
        let config = PolicyConfig::default();
        let mut evaluator = PolicyEvaluator::new(self, self, viewer, config);
        let result = evaluator.check_delete(table, &row);

        match result {
            PolicyResult::Denied { reason } => {
                return Err(DatabaseError::PolicyDenied {
                    action: PolicyAction::Delete,
                    reason,
                });
            }
            PolicyResult::Allowed { .. } => {}
        }

        // Policy passed, perform the actual delete
        self.delete(table, id)
    }

    /// Find all rows referencing a target ID via a specific column.
    /// Uses the reverse index for O(1) lookup.
    pub fn find_referencing(
        &self,
        source_table: &str,
        source_column: &str,
        target_id: ObjectId,
    ) -> Result<Vec<Row>, DatabaseError> {
        let schema = self
            .get_table(source_table)
            .ok_or_else(|| DatabaseError::TableNotFound(source_table.to_string()))?;

        // Verify column is a Ref type
        let col = schema
            .column(source_column)
            .ok_or_else(|| DatabaseError::ColumnNotFound(source_column.to_string()))?;
        if !matches!(col.ty, ColumnType::Ref(_)) {
            return Err(DatabaseError::NotAReference(source_column.to_string()));
        }

        // Look up in index
        let key = IndexKey::new(source_table, source_column);
        let source_ids: Vec<ObjectId> =
            if self.state.index_objects.read().unwrap().contains_key(&key) {
                let index = self.read_index(&key)?;
                index.get(target_id).collect()
            } else {
                return Ok(vec![]); // No index means no refs
            };

        // Fetch the actual rows
        let mut rows = Vec::new();
        for row_id in source_ids {
            if let Some(row) = self.get(source_table, row_id)? {
                rows.push(row);
            }
        }

        Ok(rows)
    }

    /// Execute a SQL statement.
    pub fn execute(&self, sql: &str) -> Result<ExecuteResult, DatabaseError> {
        let stmt = parser::parse(sql)?;

        match stmt {
            Statement::CreateTable(ct) => {
                let schema = TableSchema::new(ct.name, ct.columns);
                let id = self.create_table(schema)?;
                Ok(ExecuteResult::Created(id))
            }
            Statement::CreatePolicy(policy) => {
                let table = policy.table.clone();
                let action = policy.action;
                self.create_policy(policy)?;
                Ok(ExecuteResult::PolicyCreated { table, action })
            }
            Statement::Insert(ins) => {
                let columns: Vec<&str> = ins.columns.iter().map(|s| s.as_str()).collect();
                let id = self.insert(&ins.table, &columns, ins.values)?;
                Ok(ExecuteResult::Inserted(id))
            }
            Statement::Update(upd) => {
                // Find rows matching where clause
                let rows_to_update = if upd.where_clause.is_empty() {
                    self.select_all(&upd.table)?
                } else if upd.where_clause.len() == 1 {
                    let cond = &upd.where_clause[0];
                    let value = cond.value().ok_or_else(|| {
                        DatabaseError::ColumnNotFound(
                            "column references not supported in UPDATE WHERE".to_string(),
                        )
                    })?;
                    self.select_where(&upd.table, &cond.column.column, value)?
                } else {
                    // Multiple conditions - start with first, then filter
                    let cond = &upd.where_clause[0];
                    let value = cond.value().ok_or_else(|| {
                        DatabaseError::ColumnNotFound(
                            "column references not supported in UPDATE WHERE".to_string(),
                        )
                    })?;
                    let mut rows = self.select_where(&upd.table, &cond.column.column, value)?;
                    let schema = self.get_table(&upd.table).unwrap();

                    for cond in &upd.where_clause[1..] {
                        let col_idx =
                            schema.column_index(&cond.column.column).ok_or_else(|| {
                                DatabaseError::ColumnNotFound(cond.column.column.clone())
                            })?;
                        let value = cond.value().ok_or_else(|| {
                            DatabaseError::ColumnNotFound(
                                "column references not supported in UPDATE WHERE".to_string(),
                            )
                        })?;
                        rows.retain(|row| row.values.get(col_idx) == Some(value));
                    }
                    rows
                };

                let count = rows_to_update.len();
                let assignments: Vec<(&str, Value)> = upd
                    .assignments
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.clone()))
                    .collect();

                for row in rows_to_update {
                    self.update(&upd.table, row.id, &assignments)?;
                }

                Ok(ExecuteResult::Updated(count))
            }
            Statement::Delete(del) => {
                // Find rows matching where clause
                let rows_to_delete = if del.where_clause.is_empty() {
                    self.select_all(&del.table)?
                } else if del.where_clause.len() == 1 {
                    let cond = &del.where_clause[0];
                    let value = cond.value().ok_or_else(|| {
                        DatabaseError::ColumnNotFound(
                            "column references not supported in DELETE WHERE".to_string(),
                        )
                    })?;
                    self.select_where(&del.table, &cond.column.column, value)?
                } else {
                    // Multiple conditions - start with first, then filter
                    let cond = &del.where_clause[0];
                    let value = cond.value().ok_or_else(|| {
                        DatabaseError::ColumnNotFound(
                            "column references not supported in DELETE WHERE".to_string(),
                        )
                    })?;
                    let mut rows = self.select_where(&del.table, &cond.column.column, value)?;
                    let schema = self.get_table(&del.table).unwrap();

                    for cond in &del.where_clause[1..] {
                        let col_idx =
                            schema.column_index(&cond.column.column).ok_or_else(|| {
                                DatabaseError::ColumnNotFound(cond.column.column.clone())
                            })?;
                        let value = cond.value().ok_or_else(|| {
                            DatabaseError::ColumnNotFound(
                                "column references not supported in DELETE WHERE".to_string(),
                            )
                        })?;
                        rows.retain(|row| row.values.get(col_idx) == Some(value));
                    }
                    rows
                };

                let count = rows_to_delete.len();

                for row in rows_to_delete {
                    if del.hard {
                        self.delete_hard(&del.table, row.id)?;
                    } else {
                        self.delete(&del.table, row.id)?;
                    }
                }

                Ok(ExecuteResult::Deleted(count))
            }
            Statement::Select(sel) => {
                let rows = self.state.execute_select(&sel);
                Ok(ExecuteResult::Selected(rows))
            }
        }
    }

    // ========== Incremental Queries ==========

    /// Create an incremental query using a computation graph.
    ///
    /// Uses true incremental computation - only processing the delta from
    /// each change rather than re-evaluating the entire query.
    ///
    /// Supports single-table queries with optional WHERE filters, as well as
    /// JOIN queries between two tables.
    pub fn incremental_query(&self, sql: &str) -> Result<IncrementalQuery, DatabaseError> {
        let stmt = parser::parse(sql)?;

        let select = match stmt {
            Statement::Select(s) => s,
            _ => {
                return Err(DatabaseError::Parse(parser::ParseError {
                    message: "incremental_query only supports SELECT statements".to_string(),
                    position: 0,
                }));
            }
        };

        let graph = if select.from.joins.is_empty() {
            // Single-table query
            self.build_single_table_graph(&select)?
        } else {
            // JOIN query
            self.build_join_graph(&select)?
        };

        // Register the graph
        let graph_id = self.state.graph_registry.register(graph);

        Ok(IncrementalQuery {
            graph_id,
            db_state: self.state.clone(),
        })
    }

    /// Create an incremental query with policy filtering for the given viewer.
    ///
    /// This combines the SQL query's WHERE clause with the table's SELECT policy,
    /// ensuring only rows the viewer is allowed to see are returned.
    ///
    /// For simple policies (e.g., `owner_id = @viewer`), the policy predicate is
    /// merged into the query graph for efficient incremental evaluation.
    ///
    /// For policies with INHERITS, a runtime policy filter is applied after the
    /// user's WHERE clause.
    pub fn incremental_query_as(
        &self,
        sql: &str,
        viewer: ObjectId,
    ) -> Result<IncrementalQuery, DatabaseError> {
        let stmt = parser::parse(sql)?;

        let select = match stmt {
            Statement::Select(s) => s,
            _ => {
                return Err(DatabaseError::Parse(parser::ParseError {
                    message: "incremental_query_as only supports SELECT statements".to_string(),
                    position: 0,
                }));
            }
        };

        // Only support single-table queries for now
        if !select.from.joins.is_empty() {
            return Err(DatabaseError::Parse(parser::ParseError {
                message: "incremental_query_as does not yet support JOINs".to_string(),
                position: 0,
            }));
        }

        let graph = self.build_single_table_graph_with_policy(&select, viewer)?;

        // Register the graph
        let graph_id = self.state.graph_registry.register(graph);

        Ok(IncrementalQuery {
            graph_id,
            db_state: self.state.clone(),
        })
    }

    /// Build a query graph for a single-table SELECT with policy filtering.
    fn build_single_table_graph_with_policy(
        &self,
        select: &Select,
        viewer: ObjectId,
    ) -> Result<crate::sql::query_graph::QueryGraph, DatabaseError> {
        let table = &select.from.table;

        // Validate table exists and get schema
        let schema = self
            .get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.clone()))?;

        // Get the SELECT policy for this table (if any)
        let policies = self.get_policies(table);
        let select_policy = policies.as_ref().and_then(|p| p.get(PolicyAction::Select));

        // Check if policy contains INHERITS
        if let Some(policy) = select_policy {
            if let Some(ref where_expr) = policy.where_clause {
                if let Some(inherits_info) = self.extract_inherits(where_expr, table, &schema)? {
                    if inherits_info.is_self_referential {
                        // Self-referential INHERITS: use RecursiveFilter
                        return self.build_recursive_filter_graph(
                            select,
                            viewer,
                            &inherits_info,
                            &schema,
                        );
                    } else {
                        // Non-self-referential INHERITS: resolve the full chain
                        let mut visited = vec![table.to_string()];
                        let chain = self.resolve_inherits_chain(
                            &inherits_info,
                            table,
                            viewer,
                            &mut visited,
                        )?;

                        if chain.hops.len() == 1 {
                            // Single hop: use existing simple JOIN graph
                            return self.build_inherits_join_graph(select, viewer, &inherits_info);
                        } else {
                            // Multi-hop chain: use chain JOIN graph
                            return self.build_chain_join_graph(select, viewer, &chain, &schema);
                        }
                    }
                }
            }
        }

        // No INHERITS - build a simple single-table graph
        let mut builder = QueryGraphBuilder::new(table, schema.clone());
        let scan = builder.table_scan();

        // Apply user's WHERE clause first
        let after_user_where = if select.where_clause.is_empty() {
            scan
        } else {
            let predicate = self.build_predicate(&select.where_clause, &schema)?;
            builder.filter(scan, predicate)
        };

        // Apply policy predicate
        let after_policy = if let Some(policy) = select_policy {
            if let Some(ref where_expr) = policy.where_clause {
                match self.policy_expr_to_predicate(where_expr, viewer) {
                    Ok(policy_predicate) => builder.filter(after_user_where, policy_predicate),
                    Err(e) => {
                        // SECURITY: If we can't convert policy to predicate, we must fail
                        // rather than silently allowing all rows. This can happen with
                        // OR expressions containing INHERITS that can't be flattened to JOINs.
                        return Err(e);
                    }
                }
            } else {
                after_user_where
            }
        } else {
            after_user_where
        };

        // Apply LIMIT/OFFSET if specified
        let limited = builder.limit_offset(
            after_policy,
            select.limit,
            select.offset.unwrap_or(0),
        );

        Ok(builder.output(limited, GraphId(0)))
    }

    /// Extract INHERITS information from a policy expression.
    ///
    /// Returns Some(InheritsInfo) if the policy contains a simple INHERITS clause
    /// that can be flattened to a JOIN or handled with RecursiveFilter.
    /// Returns None for simple predicates without INHERITS.
    fn extract_inherits(
        &self,
        expr: &PolicyExpr,
        source_table: &str,
        source_schema: &TableSchema,
    ) -> Result<Option<InheritsInfo>, DatabaseError> {
        match expr {
            PolicyExpr::Inherits { action, column } => {
                if *action != PolicyAction::Select {
                    return Err(DatabaseError::Parse(parser::ParseError {
                        message: format!(
                            "INHERITS {} not supported in incremental queries, only INHERITS SELECT",
                            action
                        ),
                        position: 0,
                    }));
                }

                let col_name = column.column_name();

                // Find the target table from the Ref column
                let col_def = source_schema
                    .column(col_name)
                    .ok_or_else(|| DatabaseError::ColumnNotFound(col_name.to_string()))?;

                let target_table = match &col_def.ty {
                    ColumnType::Ref(t) => t.clone(),
                    _ => return Err(DatabaseError::NotAReference(col_name.to_string())),
                };

                let is_self_referential = target_table == source_table;

                // Get the target table's SELECT policy (only needed for non-self-referential)
                let target_predicate = if is_self_referential {
                    None // Self-referential recursion - no target policy needed
                } else {
                    let target_policies = self.get_policies(&target_table);
                    let target_select = target_policies
                        .as_ref()
                        .and_then(|p| p.get(PolicyAction::Select));
                    target_select.and_then(|p| p.where_clause.clone())
                };

                Ok(Some(InheritsInfo {
                    ref_column: col_name.to_string(),
                    target_table,
                    target_predicate,
                    additional_predicates: vec![],
                    is_self_referential,
                    base_predicate: None,
                }))
            }
            PolicyExpr::And(exprs) => {
                // Check if any sub-expression is INHERITS
                let mut inherits_info: Option<InheritsInfo> = None;
                let mut additional: Vec<PolicyExpr> = vec![];

                for e in exprs {
                    if let Some(info) = self.extract_inherits(e, source_table, source_schema)? {
                        if inherits_info.is_some() {
                            // Multiple INHERITS in AND - not yet supported
                            return Err(DatabaseError::Parse(parser::ParseError {
                                message: "Multiple INHERITS in AND not yet supported".to_string(),
                                position: 0,
                            }));
                        }
                        inherits_info = Some(info);
                    } else {
                        additional.push(e.clone());
                    }
                }

                if let Some(mut info) = inherits_info {
                    info.additional_predicates = additional;
                    Ok(Some(info))
                } else {
                    Ok(None)
                }
            }
            PolicyExpr::Or(exprs) => {
                // For OR expressions, look for pattern: base_predicate OR INHERITS
                // This is the typical self-referential pattern:
                // `owner_id = @viewer OR INHERITS SELECT FROM parent_id`
                let mut inherits_info: Option<InheritsInfo> = None;
                let mut base_predicates: Vec<PolicyExpr> = vec![];

                for e in exprs {
                    if let Some(info) = self.extract_inherits(e, source_table, source_schema)? {
                        if inherits_info.is_some() {
                            // Multiple INHERITS in OR - not yet supported
                            return Err(DatabaseError::Parse(parser::ParseError {
                                message: "Multiple INHERITS in OR not yet supported".to_string(),
                                position: 0,
                            }));
                        }
                        inherits_info = Some(info);
                    } else {
                        base_predicates.push(e.clone());
                    }
                }

                if let Some(mut info) = inherits_info {
                    // Combine base predicates with OR
                    if !base_predicates.is_empty() {
                        if base_predicates.len() == 1 {
                            info.base_predicate = Some(base_predicates.remove(0));
                        } else {
                            info.base_predicate = Some(PolicyExpr::Or(base_predicates));
                        }
                    }
                    Ok(Some(info))
                } else {
                    Ok(None)
                }
            }
            // Other expressions don't contain INHERITS at the top level
            _ => Ok(None),
        }
    }

    /// Resolve an INHERITS chain by following all hops until reaching a terminal predicate.
    ///
    /// For example, with:
    /// - documents: INHERITS SELECT FROM folder_id
    /// - folders: INHERITS SELECT FROM workspace_id
    /// - workspaces: owner_id = @viewer
    ///
    /// Returns a chain: documents→folders, folders→workspaces, terminal: owner_id = @viewer
    fn resolve_inherits_chain(
        &self,
        initial_inherits: &InheritsInfo,
        source_table: &str,
        viewer: ObjectId,
        visited: &mut Vec<String>,
    ) -> Result<InheritsChain, DatabaseError> {
        // Prevent infinite loops (should already be caught by is_self_referential)
        if visited.contains(&initial_inherits.target_table) {
            return Err(DatabaseError::Parse(parser::ParseError {
                message: format!(
                    "Circular INHERITS chain detected: {} -> {}",
                    source_table, initial_inherits.target_table
                ),
                position: 0,
            }));
        }
        visited.push(initial_inherits.target_table.clone());

        let target_schema = self
            .get_table(&initial_inherits.target_table)
            .ok_or_else(|| DatabaseError::TableNotFound(initial_inherits.target_table.clone()))?;

        // Check if target table has a policy
        let target_policies = self.get_policies(&initial_inherits.target_table);
        let target_select = target_policies
            .as_ref()
            .and_then(|p| p.get(PolicyAction::Select));

        if let Some(policy) = target_select {
            if let Some(ref where_expr) = policy.where_clause {
                // Check if target policy also has INHERITS
                if let Some(nested_inherits) = self.extract_inherits(
                    where_expr,
                    &initial_inherits.target_table,
                    &target_schema,
                )? {
                    if nested_inherits.is_self_referential {
                        // Self-referential in the chain - not supported yet
                        return Err(DatabaseError::Parse(parser::ParseError {
                            message: "Self-referential INHERITS in chain not yet supported"
                                .to_string(),
                            position: 0,
                        }));
                    }

                    // Convert the target table's base_predicate (the OR sibling at this level)
                    let target_base = if let Some(ref base_expr) = nested_inherits.base_predicate {
                        Some(self.policy_expr_to_predicate(base_expr, viewer)?)
                    } else {
                        None
                    };

                    // Create the first hop with its base predicate
                    let first_hop = ChainHop {
                        ref_column: initial_inherits.ref_column.clone(),
                        target_table: initial_inherits.target_table.clone(),
                        // The base_predicate on this hop is the target table's base predicate
                        // (the OR sibling that allows short-circuiting at this level)
                        base_predicate: target_base,
                    };

                    // Recursively resolve the rest of the chain
                    let mut rest_of_chain = self.resolve_inherits_chain(
                        &nested_inherits,
                        &initial_inherits.target_table,
                        viewer,
                        visited,
                    )?;

                    // Prepend our hop
                    let mut hops = vec![first_hop];
                    hops.extend(rest_of_chain.hops);
                    rest_of_chain.hops = hops;

                    return Ok(rest_of_chain);
                }

                // No INHERITS - this is the terminal table
                let terminal_predicate = self.policy_expr_to_predicate(where_expr, viewer)?;
                let first_hop = ChainHop {
                    ref_column: initial_inherits.ref_column.clone(),
                    target_table: initial_inherits.target_table.clone(),
                    base_predicate: None, // Terminal table - no base predicate needed
                };
                return Ok(InheritsChain {
                    hops: vec![first_hop],
                    terminal_predicate: Some(terminal_predicate),
                    terminal_table: initial_inherits.target_table.clone(),
                });
            }
        }

        // No policy on target table = allow all (terminal with no predicate)
        let first_hop = ChainHop {
            ref_column: initial_inherits.ref_column.clone(),
            target_table: initial_inherits.target_table.clone(),
            base_predicate: None,
        };
        Ok(InheritsChain {
            hops: vec![first_hop],
            terminal_predicate: None,
            terminal_table: initial_inherits.target_table.clone(),
        })
    }

    /// Build a JOIN graph to handle INHERITS policies.
    ///
    /// Transforms a query like `SELECT * FROM documents` with policy
    /// `INHERITS SELECT FROM folder_id` into an equivalent JOIN query.
    fn build_inherits_join_graph(
        &self,
        select: &Select,
        viewer: ObjectId,
        inherits: &InheritsInfo,
    ) -> Result<crate::sql::query_graph::QueryGraph, DatabaseError> {
        let left_table = &select.from.table;
        let left_schema = self
            .get_table(left_table)
            .ok_or_else(|| DatabaseError::TableNotFound(left_table.clone()))?;
        let right_schema = self
            .get_table(&inherits.target_table)
            .ok_or_else(|| DatabaseError::TableNotFound(inherits.target_table.clone()))?;

        // Build a JOIN graph
        let mut builder = JoinGraphBuilder::new(
            left_table,
            left_schema.clone(),
            &inherits.target_table,
            right_schema.clone(),
            &inherits.ref_column,
        );

        let join_node = builder.join();

        // Apply user's WHERE clause (needs qualified column handling)
        let after_user_where = if select.where_clause.is_empty() {
            join_node
        } else {
            let predicate = self.build_predicate(&select.where_clause, &left_schema)?;
            builder.filter(join_node, predicate)
        };

        // Apply additional predicates from source policy (non-INHERITS parts)
        let after_additional = if inherits.additional_predicates.is_empty() {
            after_user_where
        } else {
            let mut combined = Predicate::True;
            for expr in &inherits.additional_predicates {
                let pred = self.policy_expr_to_predicate(expr, viewer)?;
                combined = combined.and(pred);
            }
            builder.filter(after_user_where, combined)
        };

        // Apply the target table's policy predicate (the flattened INHERITS)
        let after_policy = if let Some(ref target_expr) = inherits.target_predicate {
            // Convert target policy to predicate, but with column names qualified to right table
            let predicate = self.policy_expr_to_predicate_qualified(
                target_expr,
                viewer,
                &inherits.target_table,
            )?;
            builder.filter(after_additional, predicate)
        } else {
            // No policy on target table = allow all
            after_additional
        };

        // Apply LIMIT/OFFSET if specified
        let limited = builder.limit_offset(
            after_policy,
            select.limit,
            select.offset.unwrap_or(0),
        );

        Ok(builder.output(limited, GraphId(0)))
    }

    /// Build a JOIN graph for INHERITS chains with multiple hops.
    ///
    /// For chains like: documents → folders → workspaces (with workspaces.owner_id = @viewer)
    ///
    /// This builds a 2-table join for the first hop, then applies the terminal predicate
    /// qualified to the second table. For chains with 3+ hops, we recursively build
    /// the intermediate table's join first.
    ///
    /// TODO: This currently only propagates changes from the source table incrementally.
    /// Changes to intermediate/terminal tables require re-initialization. A future
    /// optimization would track all tables and propagate changes through the full chain.
    fn build_chain_join_graph(
        &self,
        select: &Select,
        _viewer: ObjectId, // Terminal predicate is already resolved in chain
        chain: &InheritsChain,
        source_schema: &TableSchema,
    ) -> Result<crate::sql::query_graph::QueryGraph, DatabaseError> {
        // For chains, we build from the end backwards:
        // The terminal table has a simple predicate (e.g., owner_id = @viewer)
        // Each intermediate table inherits from the next
        //
        // For documents → folders → workspaces:
        // - documents JOIN folders ON documents.folder_id = folders.id
        // - WHERE folders.workspace_id IN (SELECT id FROM workspaces WHERE owner_id = @viewer)
        //
        // We simplify by building a 2-table join (source → first hop) and
        // applying a filter that walks the rest of the chain.

        let left_table = &select.from.table;

        // Support arbitrary chain lengths
        if chain.hops.is_empty() {
            return Err(DatabaseError::Parse(parser::ParseError {
                message: "Empty INHERITS chain".to_string(),
                position: 0,
            }));
        }

        // Get schema for first hop's target
        let first_hop = &chain.hops[0];
        let first_target_schema = self
            .get_table(&first_hop.target_table)
            .ok_or_else(|| DatabaseError::TableNotFound(first_hop.target_table.clone()))?;

        // Build the first join: source → first target
        let mut builder = JoinGraphBuilder::new(
            left_table,
            source_schema.clone(),
            &first_hop.target_table,
            first_target_schema.clone(),
            &first_hop.ref_column,
        );

        // Pre-add schemas for all subsequent hops
        for hop in chain.hops.iter().skip(1) {
            let target_schema = self
                .get_table(&hop.target_table)
                .ok_or_else(|| DatabaseError::TableNotFound(hop.target_table.clone()))?;
            builder.add_schema(&hop.target_table, target_schema);
        }

        // Build first join
        let mut current_node = builder.join();

        // Apply user's WHERE clause after first join
        // The predicate must use qualified column names since we're in a JOIN context
        if !select.where_clause.is_empty() {
            let predicate = self.build_predicate(&select.where_clause, source_schema)?;
            let qualified_pred = predicate.qualify(&left_table);
            current_node = builder.filter(current_node, qualified_pred);
        }

        // Add chain joins for remaining hops
        let mut prev_table = first_hop.target_table.clone();
        for hop in chain.hops.iter().skip(1) {
            current_node = builder.chain_join(
                current_node,
                &prev_table,
                &hop.ref_column,
                &hop.target_table,
            );
            prev_table = hop.target_table.clone();
        }

        // Build combined predicate: OR of all intermediate base_predicates and terminal_predicate
        // This implements the semantics: a row matches if ANY level in the chain grants access.
        //
        // For example, with:
        //   - documents: INHERITS SELECT FROM folder_id
        //   - folders: owner_id = @viewer OR INHERITS SELECT FROM workspace_id
        //   - workspaces: owner_id = @viewer
        //
        // The combined predicate is:
        //   folders.owner_id = @viewer OR workspaces.owner_id = @viewer
        let mut or_predicates: Vec<Predicate> = Vec::new();

        // Collect base predicates from each hop (qualified with target table)
        for hop in &chain.hops {
            if let Some(ref base_pred) = hop.base_predicate {
                let qualified_pred = base_pred.qualify(&hop.target_table);
                or_predicates.push(qualified_pred);
            }
        }

        // Add terminal predicate (qualified with terminal table)
        if let Some(ref terminal_pred) = chain.terminal_predicate {
            let qualified_pred = terminal_pred.qualify(&chain.terminal_table);
            or_predicates.push(qualified_pred);
        }

        // Apply combined predicate
        if !or_predicates.is_empty() {
            let combined_pred = if or_predicates.len() == 1 {
                or_predicates.remove(0)
            } else {
                Predicate::Or(or_predicates)
            };
            current_node = builder.filter(current_node, combined_pred);
        }

        // Apply LIMIT/OFFSET if specified
        let limited = builder.limit_offset(
            current_node,
            select.limit,
            select.offset.unwrap_or(0),
        );

        Ok(builder.output(limited, GraphId(0)))
    }

    /// Build a query graph with RecursiveFilter for self-referential INHERITS.
    ///
    /// This handles policies like `owner_id = @viewer OR INHERITS SELECT FROM parent_id`
    /// where `parent_id` references the same table. Uses fixpoint iteration to compute
    /// the transitive closure of accessible rows.
    fn build_recursive_filter_graph(
        &self,
        select: &Select,
        viewer: ObjectId,
        inherits: &InheritsInfo,
        schema: &TableSchema,
    ) -> Result<crate::sql::query_graph::QueryGraph, DatabaseError> {
        let table = &select.from.table;

        // Build the base predicate from the non-INHERITS part of the policy
        let base_predicate = if let Some(ref base_expr) = inherits.base_predicate {
            self.policy_expr_to_predicate(base_expr, viewer)?
        } else {
            // No base predicate means only INHERITS - pure recursive access
            // This would mean no rows are directly accessible, only inherited
            Predicate::False
        };

        let mut builder = QueryGraphBuilder::new(table, schema.clone());

        // Start with table scan
        let scan = builder.table_scan();

        // Apply user's WHERE clause first (if any)
        let after_user_where = if select.where_clause.is_empty() {
            scan
        } else {
            let predicate = self.build_predicate(&select.where_clause, schema)?;
            builder.filter(scan, predicate)
        };

        // Apply any additional predicates from AND clauses (non-INHERITS parts)
        let after_additional = if inherits.additional_predicates.is_empty() {
            after_user_where
        } else {
            let mut combined = Predicate::True;
            for expr in &inherits.additional_predicates {
                let pred = self.policy_expr_to_predicate(expr, viewer)?;
                combined = combined.and(pred);
            }
            builder.filter(after_user_where, combined)
        };

        // Add RecursiveFilter node for the self-referential policy
        let recursive =
            builder.recursive_filter(after_additional, base_predicate, &inherits.ref_column);

        // Apply LIMIT/OFFSET if specified
        let limited = builder.limit_offset(
            recursive,
            select.limit,
            select.offset.unwrap_or(0),
        );

        Ok(builder.output(limited, GraphId(0)))
    }

    /// Convert a PolicyExpr to a Predicate with qualified column names.
    ///
    /// This is used when flattening INHERITS - the target table's policy columns
    /// need to be prefixed with the table name for the JOIN context.
    fn policy_expr_to_predicate_qualified(
        &self,
        expr: &PolicyExpr,
        viewer: ObjectId,
        table_prefix: &str,
    ) -> Result<Predicate, DatabaseError> {
        match expr {
            PolicyExpr::Eq(left, right) => {
                let (column, value) =
                    self.resolve_policy_comparison_qualified(left, right, viewer, table_prefix)?;
                Ok(Predicate::eq(column, value))
            }
            PolicyExpr::Ne(left, right) => {
                let (column, value) =
                    self.resolve_policy_comparison_qualified(left, right, viewer, table_prefix)?;
                Ok(Predicate::ne(column, value))
            }
            PolicyExpr::And(exprs) => {
                let predicates: Result<Vec<_>, _> = exprs
                    .iter()
                    .map(|e| self.policy_expr_to_predicate_qualified(e, viewer, table_prefix))
                    .collect();
                Ok(predicates?
                    .into_iter()
                    .fold(Predicate::True, |acc, p| acc.and(p)))
            }
            PolicyExpr::Or(exprs) => {
                let predicates: Result<Vec<_>, _> = exprs
                    .iter()
                    .map(|e| self.policy_expr_to_predicate_qualified(e, viewer, table_prefix))
                    .collect();
                Ok(predicates?
                    .into_iter()
                    .fold(Predicate::False, |acc, p| acc.or(p)))
            }
            PolicyExpr::Not(inner) => Ok(self
                .policy_expr_to_predicate_qualified(inner, viewer, table_prefix)?
                .not()),
            PolicyExpr::Inherits { .. } => {
                // Nested INHERITS - would need recursive flattening
                Err(DatabaseError::Parse(parser::ParseError {
                    message: "Nested INHERITS not yet supported in incremental queries".to_string(),
                    position: 0,
                }))
            }
            _ => Err(DatabaseError::Parse(parser::ParseError {
                message: "Unsupported policy expression in INHERITS target".to_string(),
                position: 0,
            })),
        }
    }

    /// Resolve a policy comparison with qualified column names for JOIN context.
    fn resolve_policy_comparison_qualified(
        &self,
        left: &PolicyValue,
        right: &PolicyValue,
        viewer: ObjectId,
        table_prefix: &str,
    ) -> Result<(String, Value), DatabaseError> {
        match (left, right) {
            (PolicyValue::Column(col), PolicyValue::Viewer) => {
                Ok((format!("{}.{}", table_prefix, col), Value::Ref(viewer)))
            }
            (PolicyValue::Viewer, PolicyValue::Column(col)) => {
                Ok((format!("{}.{}", table_prefix, col), Value::Ref(viewer)))
            }
            (PolicyValue::Column(col), PolicyValue::Literal(val)) => {
                Ok((format!("{}.{}", table_prefix, col), val.clone()))
            }
            (PolicyValue::Literal(val), PolicyValue::Column(col)) => {
                Ok((format!("{}.{}", table_prefix, col), val.clone()))
            }
            _ => Err(DatabaseError::Parse(parser::ParseError {
                message: "Unsupported policy comparison pattern in INHERITS target".to_string(),
                position: 0,
            })),
        }
    }

    /// Convert a PolicyExpr to a Predicate.
    ///
    /// Returns Ok(Predicate) for simple expressions that can be evaluated statically.
    /// Returns Err for expressions containing INHERITS (which require runtime evaluation).
    fn policy_expr_to_predicate(
        &self,
        expr: &PolicyExpr,
        viewer: ObjectId,
    ) -> Result<Predicate, DatabaseError> {
        match expr {
            PolicyExpr::Eq(left, right) => {
                let (column, value) = self.resolve_policy_comparison(left, right, viewer)?;
                Ok(Predicate::eq(column, value))
            }
            PolicyExpr::Ne(left, right) => {
                let (column, value) = self.resolve_policy_comparison(left, right, viewer)?;
                Ok(Predicate::ne(column, value))
            }
            PolicyExpr::And(exprs) => {
                let predicates: Result<Vec<_>, _> = exprs
                    .iter()
                    .map(|e| self.policy_expr_to_predicate(e, viewer))
                    .collect();
                let predicates = predicates?;
                Ok(predicates
                    .into_iter()
                    .fold(Predicate::True, |acc, p| acc.and(p)))
            }
            PolicyExpr::Or(exprs) => {
                let predicates: Result<Vec<_>, _> = exprs
                    .iter()
                    .map(|e| self.policy_expr_to_predicate(e, viewer))
                    .collect();
                let predicates = predicates?;
                Ok(predicates
                    .into_iter()
                    .fold(Predicate::False, |acc, p| acc.or(p)))
            }
            PolicyExpr::Not(inner) => {
                let pred = self.policy_expr_to_predicate(inner, viewer)?;
                Ok(pred.not())
            }
            // These require runtime evaluation
            PolicyExpr::Inherits { .. } => Err(DatabaseError::Parse(parser::ParseError {
                message: "INHERITS cannot be converted to static predicate".to_string(),
                position: 0,
            })),
            // Comparison operators not yet supported in Predicate
            PolicyExpr::Lt(_, _)
            | PolicyExpr::Le(_, _)
            | PolicyExpr::Gt(_, _)
            | PolicyExpr::Ge(_, _) => Err(DatabaseError::Parse(parser::ParseError {
                message: "Comparison operators not yet supported in incremental queries"
                    .to_string(),
                position: 0,
            })),
            PolicyExpr::IsNull(_) | PolicyExpr::IsNotNull(_) => {
                Err(DatabaseError::Parse(parser::ParseError {
                    message: "NULL checks not yet supported in incremental queries".to_string(),
                    position: 0,
                }))
            }
        }
    }

    /// Resolve a policy comparison to (column_name, value).
    ///
    /// Handles patterns like:
    /// - `column = @viewer` -> (column, Ref(viewer))
    /// - `column = literal` -> (column, literal)
    /// - `@viewer = column` -> (column, Ref(viewer))
    fn resolve_policy_comparison(
        &self,
        left: &PolicyValue,
        right: &PolicyValue,
        viewer: ObjectId,
    ) -> Result<(String, Value), DatabaseError> {
        match (left, right) {
            // column = @viewer
            (PolicyValue::Column(col), PolicyValue::Viewer) => {
                Ok((col.clone(), Value::Ref(viewer)))
            }
            // @viewer = column
            (PolicyValue::Viewer, PolicyValue::Column(col)) => {
                Ok((col.clone(), Value::Ref(viewer)))
            }
            // column = literal
            (PolicyValue::Column(col), PolicyValue::Literal(val)) => Ok((col.clone(), val.clone())),
            // literal = column
            (PolicyValue::Literal(val), PolicyValue::Column(col)) => Ok((col.clone(), val.clone())),
            // @new.column = @viewer (for INSERT CHECK, but in WHERE context treat as column)
            (PolicyValue::NewColumn(col), PolicyValue::Viewer)
            | (PolicyValue::Viewer, PolicyValue::NewColumn(col)) => {
                // In SELECT context, @new doesn't apply - this is a misconfigured policy
                Err(DatabaseError::Parse(parser::ParseError {
                    message: format!("@new.{} not valid in SELECT policy WHERE clause", col),
                    position: 0,
                }))
            }
            // @old.column - not valid in SELECT
            (PolicyValue::OldColumn(col), _) | (_, PolicyValue::OldColumn(col)) => {
                Err(DatabaseError::Parse(parser::ParseError {
                    message: format!("@old.{} not valid in SELECT policy WHERE clause", col),
                    position: 0,
                }))
            }
            // Other combinations not supported
            _ => Err(DatabaseError::Parse(parser::ParseError {
                message: "Unsupported policy comparison pattern".to_string(),
                position: 0,
            })),
        }
    }

    /// Build a query graph for a single-table SELECT.
    fn build_single_table_graph(
        &self,
        select: &Select,
    ) -> Result<crate::sql::query_graph::QueryGraph, DatabaseError> {
        let table = &select.from.table;
        let outer_alias = select.from.alias.as_deref();

        // Validate table exists and get schema
        let schema = self
            .get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.clone()))?;

        // Build the query graph
        let mut builder = QueryGraphBuilder::new(table, schema.clone());

        // Start with table scan
        let scan = builder.table_scan();

        // Apply WHERE filters
        let filtered = if select.where_clause.is_empty() {
            scan
        } else {
            // Convert WHERE conditions to Predicate
            let predicate = self.build_predicate(&select.where_clause, &schema)?;
            builder.filter(scan, predicate)
        };

        // Apply LIMIT/OFFSET if specified
        let limited = builder.limit_offset(
            filtered,
            select.limit,
            select.offset.unwrap_or(0),
        );

        // Process ARRAY subqueries in projection
        let with_arrays = if let Projection::Expressions(exprs) = &select.projection {
            self.add_array_aggregates(&mut builder, limited, exprs, table, outer_alias)?
        } else {
            limited
        };

        // Create output node
        Ok(builder.output(with_arrays, GraphId(0))) // ID will be assigned by registry
    }

    /// Add ArrayAggregate nodes for ARRAY subqueries in the projection.
    fn add_array_aggregates(
        &self,
        builder: &mut QueryGraphBuilder,
        input: crate::sql::query_graph::NodeId,
        exprs: &[SelectExpr],
        outer_table: &str,
        outer_alias: Option<&str>,
    ) -> Result<crate::sql::query_graph::NodeId, DatabaseError> {
        let mut current = input;

        for expr in exprs.iter() {
            // Always append arrays at the end (-1), not at the expression index.
            // The expression index doesn't correspond to the actual column position
            // because star expressions expand to multiple columns.
            current = self.add_array_aggregate_for_expr(
                builder, current, expr, outer_table, outer_alias, -1,
            )?;
        }

        Ok(current)
    }

    /// Add ArrayAggregate node for a single expression if it's an ARRAY subquery.
    fn add_array_aggregate_for_expr(
        &self,
        builder: &mut QueryGraphBuilder,
        input: crate::sql::query_graph::NodeId,
        expr: &SelectExpr,
        outer_table: &str,
        outer_alias: Option<&str>,
        column_index: i32,
    ) -> Result<crate::sql::query_graph::NodeId, DatabaseError> {
        match expr {
            SelectExpr::ArraySubquery(subquery) => {
                let inner_table = &subquery.from.table;
                let inner_schema = self
                    .get_table(inner_table)
                    .ok_or_else(|| DatabaseError::TableNotFound(inner_table.clone()))?;

                // Find the ref column from WHERE clause
                // e.g., WHERE n.issue = i.id → ref_column is "issue"
                let ref_column = self.find_array_subquery_ref_column(
                    &subquery.where_clause,
                    inner_table,
                    subquery.from.alias.as_deref(),
                    outer_table,
                    outer_alias,
                )?;

                // Extract inner joins from the ARRAY subquery
                // e.g., ARRAY(SELECT ... FROM IssueLabels il JOIN Labels ON il.label = Labels.id ...)
                let inner_joins = self.extract_inner_joins(
                    &subquery.from.joins,
                    inner_table,
                    subquery.from.alias.as_deref(),
                    &inner_schema,
                )?;

                Ok(builder.array_aggregate(
                    input,
                    inner_table.clone(),
                    ref_column,
                    inner_schema.clone(),
                    inner_joins,
                    column_index,
                ))
            }
            SelectExpr::Aliased { expr: inner, .. } => {
                // Recurse into aliased expressions
                self.add_array_aggregate_for_expr(
                    builder, input, inner, outer_table, outer_alias, column_index,
                )
            }
            // Non-ARRAY expressions don't add nodes
            _ => Ok(input),
        }
    }

    /// Find the reference column in an ARRAY subquery WHERE clause.
    /// Expects a condition like: inner.ref_col = outer.id
    fn find_array_subquery_ref_column(
        &self,
        where_clause: &[Condition],
        inner_table: &str,
        inner_alias: Option<&str>,
        outer_table: &str,
        outer_alias: Option<&str>,
    ) -> Result<String, DatabaseError> {
        for cond in where_clause {
            // Condition is a struct with `column` and `right` fields
            // column = the left-hand side column
            // right = ConditionValue (could be a Literal or Column)
            let left_col = &cond.column;

            // Check if right side is a column reference
            if let ConditionValue::Column(right_col) = &cond.right {
                // Check for: inner.ref_col = outer.id
                let left_is_inner = left_col.table.as_deref() == Some(inner_table)
                    || left_col.table.as_deref() == inner_alias;
                let right_is_outer = right_col.table.as_deref() == Some(outer_table)
                    || right_col.table.as_deref() == outer_alias;

                if left_is_inner && right_is_outer && right_col.column == "id" {
                    return Ok(left_col.column.clone());
                }

                // Check reverse: outer.id = inner.ref_col
                let left_is_outer = left_col.table.as_deref() == Some(outer_table)
                    || left_col.table.as_deref() == outer_alias;
                let right_is_inner = right_col.table.as_deref() == Some(inner_table)
                    || right_col.table.as_deref() == inner_alias;

                if left_is_outer && right_is_inner && left_col.column == "id" {
                    return Ok(right_col.column.clone());
                }
            }
        }

        Err(DatabaseError::Parse(parser::ParseError {
            message: format!(
                "ARRAY subquery must have WHERE clause referencing outer table.id (inner: {}, outer: {})",
                inner_table, outer_table
            ),
            position: 0,
        }))
    }

    /// Extract inner JOINs from an ARRAY subquery.
    /// For each JOIN, returns (ref_column, target_table, target_schema).
    /// e.g., JOIN Labels ON il.label = Labels.id → ("label", "Labels", Labels schema)
    fn extract_inner_joins(
        &self,
        joins: &[parser::Join],
        inner_table: &str,
        inner_alias: Option<&str>,
        inner_schema: &TableSchema,
    ) -> Result<Vec<(String, String, TableSchema)>, DatabaseError> {
        let mut result = Vec::new();

        for join in joins {
            let target_table = &join.table;
            let target_schema = self
                .get_table(target_table)
                .ok_or_else(|| DatabaseError::TableNotFound(target_table.clone()))?;

            // Find which column in the inner table references the join target
            // Check ON clause: inner.col = target.id or target.id = inner.col
            let ref_column = self.find_join_ref_column(
                &join.on,
                inner_table,
                inner_alias,
                target_table,
                inner_schema,
            )?;

            result.push((ref_column, target_table.clone(), target_schema.clone()));
        }

        Ok(result)
    }

    /// Find the ref column in a JOIN ON clause.
    /// Expects: inner.ref_col = target.id or target.id = inner.ref_col
    fn find_join_ref_column(
        &self,
        on: &parser::JoinCondition,
        inner_table: &str,
        inner_alias: Option<&str>,
        target_table: &str,
        inner_schema: &TableSchema,
    ) -> Result<String, DatabaseError> {
        // Helper to check if a table reference matches inner table (by name or alias)
        let matches_inner =
            |t: &str| t == inner_table || inner_alias.map(|a| a == t).unwrap_or(false);
        let matches_target = |t: &str| t == target_table;

        // Check: inner.col = target.id
        let left_is_inner = on
            .left
            .table
            .as_ref()
            .map(|t| matches_inner(t))
            .unwrap_or(false);
        let right_is_target = on
            .right
            .table
            .as_ref()
            .map(|t| matches_target(t))
            .unwrap_or(false);

        if left_is_inner && right_is_target && on.right.column == "id" {
            // Verify it's actually a Ref column to the target
            let col_name = &on.left.column;
            if let Some(col) = inner_schema.column(col_name) {
                if matches!(&col.ty, ColumnType::Ref(t) if t == target_table) {
                    return Ok(col_name.clone());
                }
            }
        }

        // Check reverse: target.id = inner.col
        let left_is_target = on
            .left
            .table
            .as_ref()
            .map(|t| matches_target(t))
            .unwrap_or(false);
        let right_is_inner = on
            .right
            .table
            .as_ref()
            .map(|t| matches_inner(t))
            .unwrap_or(false);

        if left_is_target && right_is_inner && on.left.column == "id" {
            let col_name = &on.right.column;
            if let Some(col) = inner_schema.column(col_name) {
                if matches!(&col.ty, ColumnType::Ref(t) if t == target_table) {
                    return Ok(col_name.clone());
                }
            }
        }

        Err(DatabaseError::Parse(parser::ParseError {
            message: format!(
                "Could not find ref column for JOIN {} in inner table {}",
                target_table, inner_table
            ),
            position: 0,
        }))
    }

    /// Build a query graph for a JOIN SELECT.
    fn build_join_graph(
        &self,
        select: &Select,
    ) -> Result<crate::sql::query_graph::QueryGraph, DatabaseError> {
        if select.from.joins.is_empty() {
            return Err(DatabaseError::Parse(parser::ParseError {
                message: "build_join_graph called without JOINs".to_string(),
                position: 0,
            }));
        }

        let first_join = &select.from.joins[0];
        let sql_left_table = &select.from.table;
        let sql_first_right_table = &first_join.table;

        // Get schemas for the first two tables
        let sql_left_schema = self
            .get_table(sql_left_table)
            .ok_or_else(|| DatabaseError::TableNotFound(sql_left_table.clone()))?;
        let sql_first_right_schema = self
            .get_table(sql_first_right_table)
            .ok_or_else(|| DatabaseError::TableNotFound(sql_first_right_table.clone()))?;

        // Determine the join column and direction for first join
        // The JoinGraphBuilder expects the "left" table to have the Ref column
        let first_join_direction = self.find_join_column(
            &first_join.on,
            sql_left_table,
            select.from.alias.as_deref(), // Pass FROM alias for matching
            sql_first_right_table,
            &sql_left_schema,
            &sql_first_right_schema,
        )?;

        // For the graph builder, we need the table with the Ref to be "left"
        // If it's a reverse join (right table has Ref), we swap the roles
        let (graph_left_table, graph_left_schema, graph_right_table, graph_right_schema, ref_column) =
            match &first_join_direction {
                JoinDirection::LeftToRight(col) => (
                    sql_left_table.as_str(),
                    sql_left_schema.clone(),
                    sql_first_right_table.as_str(),
                    sql_first_right_schema.clone(),
                    col.clone(),
                ),
                JoinDirection::RightToLeft(col) => (
                    sql_first_right_table.as_str(),
                    sql_first_right_schema.clone(),
                    sql_left_table.as_str(),
                    sql_left_schema.clone(),
                    col.clone(),
                ),
            };

        // Build the JOIN query graph
        let mut builder = JoinGraphBuilder::new(
            graph_left_table,
            graph_left_schema.clone(),
            graph_right_table,
            graph_right_schema.clone(),
            &ref_column,
        );

        // For reverse JOINs, set projection to output only the SQL FROM table's columns.
        // The SQL is `SELECT Issues.* FROM Issues JOIN IssueAssignees`, but we swapped the
        // tables for the graph builder (because IssueAssignees has the Ref). We need to
        // project back to Issues (the original SQL left table, now graph_right_table).
        if matches!(first_join_direction, JoinDirection::RightToLeft(_)) {
            builder.set_projection(graph_right_table);
        }

        // Start with first join node
        let mut current_node = builder.join();

        // Track all tables involved for predicate building
        // Store (table_name, alias, schema)
        let from_alias = select.from.alias.as_deref();
        let mut all_tables: Vec<(&str, Option<&str>, TableSchema)> = vec![
            (sql_left_table.as_str(), from_alias, sql_left_schema.clone()),
            (sql_first_right_table.as_str(), None, sql_first_right_schema.clone()),
        ];

        // Track reverse-joined tables to handle their WHERE conditions specially
        let mut reverse_joined_tables: std::collections::HashSet<String> = std::collections::HashSet::new();

        // Process additional JOINs (chain joins)
        for join in select.from.joins.iter().skip(1) {
            let target_table = &join.table;
            let target_schema = self
                .get_table(target_table)
                .ok_or_else(|| DatabaseError::TableNotFound(target_table.clone()))?;

            // Add schema so chain_join can find it
            builder.add_schema(target_table.clone(), target_schema.clone());

            // Determine which table in the chain has the ref column
            let join_info = self.find_chain_join_info(
                &join.on,
                &all_tables,
                target_table,
                &target_schema,
            )?;

            current_node = match join_info {
                ChainJoinInfo::Forward { source_table, ref_column } => {
                    builder.chain_join(
                        current_node,
                        source_table,
                        ref_column,
                        target_table.clone(),
                    )
                }
                ChainJoinInfo::Reverse { existing_table, ref_column } => {
                    // For reverse chain joins, the target table has the ref column
                    // pointing to an existing table.
                    reverse_joined_tables.insert(target_table.clone());

                    // Extract WHERE conditions that apply to this reverse-joined table
                    let reverse_filter = self.extract_table_conditions(
                        &select.where_clause,
                        target_table,
                        &target_schema,
                    )?;

                    builder.reverse_chain_join_with_filter(
                        current_node,
                        existing_table,
                        ref_column,
                        target_table.clone(),
                        reverse_filter,
                    )
                }
            };

            all_tables.push((target_table.as_str(), None, target_schema.clone()));
        }

        // Apply WHERE filters (if any), excluding conditions already handled by reverse joins
        let filtered = if select.where_clause.is_empty() {
            current_node
        } else {
            // Filter out conditions on reverse-joined tables (already handled)
            let remaining_conditions: Vec<_> = select.where_clause.iter()
                .filter(|cond| !self.condition_on_table(cond, &reverse_joined_tables))
                .cloned()
                .collect();

            if remaining_conditions.is_empty() {
                current_node
            } else {
                // Build predicate for remaining conditions only (with alias support)
                let predicate = self.build_multi_join_predicate_with_aliases(&remaining_conditions, &all_tables)?;
                builder.filter(current_node, predicate)
            }
        };

        // Apply LIMIT/OFFSET if specified
        let limited = builder.limit_offset(
            filtered,
            select.limit,
            select.offset.unwrap_or(0),
        );

        // Process ARRAY subqueries in projection (for reverse refs with includes)
        let outer_table = sql_left_table;
        let outer_alias = select.from.alias.as_deref();
        let with_arrays = if let Projection::Expressions(exprs) = &select.projection {
            self.add_join_array_aggregates(&mut builder, limited, exprs, outer_table, outer_alias)?
        } else {
            limited
        };

        // Create output node
        Ok(builder.output(with_arrays, GraphId(0))) // ID will be assigned by registry
    }

    /// Find chain join information: which table has the ref column and what direction.
    fn find_chain_join_info(
        &self,
        on: &parser::JoinCondition,
        existing_tables: &[(&str, Option<&str>, TableSchema)],
        target_table: &str,
        target_schema: &TableSchema,
    ) -> Result<ChainJoinInfo, DatabaseError> {
        // Helper to check if a reference matches a table (by name or alias)
        let matches_table = |ref_name: Option<&str>, table_name: &str, alias: Option<&str>| {
            ref_name == Some(table_name) || (alias.is_some() && ref_name == alias)
        };

        // Check ON clause: existing.col = target.id (forward ref)
        for (table_name, alias, schema) in existing_tables {
            let left_is_existing = matches_table(on.left.table.as_deref(), table_name, *alias);
            let right_is_target = on.right.table.as_deref() == Some(target_table);

            if left_is_existing && right_is_target && on.right.column == "id" {
                let col_name = &on.left.column;
                if let Some(col) = schema.column(col_name) {
                    if matches!(&col.ty, ColumnType::Ref(t) if t == target_table) {
                        return Ok(ChainJoinInfo::Forward {
                            source_table: table_name.to_string(),
                            ref_column: col_name.clone(),
                        });
                    }
                }
            }

            // Check reverse: target.id = existing.col (forward ref, swapped)
            let left_is_target = on.left.table.as_deref() == Some(target_table);
            let right_is_existing = matches_table(on.right.table.as_deref(), table_name, *alias);

            if left_is_target && right_is_existing && on.left.column == "id" {
                let col_name = &on.right.column;
                if let Some(col) = schema.column(col_name) {
                    if matches!(&col.ty, ColumnType::Ref(t) if t == target_table) {
                        return Ok(ChainJoinInfo::Forward {
                            source_table: table_name.to_string(),
                            ref_column: col_name.clone(),
                        });
                    }
                }
            }
        }

        // Check for reverse ref: target.col = existing.id (target has ref to existing)
        for (table_name, alias, _schema) in existing_tables {
            let left_is_target = on.left.table.as_deref() == Some(target_table);
            let right_is_existing = matches_table(on.right.table.as_deref(), table_name, *alias);

            if left_is_target && right_is_existing && on.right.column == "id" {
                let col_name = &on.left.column;
                if let Some(col) = target_schema.column(col_name) {
                    if matches!(&col.ty, ColumnType::Ref(t) if t == *table_name) {
                        // This is a reverse join - the target table has the ref
                        return Ok(ChainJoinInfo::Reverse {
                            existing_table: table_name.to_string(),
                            ref_column: col_name.clone(),
                        });
                    }
                }
            }

            // Also check: existing.id = target.col (swapped)
            let left_is_existing = matches_table(on.left.table.as_deref(), table_name, *alias);
            let right_is_target = on.right.table.as_deref() == Some(target_table);

            if left_is_existing && right_is_target && on.left.column == "id" {
                let col_name = &on.right.column;
                if let Some(col) = target_schema.column(col_name) {
                    if matches!(&col.ty, ColumnType::Ref(t) if t == *table_name) {
                        // This is a reverse join - the target table has the ref
                        return Ok(ChainJoinInfo::Reverse {
                            existing_table: table_name.to_string(),
                            ref_column: col_name.clone(),
                        });
                    }
                }
            }
        }

        Err(DatabaseError::Parse(parser::ParseError {
            message: format!(
                "Could not find ref column for chain JOIN with {}",
                target_table
            ),
            position: 0,
        }))
    }

    /// Build predicate for multi-join queries.
    fn build_multi_join_predicate(
        &self,
        conditions: &[Condition],
        all_schemas: &[(&str, TableSchema)],
    ) -> Result<Predicate, DatabaseError> {
        let mut predicates = Vec::new();

        for cond in conditions {
            let table = cond.column.table.as_deref();
            let col_name = &cond.column.column;

            // Find the schema for this column
            let (table_name, schema) = if let Some(t) = table {
                all_schemas
                    .iter()
                    .find(|(name, _)| *name == t)
                    .ok_or_else(|| DatabaseError::Parse(parser::ParseError {
                        message: format!("Unknown table {} in WHERE clause", t),
                        position: 0,
                    }))?
            } else {
                // Unqualified column - search all schemas
                // "id" is a special column that exists on every table
                if col_name == "id" {
                    all_schemas.first().ok_or_else(|| DatabaseError::Parse(parser::ParseError {
                        message: "No tables in query".to_string(),
                        position: 0,
                    }))?
                } else {
                    all_schemas
                        .iter()
                        .find(|(_, s)| s.column(col_name).is_some())
                        .ok_or_else(|| DatabaseError::Parse(parser::ParseError {
                            message: format!("Unknown column {} in WHERE clause", col_name),
                            position: 0,
                        }))?
                }
            };

            // "id" is a special column (ObjectId/String type) that exists on every table
            let column_type = if col_name == "id" {
                ColumnType::String
            } else {
                let column = schema.column(col_name).ok_or_else(|| {
                    DatabaseError::Parse(parser::ParseError {
                        message: format!("Column {} not found in table {}", col_name, table_name),
                        position: 0,
                    })
                })?;
                column.ty.clone()
            };

            // Only handle literal values in predicates for now
            let literal_value = match cond.value() {
                Some(v) => v.clone(),
                None => {
                    // Column references not yet supported in predicate building
                    continue;
                }
            };

            let value = coerce_value(literal_value, &column_type);

            // Use qualified column name for multi-table queries
            let qualified_col = format!("{}.{}", table_name, col_name);
            predicates.push(Predicate::eq(qualified_col, value));
        }

        if predicates.is_empty() {
            Ok(Predicate::True)
        } else if predicates.len() == 1 {
            Ok(predicates.pop().unwrap())
        } else {
            Ok(Predicate::And(predicates))
        }
    }

    /// Build predicate for multi-join queries with alias support.
    ///
    /// This version takes the full table info including aliases, allowing
    /// WHERE clauses like `i.priority = 'low'` where `i` is an alias for `Issues`.
    fn build_multi_join_predicate_with_aliases(
        &self,
        conditions: &[Condition],
        all_tables: &[(&str, Option<&str>, TableSchema)],
    ) -> Result<Predicate, DatabaseError> {
        let mut predicates = Vec::new();

        for cond in conditions {
            let table_ref = cond.column.table.as_deref();
            let col_name = &cond.column.column;

            // Find the schema for this column (check both table name and alias)
            let (table_name, _schema) = if let Some(t) = table_ref {
                all_tables
                    .iter()
                    .find(|(name, alias, _)| {
                        *name == t || alias.map_or(false, |a| a == t)
                    })
                    .map(|(name, _, schema)| (*name, schema))
                    .ok_or_else(|| DatabaseError::Parse(parser::ParseError {
                        message: format!("Unknown table {} in WHERE clause", t),
                        position: 0,
                    }))?
            } else {
                // Unqualified column - search all schemas
                // "id" is a special column that exists on every table
                if col_name == "id" {
                    all_tables.first()
                        .map(|(name, _, schema)| (*name, schema))
                        .ok_or_else(|| DatabaseError::Parse(parser::ParseError {
                            message: "No tables in query".to_string(),
                            position: 0,
                        }))?
                } else {
                    all_tables
                        .iter()
                        .find(|(_, _, s)| s.column(col_name).is_some())
                        .map(|(name, _, schema)| (*name, schema))
                        .ok_or_else(|| DatabaseError::Parse(parser::ParseError {
                            message: format!("Unknown column {} in WHERE clause", col_name),
                            position: 0,
                        }))?
                }
            };

            // "id" is a special column (ObjectId/String type) that exists on every table
            let column_type = if col_name == "id" {
                ColumnType::String
            } else {
                let column = _schema.column(col_name).ok_or_else(|| {
                    DatabaseError::Parse(parser::ParseError {
                        message: format!("Column {} not found in table {}", col_name, table_name),
                        position: 0,
                    })
                })?;
                column.ty.clone()
            };

            // Only handle literal values in predicates for now
            let literal_value = match cond.value() {
                Some(v) => v.clone(),
                None => {
                    // Column references not yet supported in predicate building
                    continue;
                }
            };

            let value = coerce_value(literal_value, &column_type);

            // Use actual table name (not alias) for qualified column
            let qualified_col = format!("{}.{}", table_name, col_name);
            predicates.push(Predicate::eq(qualified_col, value));
        }

        if predicates.is_empty() {
            Ok(Predicate::True)
        } else if predicates.len() == 1 {
            Ok(predicates.pop().unwrap())
        } else {
            Ok(Predicate::And(predicates))
        }
    }

    /// Extract WHERE conditions that apply to a specific table.
    ///
    /// Returns a Predicate if any conditions apply to the table, None otherwise.
    /// Used to pass filter conditions to reverse joins for EXISTS-style filtering.
    fn extract_table_conditions(
        &self,
        conditions: &[Condition],
        target_table: &str,
        target_schema: &TableSchema,
    ) -> Result<Option<Predicate>, DatabaseError> {
        let mut predicates = Vec::new();

        for cond in conditions {
            let table = cond.column.table.as_deref();
            let col_name = &cond.column.column;

            // Check if this condition applies to the target table
            // "id" is a special column that exists on every table
            let applies = match table {
                Some(t) => t == target_table,
                None => {
                    // Unqualified column - check if it's in target schema
                    col_name == "id" || target_schema.column(col_name).is_some()
                }
            };

            if !applies {
                continue;
            }

            // "id" is a special column (ObjectId/String type) that exists on every table
            let column_type = if col_name == "id" {
                ColumnType::String
            } else {
                let column = target_schema.column(col_name).ok_or_else(|| {
                    DatabaseError::Parse(parser::ParseError {
                        message: format!("Column {} not found in table {}", col_name, target_table),
                        position: 0,
                    })
                })?;
                column.ty.clone()
            };

            // Only handle literal values for now
            let literal_value = match cond.value() {
                Some(v) => v.clone(),
                None => continue,
            };

            let value = coerce_value(literal_value, &column_type);

            // Use unqualified column name (the join table's schema doesn't have qualified names)
            predicates.push(Predicate::eq(col_name, value));
        }

        if predicates.is_empty() {
            Ok(None)
        } else if predicates.len() == 1 {
            Ok(Some(predicates.pop().unwrap()))
        } else {
            Ok(Some(Predicate::And(predicates)))
        }
    }

    /// Check if a condition applies to any of the specified tables.
    fn condition_on_table(
        &self,
        cond: &Condition,
        tables: &std::collections::HashSet<String>,
    ) -> bool {
        match cond.column.table.as_deref() {
            Some(t) => tables.contains(t),
            None => false, // Unqualified columns are ambiguous, keep them
        }
    }

    /// Add ArrayAggregate nodes to a JOIN graph for ARRAY subqueries in projection.
    fn add_join_array_aggregates(
        &self,
        builder: &mut JoinGraphBuilder,
        input: crate::sql::query_graph::NodeId,
        exprs: &[SelectExpr],
        outer_table: &str,
        outer_alias: Option<&str>,
    ) -> Result<crate::sql::query_graph::NodeId, DatabaseError> {
        let mut current = input;

        for expr in exprs.iter() {
            // Always append arrays at the end (-1), not at the expression index.
            // The expression index doesn't correspond to the actual column position
            // because star expressions expand to multiple columns and joins add more columns.
            current = self.add_join_array_aggregate_for_expr(
                builder, current, expr, outer_table, outer_alias, -1,
            )?;
        }

        Ok(current)
    }

    /// Add ArrayAggregate node to a JOIN graph for a single expression.
    fn add_join_array_aggregate_for_expr(
        &self,
        builder: &mut JoinGraphBuilder,
        input: crate::sql::query_graph::NodeId,
        expr: &SelectExpr,
        outer_table: &str,
        outer_alias: Option<&str>,
        column_index: i32,
    ) -> Result<crate::sql::query_graph::NodeId, DatabaseError> {
        match expr {
            SelectExpr::ArraySubquery(subquery) => {
                let inner_table = &subquery.from.table;
                let inner_schema = self
                    .get_table(inner_table)
                    .ok_or_else(|| DatabaseError::TableNotFound(inner_table.clone()))?;

                // Find the ref column from WHERE clause
                let ref_column = self.find_array_subquery_ref_column(
                    &subquery.where_clause,
                    inner_table,
                    subquery.from.alias.as_deref(),
                    outer_table,
                    outer_alias,
                )?;

                // Extract inner joins from the ARRAY subquery
                let inner_joins = self.extract_inner_joins(
                    &subquery.from.joins,
                    inner_table,
                    subquery.from.alias.as_deref(),
                    &inner_schema,
                )?;

                Ok(builder.array_aggregate(
                    input,
                    inner_table.clone(),
                    ref_column,
                    inner_schema.clone(),
                    inner_joins,
                    column_index,
                ))
            }
            SelectExpr::Aliased { expr: inner, .. } => {
                // Recurse into aliased expressions
                self.add_join_array_aggregate_for_expr(
                    builder, input, inner, outer_table, outer_alias, column_index,
                )
            }
            // Non-ARRAY expressions don't add nodes
            _ => Ok(input),
        }
    }

    /// Find the Ref column that connects the two tables in a JOIN.
    /// Returns the column name and which direction the reference goes.
    fn find_join_column(
        &self,
        on: &parser::JoinCondition,
        left_table: &str,
        left_alias: Option<&str>, // FROM clause alias (e.g., "i" for "Issues i")
        right_table: &str,
        left_schema: &TableSchema,
        right_schema: &TableSchema,
    ) -> Result<JoinDirection, DatabaseError> {
        // Helper to check if a table reference matches (by name or alias)
        let matches_left = |t: &str| t == left_table || left_alias == Some(t);
        let matches_right = |t: &str| t == right_table;

        // Check if the left side of the ON clause references the left table
        let left_is_from_left = on
            .left
            .table
            .as_ref()
            .map(|t| matches_left(t))
            .unwrap_or(true);
        let right_is_from_right = on
            .right
            .table
            .as_ref()
            .map(|t| matches_right(t))
            .unwrap_or(true);

        if left_is_from_left && right_is_from_right {
            // ON left_table.col = right_table.id pattern
            // Check if left column is a Ref to right table
            let col_name = &on.left.column;
            if let Some(col) = left_schema.column(col_name) {
                if matches!(&col.ty, ColumnType::Ref(target) if target == right_table) {
                    return Ok(JoinDirection::LeftToRight(col_name.clone()));
                }
            }
        }

        let right_is_from_left = on
            .right
            .table
            .as_ref()
            .map(|t| matches_left(t))
            .unwrap_or(false);
        let left_is_from_right = on
            .left
            .table
            .as_ref()
            .map(|t| matches_right(t))
            .unwrap_or(false);

        if right_is_from_left && left_is_from_right {
            // ON right_table.id = left_table.col pattern
            let col_name = &on.right.column;
            if let Some(col) = left_schema.column(col_name) {
                if matches!(&col.ty, ColumnType::Ref(target) if target == right_table) {
                    return Ok(JoinDirection::LeftToRight(col_name.clone()));
                }
            }
        }

        // Try to find any Ref column in left_schema that points to right_table
        for col in &left_schema.columns {
            if matches!(&col.ty, ColumnType::Ref(target) if target == right_table) {
                return Ok(JoinDirection::LeftToRight(col.name.clone()));
            }
        }

        // Check for reverse join: right table has Ref to left table
        // Pattern: ON right_table.col = left_table.id (or with alias)
        let left_is_from_right_2 = on
            .left
            .table
            .as_ref()
            .map(|t| matches_right(t))
            .unwrap_or(false);
        let right_is_from_left_2 = on
            .right
            .table
            .as_ref()
            .map(|t| matches_left(t))
            .unwrap_or(false);

        if left_is_from_right_2 && right_is_from_left_2 {
            let col_name = &on.left.column;
            if let Some(col) = right_schema.column(col_name) {
                if matches!(&col.ty, ColumnType::Ref(target) if target == left_table) {
                    return Ok(JoinDirection::RightToLeft(col_name.clone()));
                }
            }
        }

        // Also check: ON left_table.id = right_table.col
        if left_is_from_left && right_is_from_right {
            let col_name = &on.right.column;
            if let Some(col) = right_schema.column(col_name) {
                if matches!(&col.ty, ColumnType::Ref(target) if target == left_table) {
                    return Ok(JoinDirection::RightToLeft(col_name.clone()));
                }
            }
        }

        // Try to find any Ref column in right_schema that points to left_table
        for col in &right_schema.columns {
            if matches!(&col.ty, ColumnType::Ref(target) if target == left_table) {
                return Ok(JoinDirection::RightToLeft(col.name.clone()));
            }
        }

        Err(DatabaseError::Parse(parser::ParseError {
            message: format!(
                "Could not find Ref column connecting '{}' and '{}'",
                left_table, right_table
            ),
            position: 0,
        }))
    }

    /// Build a Predicate from SQL WHERE conditions for JOIN queries.
    /// Handles qualified column names (table.column) and resolves aliases.
    fn build_join_predicate(
        &self,
        conditions: &[parser::Condition],
        left_schema: &TableSchema,
        right_schema: &TableSchema,
    ) -> Result<Predicate, DatabaseError> {
        if conditions.is_empty() {
            return Ok(Predicate::True);
        }

        let mut predicates = Vec::new();

        for cond in conditions {
            let column = &cond.column.column;

            // Determine which schema has the column and get the qualified name
            // The combined schema uses qualified names like "Issues.priority"
            let (qualified_column, col_type) = if column == "id" {
                // Special case: id exists in both tables
                // Use the table qualifier to determine which one
                if let Some(_table) = &cond.column.table {
                    // Try to match the qualifier (might be alias) to a schema
                    if left_schema.column_index(column).is_some() {
                        (format!("{}.id", left_schema.name), ColumnType::Ref("".to_string()))
                    } else {
                        (format!("{}.id", right_schema.name), ColumnType::Ref("".to_string()))
                    }
                } else {
                    // Default to left schema for unqualified id
                    (format!("{}.id", left_schema.name), ColumnType::Ref("".to_string()))
                }
            } else if let Some(idx) = left_schema.column_index(column) {
                // Column found in left schema - use qualified name
                let qualified = format!("{}.{}", left_schema.name, column);
                (qualified, left_schema.columns[idx].ty.clone())
            } else if let Some(idx) = right_schema.column_index(column) {
                // Column found in right schema - use qualified name
                let qualified = format!("{}.{}", right_schema.name, column);
                (qualified, right_schema.columns[idx].ty.clone())
            } else {
                return Err(DatabaseError::ColumnNotFound(column.clone()));
            };

            // Only handle literal values in predicates for now
            let literal_value = match cond.value() {
                Some(v) => v.clone(),
                None => {
                    // Column references not yet supported in predicate building
                    return Err(DatabaseError::ColumnNotFound(
                        "column references not supported in join predicate building".to_string(),
                    ));
                }
            };
            let value = coerce_value(literal_value, &col_type);
            predicates.push(Predicate::eq(qualified_column, value));
        }

        // AND all conditions together and optimize
        let combined = predicates
            .into_iter()
            .reduce(|a, b| a.and(b))
            .unwrap_or(Predicate::True);
        Ok(combined.optimize())
    }

    /// Build a Predicate from SQL WHERE conditions.
    fn build_predicate(
        &self,
        conditions: &[parser::Condition],
        schema: &TableSchema,
    ) -> Result<Predicate, DatabaseError> {
        if conditions.is_empty() {
            return Ok(Predicate::True);
        }

        let mut predicates = Vec::new();

        for cond in conditions {
            let column = &cond.column.column;

            // Validate column exists (or is 'id')
            if column != "id" && schema.column_index(column).is_none() {
                return Err(DatabaseError::ColumnNotFound(column.clone()));
            }

            // Only handle literal values in predicates for now
            let literal_value = match cond.value() {
                Some(v) => v.clone(),
                None => {
                    // Column references not yet supported in predicate building
                    return Err(DatabaseError::ColumnNotFound(
                        "column references not supported in predicate building".to_string(),
                    ));
                }
            };

            // Coerce value if needed
            let value = if column == "id" {
                coerce_value(literal_value, &ColumnType::Ref("".to_string()))
            } else {
                let col_idx = schema.column_index(column).unwrap();
                coerce_value(literal_value, &schema.columns[col_idx].ty)
            };

            predicates.push(Predicate::eq(column, value));
        }

        // AND all conditions together and optimize
        let combined = predicates
            .into_iter()
            .reduce(|a, b| a.and(b))
            .unwrap_or(Predicate::True);
        Ok(combined.optimize())
    }
}

/// Get current timestamp in milliseconds.
#[cfg(not(feature = "wasm"))]
fn timestamp_now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Get current timestamp in milliseconds (WASM version).
#[cfg(feature = "wasm")]
fn timestamp_now() -> u64 {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
