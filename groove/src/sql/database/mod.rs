use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::listener::ListenerId;
use crate::node::{generate_object_id, LocalNode};
use crate::object::ObjectId;
use crate::sql::index::RefIndex;
use crate::sql::parser::{self, Condition, Join, Projection, Select, Statement};
use crate::sql::policy::{Policy, PolicyAction, PolicyError, PolicyExpr, PolicyValue, TablePolicies};
use crate::sql::query_graph::registry::{GraphRegistry, OutputCallback};
use crate::sql::query_graph::{GraphId, JoinGraphBuilder, Predicate, PriorState, QueryGraphBuilder, RowDelta};
use crate::sql::row::{decode_row, encode_row, Row, RowError, Value};
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
        self.tables.insert(table.to_string(), (schema.clone(), row.id, row.values));
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
        let table = cond.column.table.as_deref();
        let column = &cond.column.column;

        // Handle special "id" column
        if column == "id" {
            // Coerce String to Ref for id comparison
            let coerced = coerce_value(cond.value.clone(), &ColumnType::Ref("".to_string()));
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
                    let coerced = coerce_value(cond.value.clone(), &schema.columns[idx].ty);
                    return values.get(idx) == Some(&coerced);
                }
            }
        } else {
            // Unqualified column: search all tables
            for (schema, _, values) in self.tables.values() {
                if let Some(idx) = schema.column_index(column) {
                    let coerced = coerce_value(cond.value.clone(), &schema.columns[idx].ty);
                    return values.get(idx) == Some(&coerced);
                }
            }
        }
        false
    }

    /// Convert to output Row with combined values from all tables.
    /// Uses primary table's row ID as the output row ID.
    fn to_output_row(self, projection: &Projection) -> Row {
        let row_id = self.tables.get(&self.primary_table)
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
    /// Returns a `ListenerId` that can be used to unsubscribe.
    pub fn subscribe_delta(&self, callback: OutputCallback) -> Option<ListenerId> {
        self.db_state.graph_registry.subscribe(self.graph_id, callback)
    }

    /// Subscribe to query output changes with a full rows callback.
    ///
    /// This is a convenience wrapper that provides the full current row set
    /// on each change, rather than the delta. Less efficient but simpler.
    #[cfg(not(feature = "wasm"))]
    pub fn subscribe(&self, callback: impl Fn(Vec<Row>) + Send + Sync + 'static) -> Option<ListenerId> {
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
        self.db_state.graph_registry.unsubscribe(self.graph_id, listener_id)
    }

    /// Get the graph ID (for testing/debugging).
    pub fn graph_id(&self) -> GraphId {
        self.graph_id
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
    /// Map from table name to schema object ID.
    tables: RwLock<HashMap<String, SchemaId>>,
    /// Cached schemas by ID.
    schemas: RwLock<HashMap<SchemaId, TableSchema>>,
    /// Map from table name to table rows object ID.
    table_rows_objects: RwLock<HashMap<String, ObjectId>>,
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
            .field("tables", &self.tables.read().unwrap().keys().collect::<Vec<_>>())
            .finish()
    }
}

impl DatabaseState {
    fn new(env: Arc<dyn Environment>) -> Self {
        DatabaseState {
            node: LocalNode::new(env),
            tables: RwLock::new(HashMap::new()),
            schemas: RwLock::new(HashMap::new()),
            table_rows_objects: RwLock::new(HashMap::new()),
            row_table: RwLock::new(HashMap::new()),
            index_objects: RwLock::new(HashMap::new()),
            policies: RwLock::new(HashMap::new()),
            graph_registry: GraphRegistry::new(),
        }
    }

    fn in_memory() -> Self {
        DatabaseState {
            node: LocalNode::in_memory(),
            tables: RwLock::new(HashMap::new()),
            schemas: RwLock::new(HashMap::new()),
            table_rows_objects: RwLock::new(HashMap::new()),
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
                if let Ok(Some(data)) = self.node.read_sync(*rows_id, "main") {
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
            let data = match self.node.read_sync(row_id, "main") {
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

        let data = match self.node.read_sync(id, "main") {
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

        // Get primary table schema
        let primary_schema = match self.get_schema(primary_table) {
            Some(s) => s,
            None => return vec![],
        };

        // Read all rows from primary table
        let primary_rows = self.read_all_rows(primary_table);

        if select.from.joins.is_empty() {
            // Simple case: no JOINs
            primary_rows
                .into_iter()
                .filter(|row| Self::matches_where_simple(&select.where_clause, &row.values, &primary_schema))
                .collect()
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
        }
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
            let (lookup_value, join_column) = if right_table == Some(join.table.as_str()) ||
                (right_table.is_none() && join_schema.column_index(right_column).is_some()) {
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
    fn matches_where_simple(where_clause: &[Condition], values: &[Value], schema: &TableSchema) -> bool {
        for cond in where_clause {
            let col_idx = match schema.column_index(&cond.column.column) {
                Some(idx) => idx,
                None => return false,
            };
            // Coerce the condition value to match the column type
            let coerced = coerce_value(cond.value.clone(), &schema.columns[col_idx].ty);
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
    TypeMismatch { column: String, expected: String, got: String },
    /// Missing required column in INSERT.
    MissingColumn(String),
    /// Storage error.
    Storage(String),
    /// Invalid reference: target row doesn't exist.
    InvalidReference { column: String, target_table: String, target_id: ObjectId },
    /// Column is not a reference type.
    NotAReference(String),
    /// Policy error.
    Policy(PolicyError),
    /// Policy denied the operation.
    PolicyDenied { action: PolicyAction, reason: String },
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
                write!(f, "column count mismatch: expected {}, got {}", expected, got)
            }
            DatabaseError::TypeMismatch { column, expected, got } => {
                write!(f, "type mismatch for '{}': expected {}, got {}", column, expected, got)
            }
            DatabaseError::MissingColumn(name) => write!(f, "missing required column: {}", name),
            DatabaseError::Storage(e) => write!(f, "storage error: {}", e),
            DatabaseError::InvalidReference { column, target_table, target_id } => {
                write!(f, "invalid reference in '{}': row {} not found in table '{}'",
                       column, target_id, target_table)
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
        let index_id = index_objects.get(key)
            .ok_or_else(|| DatabaseError::ColumnNotFound(key.source_column.clone()))?;

        let data = self.state.node.read_sync(*index_id, "main")
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
        let index_id = index_objects.get(key)
            .ok_or_else(|| DatabaseError::ColumnNotFound(key.source_column.clone()))?;

        self.state.node
            .write_sync(*index_id, "main", &index.to_bytes(), "system", timestamp_now())
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
        let rows_id = table_rows_objects.get(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

        let data = self.state.node.read_sync(*rows_id, "main")
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
        let rows_id = table_rows_objects.get(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

        self.state.node
            .write_sync(*rows_id, "main", &rows.to_bytes(), "system", timestamp_now())
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

        Ok(())
    }

    /// Get the object ID for a table's row set.
    pub fn table_rows_object_id(&self, table: &str) -> Option<ObjectId> {
        self.state.table_rows_objects.read().unwrap().get(table).copied()
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
        let schema_id = self.state.node.create_object(&format!("schema:{}", schema.name));

        // Serialize and store schema
        let schema_bytes = schema.to_bytes();
        self.state.node
            .write_sync(schema_id, "main", &schema_bytes, "system", timestamp_now())
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

        // Create table rows object to track row membership
        let rows_id = self.state.node.create_object(&format!("rows:{}", schema.name));
        let empty_rows = TableRows::new();
        self.state.node
            .write_sync(rows_id, "main", &empty_rows.to_bytes(), "system", timestamp_now())
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;
        self.state.table_rows_objects.write().unwrap().insert(schema.name.clone(), rows_id);

        // Create index objects for Ref columns
        for col in &schema.columns {
            if matches!(col.ty, ColumnType::Ref(_)) {
                let key = IndexKey::new(&schema.name, &col.name);
                let index_id = self.state.node.create_object(&format!("index:{}:{}", schema.name, col.name));

                // Initialize with empty index
                let empty_index = RefIndex::new();
                self.state.node
                    .write_sync(index_id, "main", &empty_index.to_bytes(), "system", timestamp_now())
                    .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

                self.state.index_objects.write().unwrap().insert(key, index_id);
            }
        }

        // Cache schema
        self.state.tables.write().unwrap().insert(schema.name.clone(), schema_id);
        self.state.schemas.write().unwrap().insert(schema_id, schema);

        Ok(schema_id)
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
        // Verify table exists
        {
            let tables = self.state.tables.read().unwrap();
            if !tables.contains_key(&policy.table) {
                return Err(DatabaseError::TableNotFound(policy.table.clone()));
            }
        }

        // Add policy to table's policy collection
        let mut policies = self.state.policies.write().unwrap();
        let table_policies = policies
            .entry(policy.table.clone())
            .or_insert_with(TablePolicies::new);

        table_policies.add(policy)?;
        Ok(())
    }

    /// Get policies for a table.
    pub fn get_policies(&self, table: &str) -> Option<TablePolicies> {
        let policies = self.state.policies.read().unwrap();
        policies.get(table).cloned()
    }

    /// Insert a new row into a table.
    pub fn insert(&self, table: &str, columns: &[&str], values: Vec<Value>) -> Result<ObjectId, DatabaseError> {
        let schema = self.get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

        // Build full row values in schema order
        let mut row_values = vec![Value::Null; schema.columns.len()];

        if columns.len() != values.len() {
            return Err(DatabaseError::ColumnMismatch {
                expected: columns.len(),
                got: values.len(),
            });
        }

        for (col_name, value) in columns.iter().zip(values) {
            let idx = schema.column_index(col_name)
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
        let row_id = self.state.node.create_object(&format!("row:{}:{}", table, generate_object_id()));

        // Store row data
        self.state.node
            .write_sync(row_id, "main", &row_bytes, "system", timestamp_now())
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

        // Track row -> table mapping
        self.state.row_table.write().unwrap().insert(row_id, table.to_string());

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
        self.state.graph_registry.notify_row_change(table, RowDelta::Added(row), &*self.state);

        Ok(row_id)
    }

    /// Get a row by ID.
    pub fn get(&self, table: &str, id: ObjectId) -> Result<Option<Row>, DatabaseError> {
        let schema = self.get_table(table)
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
        let data = match self.state.node.read_sync(id, "main") {
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
        let schema = self.get_table(table)
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
        let data = match self.state.node.read_sync(id, "main") {
            Ok(Some(data)) => data,
            Ok(None) => return Ok(false),
            Err(e) => return Err(DatabaseError::Storage(format!("{:?}", e))),
        };

        // Decode current values
        let old_values = decode_row(&data, &schema)?;
        let mut new_values = old_values.clone();

        // Apply assignments
        for (col_name, value) in assignments {
            let idx = schema.column_index(col_name)
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
        self.state.node
            .write_sync(id, "main", &row_bytes, "system", timestamp_now())
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

    /// Delete a row by ID (tombstone).
    pub fn delete(&self, table: &str, id: ObjectId) -> Result<bool, DatabaseError> {
        let schema = self.get_table(table)
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
        let data = match self.state.node.read_sync(id, "main") {
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

        // Write tombstone marker (empty content)
        self.state.node
            .write_sync(id, "main", &[], "system", timestamp_now())
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

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
        let schema = self.get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

        let mut rows = Vec::new();

        // Find all rows for this table
        let row_table = self.state.row_table.read().unwrap();
        for (&row_id, row_tbl) in row_table.iter() {
            if row_tbl != table {
                continue;
            }

            // Read row data
            let data = match self.state.node.read_sync(row_id, "main") {
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
        let schema = self.get_table(table)
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

        let col_idx = schema.column_index(column)
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

            let data = match self.state.node.read_sync(row_id, "main") {
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

        let schema = self.get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

        // Build the row values first (to validate and check policy)
        let mut row_values = vec![Value::Null; schema.columns.len()];

        if columns.len() != values.len() {
            return Err(DatabaseError::ColumnMismatch {
                expected: columns.len(),
                got: values.len(),
            });
        }

        for (col_name, value) in columns.iter().zip(values.clone()) {
            let idx = schema.column_index(col_name)
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

        let schema = self.get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

        // Get the existing row
        let old_row = match self.get(table, id)? {
            Some(row) => row,
            None => return Ok(false),
        };

        // Build the new row values
        let mut new_values = old_row.values.clone();
        for (col_name, value) in assignments {
            let idx = schema.column_index(col_name)
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
        let schema = self.get_table(source_table)
            .ok_or_else(|| DatabaseError::TableNotFound(source_table.to_string()))?;

        // Verify column is a Ref type
        let col = schema.column(source_column)
            .ok_or_else(|| DatabaseError::ColumnNotFound(source_column.to_string()))?;
        if !matches!(col.ty, ColumnType::Ref(_)) {
            return Err(DatabaseError::NotAReference(source_column.to_string()));
        }

        // Look up in index
        let key = IndexKey::new(source_table, source_column);
        let source_ids: Vec<ObjectId> = if self.state.index_objects.read().unwrap().contains_key(&key) {
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
                    self.select_where(&upd.table, &cond.column.column, &cond.value)?
                } else {
                    // Multiple conditions - start with first, then filter
                    let cond = &upd.where_clause[0];
                    let mut rows = self.select_where(&upd.table, &cond.column.column, &cond.value)?;
                    let schema = self.get_table(&upd.table).unwrap();

                    for cond in &upd.where_clause[1..] {
                        let col_idx = schema.column_index(&cond.column.column)
                            .ok_or_else(|| DatabaseError::ColumnNotFound(cond.column.column.clone()))?;
                        rows.retain(|row| &row.values[col_idx] == &cond.value);
                    }
                    rows
                };

                let count = rows_to_update.len();
                let assignments: Vec<(&str, Value)> = upd.assignments
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.clone()))
                    .collect();

                for row in rows_to_update {
                    self.update(&upd.table, row.id, &assignments)?;
                }

                Ok(ExecuteResult::Updated(count))
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
            _ => return Err(DatabaseError::Parse(parser::ParseError {
                message: "incremental_query only supports SELECT statements".to_string(),
                position: 0,
            })),
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
            _ => return Err(DatabaseError::Parse(parser::ParseError {
                message: "incremental_query_as only supports SELECT statements".to_string(),
                position: 0,
            })),
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
        let schema = self.get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.clone()))?;

        // Get the SELECT policy for this table (if any)
        let policies = self.get_policies(table);
        let select_policy = policies.as_ref().and_then(|p| p.get(PolicyAction::Select));

        // Check if policy contains INHERITS - if so, we need to build a JOIN graph
        if let Some(policy) = select_policy {
            if let Some(ref where_expr) = policy.where_clause {
                if let Some(inherits_info) = self.extract_inherits(where_expr, table, &schema)? {
                    // Build a JOIN graph to handle INHERITS
                    return self.build_inherits_join_graph(select, viewer, &inherits_info);
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

        Ok(builder.output(after_policy, GraphId(0)))
    }

    /// Extract INHERITS information from a policy expression.
    ///
    /// Returns Some(InheritsInfo) if the policy contains a simple INHERITS clause
    /// that can be flattened to a JOIN. Returns None for simple predicates.
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
                        message: format!("INHERITS {} not supported in incremental queries, only INHERITS SELECT", action),
                        position: 0,
                    }));
                }

                let col_name = column.column_name();

                // Find the target table from the Ref column
                let col_def = source_schema.column(col_name)
                    .ok_or_else(|| DatabaseError::ColumnNotFound(col_name.to_string()))?;

                let target_table = match &col_def.ty {
                    ColumnType::Ref(t) => t.clone(),
                    _ => return Err(DatabaseError::NotAReference(col_name.to_string())),
                };

                // Get the target table's SELECT policy
                let target_policies = self.get_policies(&target_table);
                let target_select = target_policies.as_ref().and_then(|p| p.get(PolicyAction::Select));
                let target_predicate = target_select.and_then(|p| p.where_clause.clone());

                Ok(Some(InheritsInfo {
                    ref_column: col_name.to_string(),
                    target_table,
                    target_predicate,
                    additional_predicates: vec![],
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
            // Other expressions don't contain INHERITS at the top level
            _ => Ok(None),
        }
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
        let left_schema = self.get_table(left_table)
            .ok_or_else(|| DatabaseError::TableNotFound(left_table.clone()))?;
        let right_schema = self.get_table(&inherits.target_table)
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
            let predicate = self.policy_expr_to_predicate_qualified(target_expr, viewer, &inherits.target_table)?;
            builder.filter(after_additional, predicate)
        } else {
            // No policy on target table = allow all
            after_additional
        };

        Ok(builder.output(after_policy, GraphId(0)))
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
                let (column, value) = self.resolve_policy_comparison_qualified(left, right, viewer, table_prefix)?;
                Ok(Predicate::eq(column, value))
            }
            PolicyExpr::Ne(left, right) => {
                let (column, value) = self.resolve_policy_comparison_qualified(left, right, viewer, table_prefix)?;
                Ok(Predicate::ne(column, value))
            }
            PolicyExpr::And(exprs) => {
                let predicates: Result<Vec<_>, _> = exprs
                    .iter()
                    .map(|e| self.policy_expr_to_predicate_qualified(e, viewer, table_prefix))
                    .collect();
                Ok(predicates?.into_iter().fold(Predicate::True, |acc, p| acc.and(p)))
            }
            PolicyExpr::Or(exprs) => {
                let predicates: Result<Vec<_>, _> = exprs
                    .iter()
                    .map(|e| self.policy_expr_to_predicate_qualified(e, viewer, table_prefix))
                    .collect();
                Ok(predicates?.into_iter().fold(Predicate::False, |acc, p| acc.or(p)))
            }
            PolicyExpr::Not(inner) => {
                Ok(self.policy_expr_to_predicate_qualified(inner, viewer, table_prefix)?.not())
            }
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
                Ok(predicates.into_iter().fold(Predicate::True, |acc, p| acc.and(p)))
            }
            PolicyExpr::Or(exprs) => {
                let predicates: Result<Vec<_>, _> = exprs
                    .iter()
                    .map(|e| self.policy_expr_to_predicate(e, viewer))
                    .collect();
                let predicates = predicates?;
                Ok(predicates.into_iter().fold(Predicate::False, |acc, p| acc.or(p)))
            }
            PolicyExpr::Not(inner) => {
                let pred = self.policy_expr_to_predicate(inner, viewer)?;
                Ok(pred.not())
            }
            // These require runtime evaluation
            PolicyExpr::Inherits { .. } => {
                Err(DatabaseError::Parse(parser::ParseError {
                    message: "INHERITS cannot be converted to static predicate".to_string(),
                    position: 0,
                }))
            }
            // Comparison operators not yet supported in Predicate
            PolicyExpr::Lt(_, _) | PolicyExpr::Le(_, _) |
            PolicyExpr::Gt(_, _) | PolicyExpr::Ge(_, _) => {
                Err(DatabaseError::Parse(parser::ParseError {
                    message: "Comparison operators not yet supported in incremental queries".to_string(),
                    position: 0,
                }))
            }
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
            (PolicyValue::Column(col), PolicyValue::Literal(val)) => {
                Ok((col.clone(), val.clone()))
            }
            // literal = column
            (PolicyValue::Literal(val), PolicyValue::Column(col)) => {
                Ok((col.clone(), val.clone()))
            }
            // @new.column = @viewer (for INSERT CHECK, but in WHERE context treat as column)
            (PolicyValue::NewColumn(col), PolicyValue::Viewer) |
            (PolicyValue::Viewer, PolicyValue::NewColumn(col)) => {
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

        // Validate table exists and get schema
        let schema = self.get_table(table)
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

        // Create output node
        Ok(builder.output(filtered, GraphId(0))) // ID will be assigned by registry
    }

    /// Build a query graph for a JOIN SELECT.
    fn build_join_graph(
        &self,
        select: &Select,
    ) -> Result<crate::sql::query_graph::QueryGraph, DatabaseError> {
        // Currently only support single JOIN
        if select.from.joins.len() != 1 {
            return Err(DatabaseError::Parse(parser::ParseError {
                message: "incremental_query only supports single JOINs".to_string(),
                position: 0,
            }));
        }

        let join = &select.from.joins[0];
        let left_table = &select.from.table;
        let right_table = &join.table;

        // Get schemas for both tables
        let left_schema = self.get_table(left_table)
            .ok_or_else(|| DatabaseError::TableNotFound(left_table.clone()))?;
        let right_schema = self.get_table(right_table)
            .ok_or_else(|| DatabaseError::TableNotFound(right_table.clone()))?;

        // Determine the join column (the Ref column in the left table pointing to right table)
        // The ON clause is: left.column = right.column
        // We need to find which side has the Ref column pointing to the other table
        let left_column = self.find_join_column(&join.on, left_table, right_table, &left_schema)?;

        // Build the JOIN query graph
        let mut builder = JoinGraphBuilder::new(
            left_table,
            left_schema.clone(),
            right_table,
            right_schema.clone(),
            &left_column,
        );

        // Start with join node
        let join_node = builder.join();

        // Apply WHERE filters (if any)
        let filtered = if select.where_clause.is_empty() {
            join_node
        } else {
            // For JOIN queries, we need to handle qualified column names
            let predicate = self.build_join_predicate(&select.where_clause, &left_schema, &right_schema)?;
            builder.filter(join_node, predicate)
        };

        // Create output node
        Ok(builder.output(filtered, GraphId(0))) // ID will be assigned by registry
    }

    /// Find the Ref column in the left table that joins to the right table.
    fn find_join_column(
        &self,
        on: &parser::JoinCondition,
        left_table: &str,
        right_table: &str,
        left_schema: &TableSchema,
    ) -> Result<String, DatabaseError> {
        // Check if the left side of the ON clause references the left table
        let left_is_from_left = on.left.table.as_ref().map(|t| t == left_table).unwrap_or(true);
        let right_is_from_right = on.right.table.as_ref().map(|t| t == right_table).unwrap_or(true);

        if left_is_from_left && right_is_from_right {
            // ON left_table.col = right_table.id pattern
            // The left column should be a Ref column
            let col_name = &on.left.column;
            if let Some(col) = left_schema.column(col_name) {
                if matches!(&col.ty, ColumnType::Ref(target) if target == right_table) {
                    return Ok(col_name.clone());
                }
            }
        }

        let right_is_from_left = on.right.table.as_ref().map(|t| t == left_table).unwrap_or(false);
        let left_is_from_right = on.left.table.as_ref().map(|t| t == right_table).unwrap_or(false);

        if right_is_from_left && left_is_from_right {
            // ON right_table.id = left_table.col pattern
            let col_name = &on.right.column;
            if let Some(col) = left_schema.column(col_name) {
                if matches!(&col.ty, ColumnType::Ref(target) if target == right_table) {
                    return Ok(col_name.clone());
                }
            }
        }

        // Try to find any Ref column in left_schema that points to right_table
        for col in &left_schema.columns {
            if matches!(&col.ty, ColumnType::Ref(target) if target == right_table) {
                return Ok(col.name.clone());
            }
        }

        Err(DatabaseError::Parse(parser::ParseError {
            message: format!(
                "Could not find Ref column in '{}' pointing to '{}'",
                left_table, right_table
            ),
            position: 0,
        }))
    }

    /// Build a Predicate from SQL WHERE conditions for JOIN queries.
    /// Handles qualified column names (table.column).
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

            // Determine which schema to use for type coercion
            let col_type = if column == "id" {
                ColumnType::Ref("".to_string())
            } else if let Some(table) = &cond.column.table {
                // Qualified column - find in specific schema
                if let Some(idx) = left_schema.column_index(column) {
                    if left_schema.name == *table {
                        left_schema.columns[idx].ty.clone()
                    } else if let Some(idx) = right_schema.column_index(column) {
                        right_schema.columns[idx].ty.clone()
                    } else {
                        return Err(DatabaseError::ColumnNotFound(column.clone()));
                    }
                } else if let Some(idx) = right_schema.column_index(column) {
                    right_schema.columns[idx].ty.clone()
                } else {
                    return Err(DatabaseError::ColumnNotFound(column.clone()));
                }
            } else {
                // Unqualified column - search both schemas
                if let Some(idx) = left_schema.column_index(column) {
                    left_schema.columns[idx].ty.clone()
                } else if let Some(idx) = right_schema.column_index(column) {
                    right_schema.columns[idx].ty.clone()
                } else {
                    return Err(DatabaseError::ColumnNotFound(column.clone()));
                }
            };

            let value = coerce_value(cond.value.clone(), &col_type);
            predicates.push(Predicate::eq(column, value));
        }

        // AND all conditions together and optimize
        let combined = predicates.into_iter().reduce(|a, b| a.and(b)).unwrap_or(Predicate::True);
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

            // Coerce value if needed
            let value = if column == "id" {
                coerce_value(cond.value.clone(), &ColumnType::Ref("".to_string()))
            } else {
                let col_idx = schema.column_index(column).unwrap();
                coerce_value(cond.value.clone(), &schema.columns[col_idx].ty)
            };

            predicates.push(Predicate::eq(column, value));
        }

        // AND all conditions together and optimize
        let combined = predicates.into_iter().reduce(|a, b| a.and(b)).unwrap_or(Predicate::True);
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
