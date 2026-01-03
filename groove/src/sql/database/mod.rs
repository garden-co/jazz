use std::collections::HashMap;
use std::sync::{Arc, RwLock, Weak};

use crate::node::{generate_object_id, LocalNode};
use crate::listener::ListenerId;
use crate::sql::index::RefIndex;
use crate::sql::parser::{self, Condition, Join, Projection, Select, Statement};
use crate::sql::row::{decode_row, encode_row, Row, RowError, Value};
use crate::sql::schema::{ColumnType, SchemaError, TableSchema};
use crate::sql::table_rows::TableRows;
use crate::sql::types::{IndexKey, ObjectId, QueryState, SchemaId};
use crate::storage::Environment;

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
            if let Value::Ref(target_id) = &cond.value {
                if let Some(table_name) = table {
                    if let Some(row_id) = self.get_row_id(table_name) {
                        return row_id == *target_id;
                    }
                } else {
                    // Unqualified id - check primary table
                    if let Some(row_id) = self.get_row_id(&self.primary_table) {
                        return row_id == *target_id;
                    }
                }
            }
            return false;
        }

        // Regular column
        if let Some(value) = self.get_column(table, column) {
            value == &cond.value
        } else {
            false
        }
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

/// Type alias for query callbacks.
/// For native builds, requires Send + Sync for thread safety.
/// For WASM, allows non-Send/Sync callbacks (WASM is single-threaded).
#[cfg(not(feature = "wasm"))]
pub type QueryCallback = Box<dyn Fn(Arc<Vec<Row>>) + Send + Sync>;

#[cfg(feature = "wasm")]
pub type QueryCallback = Box<dyn Fn(Arc<Vec<Row>>)>;

// ========== Callback-based Reactive Query ==========

/// Internal shared state for a ReactiveQuery.
struct ReactiveQueryInner {
    /// Reference to database state for re-evaluation.
    db_state: Arc<DatabaseState>,
    /// The parsed SELECT statement.
    select: Select,
    /// Table name (for registry lookup).
    table: String,
    /// Current cached result.
    current: RwLock<Option<Arc<Vec<Row>>>>,
    /// User callbacks for query changes.
    callbacks: RwLock<HashMap<ListenerId, QueryCallback>>,
    /// Next callback ID counter.
    next_callback_id: RwLock<u64>,
}

impl ReactiveQueryInner {
    fn evaluate_and_notify(&self) {
        let rows = Arc::new(self.evaluate());
        *self.current.write().unwrap() = Some(rows.clone());

        // Notify all callbacks synchronously
        let callbacks = self.callbacks.read().unwrap();
        for callback in callbacks.values() {
            callback(rows.clone());
        }
    }

    fn evaluate(&self) -> Vec<Row> {
        self.db_state.execute_select(&self.select)
    }
}

/// A reactive query with synchronous callback support.
/// When underlying data changes, all registered callbacks are called synchronously.
#[derive(Clone)]
pub struct ReactiveQuery {
    inner: Arc<ReactiveQueryInner>,
}

impl std::fmt::Debug for ReactiveQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReactiveQuery")
            .field("table", &self.inner.table)
            .finish()
    }
}

impl ReactiveQuery {
    fn new(db_state: Arc<DatabaseState>, select: Select) -> Self {
        let table = select.from.table.clone();
        let inner = Arc::new(ReactiveQueryInner {
            db_state,
            select,
            table,
            current: RwLock::new(None),
            callbacks: RwLock::new(HashMap::new()),
            next_callback_id: RwLock::new(1),
        });

        // Evaluate immediately
        inner.evaluate_and_notify();

        ReactiveQuery { inner }
    }

    /// Get the table name this query is for.
    pub fn table(&self) -> &str {
        &self.inner.table
    }

    /// Get the current query state.
    pub fn get(&self) -> QueryState {
        match self.inner.current.read().unwrap().as_ref() {
            Some(rows) => QueryState::Loaded(rows.as_ref().clone()),
            None => QueryState::Loading,
        }
    }

    /// Get the rows if loaded.
    pub fn rows(&self) -> Option<Vec<Row>> {
        self.inner.current.read().unwrap().as_ref().map(|r| r.as_ref().clone())
    }

    /// Subscribe to query updates with a callback.
    /// The callback is called immediately with current state, then on every update.
    /// Returns a listener ID that can be used to unsubscribe.
    pub fn subscribe(&self, callback: QueryCallback) -> ListenerId {
        let id = {
            let mut next = self.inner.next_callback_id.write().unwrap();
            let id = ListenerId::new(*next);
            *next += 1;
            id
        };

        // Call immediately with current state
        if let Some(rows) = self.inner.current.read().unwrap().as_ref() {
            callback(rows.clone());
        }

        self.inner.callbacks.write().unwrap().insert(id, callback);
        id
    }

    /// Unsubscribe a callback.
    pub fn unsubscribe(&self, id: ListenerId) -> bool {
        self.inner.callbacks.write().unwrap().remove(&id).is_some()
    }

    /// Execute a one-shot query: get the current rows and immediately unsubscribe.
    /// This is useful for non-reactive queries where you just want the current state.
    pub fn once(self) -> Vec<Row> {
        self.rows().unwrap_or_default()
    }

    /// Get the inner Arc (for registry).
    fn inner_weak(&self) -> Weak<ReactiveQueryInner> {
        Arc::downgrade(&self.inner)
    }
}

/// Registry for ReactiveQuery instances.
/// Uses weak references so queries are automatically cleaned up when dropped.
#[derive(Default)]
struct ReactiveQueryRegistry {
    /// Active queries by table name.
    /// Multiple queries can exist for the same table.
    queries: RwLock<HashMap<String, Vec<Weak<ReactiveQueryInner>>>>,
}

impl ReactiveQueryRegistry {
    fn new() -> Self {
        ReactiveQueryRegistry {
            queries: RwLock::new(HashMap::new()),
        }
    }

    /// Register a query for all tables it depends on.
    fn register(&self, query: &ReactiveQuery, tables: &[String]) {
        let mut queries = self.queries.write().unwrap();
        for table in tables {
            queries
                .entry(table.clone())
                .or_default()
                .push(query.inner_weak());
        }
    }

    /// Refresh all active queries for a table.
    fn refresh_table(&self, table: &str) {
        let queries = self.queries.read().unwrap();
        if let Some(table_queries) = queries.get(table) {
            for weak in table_queries {
                if let Some(inner) = weak.upgrade() {
                    inner.evaluate_and_notify();
                }
            }
        }
    }

    /// Clean up expired queries.
    #[cfg(test)]
    fn cleanup(&self) {
        let mut queries = self.queries.write().unwrap();
        for table_queries in queries.values_mut() {
            table_queries.retain(|w| w.strong_count() > 0);
        }
        queries.retain(|_, v| !v.is_empty());
    }

    /// Count the number of active queries (for testing).
    /// This cleans up expired weak references first, then counts.
    #[cfg(test)]
    fn active_query_count(&self) -> usize {
        self.cleanup();
        let queries = self.queries.read().unwrap();
        queries.values().map(|v| v.len()).sum()
    }

    /// Count the number of active queries for a specific table (for testing).
    #[cfg(test)]
    fn active_query_count_for_table(&self, table: &str) -> usize {
        self.cleanup();
        let queries = self.queries.read().unwrap();
        queries.get(table).map(|v| v.len()).unwrap_or(0)
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
    /// Registry for ReactiveQuery instances.
    reactive_queries: ReactiveQueryRegistry,
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
            reactive_queries: ReactiveQueryRegistry::new(),
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
            reactive_queries: ReactiveQueryRegistry::new(),
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
    fn read_all_rows(&self, table: &str) -> Vec<Row> {
        let schema = match self.get_schema(table) {
            Some(s) => s,
            None => return vec![],
        };

        let row_ids: Vec<ObjectId> = {
            let table_rows_objects = self.table_rows_objects.read().unwrap();
            if let Some(rows_id) = table_rows_objects.get(table) {
                if let Ok(Some(data)) = self.node.read_sync(rows_id.0, "main") {
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
            let data = match self.node.read_sync(row_id.0, "main") {
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
    fn get_row(&self, table: &str, id: ObjectId) -> Option<Row> {
        let schema = self.get_schema(table)?;

        // Check if row belongs to this table
        {
            let row_table = self.row_table.read().unwrap();
            match row_table.get(&id) {
                Some(t) if t == table => {}
                _ => return None,
            }
        }

        let data = match self.node.read_sync(id.0, "main") {
            Ok(Some(data)) if !data.is_empty() => data,
            _ => return None,
        };

        let values = match decode_row(&data, &schema) {
            Ok(v) => v,
            Err(_) => return None,
        };

        Some(Row::new(id, values))
    }

    /// Find rows matching a column = value condition.
    fn select_where(&self, table: &str, column: &str, value: &Value) -> Vec<Row> {
        let schema = match self.get_schema(table) {
            Some(s) => s,
            None => return vec![],
        };

        // Special case: id column
        if column == "id" {
            if let Value::Ref(id) = value {
                return match self.get_row(table, *id) {
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

        self.read_all_rows(table)
            .into_iter()
            .filter(|row| row.values.get(col_idx) == Some(value))
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
            if &values[col_idx] != &cond.value {
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
    /// INSERT - returns new row ID
    Inserted(ObjectId),
    /// UPDATE - returns number of rows affected
    Updated(usize),
    /// DELETE - returns number of rows affected
    Deleted(usize),
    /// SELECT - returns matching rows
    Selected(Vec<Row>),
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

impl From<parser::ParseError> for DatabaseError {
    fn from(e: parser::ParseError) -> Self {
        DatabaseError::Parse(e)
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

    /// Get the count of active reactive queries (for testing).
    /// This triggers cleanup of expired weak references first.
    #[cfg(test)]
    pub fn active_query_count(&self) -> usize {
        self.state.reactive_queries.active_query_count()
    }

    /// Get the count of active reactive queries for a specific table (for testing).
    #[cfg(test)]
    pub fn active_query_count_for_table(&self, table: &str) -> usize {
        self.state.reactive_queries.active_query_count_for_table(table)
    }

    // ========== Index Object Helpers ==========

    /// Read an index from its object.
    fn read_index(&self, key: &IndexKey) -> Result<RefIndex, DatabaseError> {
        let index_objects = self.state.index_objects.read().unwrap();
        let index_id = index_objects.get(key)
            .ok_or_else(|| DatabaseError::ColumnNotFound(key.source_column.clone()))?;

        let data = self.state.node.read_sync(index_id.0, "main")
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
            .write_sync(index_id.0, "main", &index.to_bytes(), "system", timestamp_now())
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

        let data = self.state.node.read_sync(rows_id.0, "main")
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
            .write_sync(rows_id.0, "main", &rows.to_bytes(), "system", timestamp_now())
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
            for col in &schema.columns {
                if let ColumnType::Ref(target_table) = &col.ty {
                    if !tables.contains_key(target_table) {
                        return Err(DatabaseError::TableNotFound(target_table.clone()));
                    }
                }
            }
        }

        // Create object for schema (uses internal mutability)
        let schema_id = ObjectId::new(self.state.node.create_object(&format!("schema:{}", schema.name)));

        // Serialize and store schema
        let schema_bytes = schema.to_bytes();
        self.state.node
            .write_sync(schema_id.0, "main", &schema_bytes, "system", timestamp_now())
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

        // Create table rows object to track row membership
        let rows_id = ObjectId::new(self.state.node.create_object(&format!("rows:{}", schema.name)));
        let empty_rows = TableRows::new();
        self.state.node
            .write_sync(rows_id.0, "main", &empty_rows.to_bytes(), "system", timestamp_now())
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;
        self.state.table_rows_objects.write().unwrap().insert(schema.name.clone(), rows_id);

        // Create index objects for Ref columns
        for col in &schema.columns {
            if matches!(col.ty, ColumnType::Ref(_)) {
                let key = IndexKey::new(&schema.name, &col.name);
                let index_id = ObjectId::new(self.state.node.create_object(&format!("index:{}:{}", schema.name, col.name)));

                // Initialize with empty index
                let empty_index = RefIndex::new();
                self.state.node
                    .write_sync(index_id.0, "main", &empty_index.to_bytes(), "system", timestamp_now())
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
            row_values[idx] = value;
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
        let row_id = ObjectId::new(self.state.node.create_object(&format!("row:{}:{}", table, ObjectId::new(generate_object_id()))));

        // Store row data
        self.state.node
            .write_sync(row_id.0, "main", &row_bytes, "system", timestamp_now())
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

        // Re-evaluate affected queries synchronously
        self.refresh_table_queries(table);

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
        let data = match self.state.node.read_sync(id.0, "main") {
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
        let data = match self.state.node.read_sync(id.0, "main") {
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
            new_values[idx] = value.clone();
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
            .write_sync(id.0, "main", &row_bytes, "system", timestamp_now())
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

        // Re-evaluate affected queries synchronously
        self.refresh_table_queries(table);

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
        let data = match self.state.node.read_sync(id.0, "main") {
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
            .write_sync(id.0, "main", &[], "system", timestamp_now())
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

        // Remove from row_table (logically deleted)
        self.state.row_table.write().unwrap().remove(&id);

        // Remove from table rows object (for reactive queries)
        let mut table_rows = self.read_table_rows(table)?;
        table_rows.remove(id);
        self.write_table_rows(table, &table_rows)?;

        // Re-evaluate affected queries synchronously
        self.refresh_table_queries(table);

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
            let data = match self.state.node.read_sync(row_id.0, "main") {
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
            if let Value::Ref(id) = value {
                return match self.get(table, *id)? {
                    Some(row) => Ok(vec![row]),
                    None => Ok(vec![]),
                };
            }
            // id column but non-Ref value - no matches
            return Ok(vec![]);
        }

        let col_idx = schema.column_index(column)
            .ok_or_else(|| DatabaseError::ColumnNotFound(column.to_string()))?;

        let mut rows = Vec::new();

        // Scan all rows
        let row_table = self.state.row_table.read().unwrap();
        for (&row_id, row_tbl) in row_table.iter() {
            if row_tbl != table {
                continue;
            }

            let data = match self.state.node.read_sync(row_id.0, "main") {
                Ok(Some(data)) if !data.is_empty() => data,
                _ => continue,
            };

            match decode_row(&data, &schema) {
                Ok(values) => {
                    if &values[col_idx] == value {
                        rows.push(Row::new(row_id, values));
                    }
                }
                Err(_) => continue,
            }
        }

        Ok(rows)
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

    // ========== Reactive Queries ==========

    /// Internal: Re-evaluate all active queries for a table.
    /// Called synchronously after insert/update/delete operations.
    fn refresh_table_queries(&self, table: &str) {
        self.state.reactive_queries.refresh_table(table);
    }

    /// Create a reactive query with synchronous callback support.
    /// Returns a ReactiveQuery that can have callbacks registered.
    ///
    /// When underlying data changes, callbacks are called synchronously.
    pub fn reactive_query(&self, sql: &str) -> Result<ReactiveQuery, DatabaseError> {
        let stmt = parser::parse(sql)?;

        let select = match stmt {
            Statement::Select(s) => s,
            _ => return Err(DatabaseError::Parse(parser::ParseError {
                message: "reactive_query only supports SELECT statements".to_string(),
                position: 0,
            })),
        };

        let primary_table = &select.from.table;

        // Validate primary table exists
        if !self.state.tables.read().unwrap().contains_key(primary_table) {
            return Err(DatabaseError::TableNotFound(primary_table.clone()));
        }

        // Collect all tables this query depends on (primary + joined)
        let mut all_tables = vec![primary_table.clone()];
        for join in &select.from.joins {
            // Validate joined table exists
            if !self.state.tables.read().unwrap().contains_key(&join.table) {
                return Err(DatabaseError::TableNotFound(join.table.clone()));
            }
            all_tables.push(join.table.clone());
        }

        // Create the reactive query
        let query = ReactiveQuery::new(self.state.clone(), select);

        // Register the query for all tables it depends on
        self.state.reactive_queries.register(&query, &all_tables);

        Ok(query)
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
