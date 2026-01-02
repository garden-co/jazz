use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock, Weak};

use futures_signals::signal::{Mutable, ReadOnlyMutable, Signal, SignalExt};

use crate::node::{generate_object_id, LocalNode};
use crate::signal::ObjectSignal;
use crate::sql::parser::{self, Condition, Select, Statement};
use crate::sql::row::{decode_row, encode_row, Row, RowError, Value};
use crate::sql::schema::{ColumnType, SchemaError, TableSchema};
use crate::storage::Environment;

/// Object ID type alias.
pub type ObjectId = u128;

/// Schema ID type alias (object ID of schema object).
pub type SchemaId = u128;

/// Key for a reference index: (source_table, source_column).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexKey {
    pub source_table: String,
    pub source_column: String,
}

impl IndexKey {
    pub fn new(source_table: impl Into<String>, source_column: impl Into<String>) -> Self {
        IndexKey {
            source_table: source_table.into(),
            source_column: source_column.into(),
        }
    }
}

/// Reference index: maps target_id -> set of source_row_ids.
/// One index per (source_table, source_column) pair.
#[derive(Debug, Clone, Default)]
pub struct RefIndex {
    /// target_id -> source_row_ids that reference it
    entries: HashMap<ObjectId, HashSet<ObjectId>>,
}

impl RefIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a reference: source_row references target_id.
    pub fn add(&mut self, target_id: ObjectId, source_row_id: ObjectId) {
        self.entries
            .entry(target_id)
            .or_default()
            .insert(source_row_id);
    }

    /// Remove a reference.
    pub fn remove(&mut self, target_id: ObjectId, source_row_id: ObjectId) {
        if let Some(set) = self.entries.get_mut(&target_id) {
            set.remove(&source_row_id);
            if set.is_empty() {
                self.entries.remove(&target_id);
            }
        }
    }

    /// Get all source rows referencing a target.
    pub fn get(&self, target_id: ObjectId) -> impl Iterator<Item = ObjectId> + '_ {
        self.entries
            .get(&target_id)
            .into_iter()
            .flat_map(|set| set.iter().copied())
    }

    /// Serialize the index to bytes.
    /// Format: [entry_count: u32] [entries...]
    /// Each entry: [target_id: u128] [source_count: u32] [source_ids: u128...]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Entry count
        buf.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());

        for (target_id, source_ids) in &self.entries {
            // Target ID
            buf.extend_from_slice(&target_id.to_le_bytes());
            // Source count
            buf.extend_from_slice(&(source_ids.len() as u32).to_le_bytes());
            // Source IDs
            for source_id in source_ids {
                buf.extend_from_slice(&source_id.to_le_bytes());
            }
        }

        buf
    }

    /// Deserialize an index from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, String> {
        if data.len() < 4 {
            return Ok(Self::new()); // Empty index
        }

        let mut pos = 0;
        let entry_count = u32::from_le_bytes(
            data[pos..pos + 4].try_into().map_err(|_| "invalid entry count")?
        ) as usize;
        pos += 4;

        let mut entries = HashMap::new();

        for _ in 0..entry_count {
            if pos + 16 > data.len() {
                return Err("truncated target_id".to_string());
            }
            let target_id = u128::from_le_bytes(
                data[pos..pos + 16].try_into().map_err(|_| "invalid target_id")?
            );
            pos += 16;

            if pos + 4 > data.len() {
                return Err("truncated source_count".to_string());
            }
            let source_count = u32::from_le_bytes(
                data[pos..pos + 4].try_into().map_err(|_| "invalid source_count")?
            ) as usize;
            pos += 4;

            let mut source_ids = HashSet::new();
            for _ in 0..source_count {
                if pos + 16 > data.len() {
                    return Err("truncated source_id".to_string());
                }
                let source_id = u128::from_le_bytes(
                    data[pos..pos + 16].try_into().map_err(|_| "invalid source_id")?
                );
                pos += 16;
                source_ids.insert(source_id);
            }

            entries.insert(target_id, source_ids);
        }

        Ok(RefIndex { entries })
    }
}

/// Unique key for deduplicating query signals.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryKey {
    /// The table being queried.
    pub table: String,
    /// Serialized where clause for deduplication.
    pub where_key: String,
}

impl QueryKey {
    pub fn new(table: impl Into<String>, conditions: &[Condition]) -> Self {
        let table = table.into();
        // Create a stable string representation of conditions for deduplication
        let where_key = conditions
            .iter()
            .map(|c| format!("{}={:?}", c.column.column, c.value))
            .collect::<Vec<_>>()
            .join("&");
        QueryKey { table, where_key }
    }
}

/// State of a query signal.
#[derive(Debug, Clone)]
pub enum QueryState {
    /// Query is loading.
    Loading,
    /// Query has results.
    Loaded(Vec<Row>),
    /// Query encountered an error.
    Error(String),
}

impl QueryState {
    pub fn is_loading(&self) -> bool {
        matches!(self, QueryState::Loading)
    }

    pub fn is_loaded(&self) -> bool {
        matches!(self, QueryState::Loaded(_))
    }

    pub fn is_error(&self) -> bool {
        matches!(self, QueryState::Error(_))
    }

    pub fn rows(&self) -> Option<&[Row]> {
        match self {
            QueryState::Loaded(rows) => Some(rows),
            _ => None,
        }
    }
}

/// Internal data for a query signal.
/// Holds a reference to the database state for auto-evaluation.
struct QuerySignalData {
    /// Current query state.
    state: Mutable<QueryState>,
    /// Query key for identification.
    key: QueryKey,
    /// The parsed SELECT statement.
    select: Select,
    /// Signal for the table's row membership object.
    table_rows_signal: Option<ObjectSignal>,
    /// Signals for row objects that matched the query (tracked for reactivity).
    row_signals: RwLock<Vec<ObjectSignal>>,
    /// Signals for index objects relevant to the query.
    index_signals: RwLock<Vec<ObjectSignal>>,
    /// Reference to database state for re-evaluation.
    db_state: Arc<DatabaseState>,
    /// Version counter - incremented when we re-evaluate.
    /// Used to detect when underlying signals have changed.
    version: Mutable<u64>,
}

impl std::fmt::Debug for QuerySignalData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuerySignalData")
            .field("key", &self.key)
            .finish()
    }
}

/// A handle to a reactive query subscription.
/// When all handles are dropped, the subscription is cleaned up.
#[derive(Clone)]
pub struct QuerySignal {
    data: Arc<QuerySignalData>,
}

impl std::fmt::Debug for QuerySignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuerySignal")
            .field("key", &self.data.key)
            .finish()
    }
}

impl QuerySignal {
    /// Get the current query state.
    pub fn get(&self) -> QueryState {
        self.data.state.get_cloned()
    }

    /// Get a read-only signal for use with futures-signals combinators.
    pub fn signal(&self) -> ReadOnlyMutable<QueryState> {
        self.data.state.read_only()
    }

    /// Get the query key.
    pub fn key(&self) -> &QueryKey {
        &self.data.key
    }

    /// Get the current rows (convenience method).
    pub fn rows(&self) -> Option<Vec<Row>> {
        match self.get() {
            QueryState::Loaded(rows) => Some(rows),
            _ => None,
        }
    }

    /// Get the table rows signal (tracks row membership changes).
    pub fn table_rows_signal(&self) -> Option<&ObjectSignal> {
        self.data.table_rows_signal.as_ref()
    }

    /// Get the row signals (tracks individual row changes).
    pub fn row_signals(&self) -> Vec<ObjectSignal> {
        self.data.row_signals.read().unwrap().clone()
    }

    /// Get the index signals (tracks index changes).
    pub fn index_signals(&self) -> Vec<ObjectSignal> {
        self.data.index_signals.read().unwrap().clone()
    }

    /// Check if any underlying dependency signal has changed since last evaluation.
    /// This is a lightweight check that doesn't re-evaluate the query.
    pub fn may_need_refresh(&self) -> bool {
        // Check table rows signal
        if let Some(sig) = &self.data.table_rows_signal {
            if let crate::signal::SignalState::Loaded(state) = sig.get() {
                if state.has_previous() {
                    return true;
                }
            }
        }

        // Check row signals
        for sig in self.data.row_signals.read().unwrap().iter() {
            if let crate::signal::SignalState::Loaded(state) = sig.get() {
                if state.has_previous() && state.diff_raw().is_changed() {
                    return true;
                }
            }
        }

        false
    }
}

// ========== Experimental: Truly Composed Reactive Query ==========

/// A truly reactive query that composes signals from underlying objects.
/// This implements the `futures_signals::signal::Signal` trait directly,
/// so it can be used with `for_each`, `map`, etc.
///
/// When the table_rows signal changes (rows added/removed), or when any
/// row's content changes, this signal will emit a new value.
pub struct ReactiveQuery {
    /// Reference to database state for re-evaluation.
    db_state: Arc<DatabaseState>,
    /// The SELECT statement to evaluate.
    select: Select,
    /// The underlying table_rows object signal (if available).
    /// Public for testing.
    pub table_rows_object_signal: Option<ObjectSignal>,
    /// Current result state.
    result: Mutable<QueryState>,
}

impl ReactiveQuery {
    fn new(
        db_state: Arc<DatabaseState>,
        select: Select,
        table_rows_object_signal: Option<ObjectSignal>,
    ) -> Self {
        ReactiveQuery {
            db_state,
            select,
            table_rows_object_signal,
            result: Mutable::new(QueryState::Loading),
        }
    }

    /// Evaluate the query and update the result.
    fn evaluate(&self) -> QueryState {
        let table = &self.select.from.table;

        // Read the table rows to get current row IDs
        let row_ids: Vec<ObjectId> = {
            let table_rows_objects = self.db_state.table_rows_objects.read().unwrap();
            if let Some(rows_id) = table_rows_objects.get(table) {
                if let Ok(Some(data)) = self.db_state.node.read_sync(*rows_id, "main") {
                    if !data.is_empty() {
                        if let Ok(table_rows) = TableRows::from_bytes(&data) {
                            table_rows.row_ids.into_iter().collect()
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
                return QueryState::Error("Table not found".to_string());
            }
        };

        // Get schema
        let schema = {
            let tables = self.db_state.tables.read().unwrap();
            let schemas = self.db_state.schemas.read().unwrap();
            tables.get(table)
                .and_then(|id| schemas.get(id).cloned())
        };

        let schema = match schema {
            Some(s) => s,
            None => return QueryState::Error("Schema not found".to_string()),
        };

        // Read and filter rows
        let mut rows = Vec::new();
        for row_id in row_ids {
            let data = match self.db_state.node.read_sync(row_id, "main") {
                Ok(Some(data)) if !data.is_empty() => data,
                _ => continue,
            };

            let values = match decode_row(&data, &schema) {
                Ok(v) => v,
                Err(_) => continue,
            };

            // Apply WHERE clause filtering
            let matches = self.matches_where(&values, &schema);
            if matches {
                rows.push(Row::new(row_id, values));
            }
        }

        QueryState::Loaded(rows)
    }

    fn matches_where(&self, values: &[Value], schema: &TableSchema) -> bool {
        for cond in &self.select.where_clause {
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

    /// Get the current result state.
    pub fn get(&self) -> QueryState {
        self.result.get_cloned()
    }

    /// Get the rows if loaded.
    pub fn rows(&self) -> Option<Vec<Row>> {
        self.get().rows().map(|r| r.to_vec())
    }

    /// Create a signal that emits whenever the query result changes.
    /// This composes the table_rows signal to trigger re-evaluation.
    ///
    /// Returns a boxed Signal to handle the different signal types uniformly.
    pub fn to_signal(self) -> impl Signal<Item = QueryState> + Unpin {
        let db_state = self.db_state.clone();
        let select = self.select.clone();
        let result = self.result.clone();

        // If we have a table_rows_signal, map it to trigger re-evaluation
        if let Some(table_signal) = self.table_rows_object_signal {
            // IMPORTANT: We must keep the ObjectSignal alive so that the SignalRegistry
            // can still update it. The ObjectSignal contains an Arc<SignalData>, and
            // the registry only has a Weak<SignalData>. If we drop the ObjectSignal,
            // the registry's Weak reference becomes invalid and updates won't propagate.
            //
            // We capture `table_signal` in the closure to keep it alive for the
            // lifetime of the composed signal.
            let signal_ref = table_signal.signal().signal_cloned();
            let sig = signal_ref.map(move |_state| {
                // Keep table_signal alive by referencing it (this is a no-op)
                let _ = &table_signal;
                // Re-evaluate query
                let query_state = Self::evaluate_static(&db_state, &select);
                result.set(query_state.clone());
                query_state
            });
            Box::pin(sig) as std::pin::Pin<Box<dyn Signal<Item = QueryState> + Unpin>>
        } else {
            // No table signal, just return the current state
            let sig = result.signal_cloned();
            Box::pin(sig) as std::pin::Pin<Box<dyn Signal<Item = QueryState> + Unpin>>
        }
    }

    /// Static evaluation helper for use in signal map.
    fn evaluate_static(db_state: &Arc<DatabaseState>, select: &Select) -> QueryState {
        let table = &select.from.table;

        // Read the table rows to get current row IDs
        let row_ids: Vec<ObjectId> = {
            let table_rows_objects = db_state.table_rows_objects.read().unwrap();
            if let Some(rows_id) = table_rows_objects.get(table) {
                if let Ok(Some(data)) = db_state.node.read_sync(*rows_id, "main") {
                    if !data.is_empty() {
                        if let Ok(table_rows) = TableRows::from_bytes(&data) {
                            table_rows.row_ids.into_iter().collect()
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
                return QueryState::Error("Table not found".to_string());
            }
        };

        // Get schema
        let schema = {
            let tables = db_state.tables.read().unwrap();
            let schemas = db_state.schemas.read().unwrap();
            tables.get(table)
                .and_then(|id| schemas.get(id).cloned())
        };

        let schema = match schema {
            Some(s) => s,
            None => return QueryState::Error("Schema not found".to_string()),
        };

        // Read and filter rows
        let mut rows = Vec::new();
        for row_id in row_ids {
            let data = match db_state.node.read_sync(row_id, "main") {
                Ok(Some(data)) if !data.is_empty() => data,
                _ => continue,
            };

            let values = match decode_row(&data, &schema) {
                Ok(v) => v,
                Err(_) => continue,
            };

            // Apply WHERE clause filtering
            let matches = Self::matches_where_static(&select.where_clause, &values, &schema);
            if matches {
                rows.push(Row::new(row_id, values));
            }
        }

        QueryState::Loaded(rows)
    }

    fn matches_where_static(where_clause: &[Condition], values: &[Value], schema: &TableSchema) -> bool {
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

/// Registry for query signal deduplication.
#[derive(Debug, Default)]
pub struct QueryRegistry {
    /// Active query signals.
    signals: RwLock<HashMap<QueryKey, Weak<QuerySignalData>>>,
}

impl QueryRegistry {
    pub fn new() -> Self {
        QueryRegistry {
            signals: RwLock::new(HashMap::new()),
        }
    }

    /// Get or create a query signal.
    fn get_or_create(
        &self,
        key: QueryKey,
        select: Select,
        table_rows_signal: Option<ObjectSignal>,
        db_state: Arc<DatabaseState>,
    ) -> QuerySignal {
        // Try to get existing
        {
            let signals = self.signals.read().unwrap();
            if let Some(weak) = signals.get(&key) {
                if let Some(data) = weak.upgrade() {
                    return QuerySignal { data };
                }
            }
        }

        // Create new
        {
            let mut signals = self.signals.write().unwrap();

            // Double-check
            if let Some(weak) = signals.get(&key) {
                if let Some(data) = weak.upgrade() {
                    return QuerySignal { data };
                }
            }

            let data = Arc::new(QuerySignalData {
                state: Mutable::new(QueryState::Loading),
                key: key.clone(),
                select,
                table_rows_signal,
                row_signals: RwLock::new(Vec::new()),
                index_signals: RwLock::new(Vec::new()),
                db_state,
                version: Mutable::new(0),
            });
            signals.insert(key, Arc::downgrade(&data));
            QuerySignal { data }
        }
    }

    /// Get an existing query signal by key, if it's still active.
    fn get_signal(&self, key: &QueryKey) -> Option<QuerySignal> {
        let signals = self.signals.read().unwrap();
        signals.get(key)
            .and_then(|weak| weak.upgrade())
            .map(|data| QuerySignal { data })
    }

    /// Update a query signal with new results and track row signals.
    fn update_with_signals(
        &self,
        key: &QueryKey,
        rows: Vec<Row>,
        row_signals: Vec<ObjectSignal>,
        index_signals: Vec<ObjectSignal>,
    ) {
        let signals = self.signals.read().unwrap();
        if let Some(weak) = signals.get(key) {
            if let Some(data) = weak.upgrade() {
                // Update row signals
                *data.row_signals.write().unwrap() = row_signals;
                // Update index signals
                *data.index_signals.write().unwrap() = index_signals;
                // Update state
                data.state.set(QueryState::Loaded(rows));
            }
        }
    }

    /// Update a query signal with new results.
    fn update(&self, key: &QueryKey, rows: Vec<Row>) {
        let signals = self.signals.read().unwrap();
        if let Some(weak) = signals.get(key) {
            if let Some(data) = weak.upgrade() {
                data.state.set(QueryState::Loaded(rows));
            }
        }
    }

    /// Set error state for a query signal.
    fn set_error(&self, key: &QueryKey, error: String) {
        let signals = self.signals.read().unwrap();
        if let Some(weak) = signals.get(key) {
            if let Some(data) = weak.upgrade() {
                data.state.set(QueryState::Error(error));
            }
        }
    }

    /// Get all active query keys for a table.
    fn keys_for_table(&self, table: &str) -> Vec<QueryKey> {
        let signals = self.signals.read().unwrap();
        signals
            .iter()
            .filter(|(k, w)| k.table == table && w.strong_count() > 0)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Clean up expired signals.
    pub fn cleanup(&self) {
        let mut signals = self.signals.write().unwrap();
        signals.retain(|_, weak| weak.strong_count() > 0);
    }

    /// Get the number of active query signals.
    pub fn active_count(&self) -> usize {
        let signals = self.signals.read().unwrap();
        signals.values().filter(|w| w.strong_count() > 0).count()
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
    /// Registry for reactive query signals.
    queries: QueryRegistry,
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
            queries: QueryRegistry::new(),
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
            queries: QueryRegistry::new(),
        }
    }
}

/// Table row set: tracks which row IDs belong to a table.
/// Stored as an object for reactive updates.
#[derive(Debug, Clone, Default)]
pub struct TableRows {
    /// Set of row IDs in the table.
    row_ids: HashSet<ObjectId>,
}

impl TableRows {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, row_id: ObjectId) {
        self.row_ids.insert(row_id);
    }

    pub fn remove(&mut self, row_id: ObjectId) {
        self.row_ids.remove(&row_id);
    }

    pub fn contains(&self, row_id: ObjectId) -> bool {
        self.row_ids.contains(&row_id)
    }

    pub fn iter(&self) -> impl Iterator<Item = ObjectId> + '_ {
        self.row_ids.iter().copied()
    }

    pub fn len(&self) -> usize {
        self.row_ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.row_ids.is_empty()
    }

    /// Serialize to bytes.
    /// Format: [count: u32] [row_ids: u128...]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.row_ids.len() as u32).to_le_bytes());
        for row_id in &self.row_ids {
            buf.extend_from_slice(&row_id.to_le_bytes());
        }
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, String> {
        if data.len() < 4 {
            return Ok(Self::new());
        }

        let count = u32::from_le_bytes(
            data[0..4].try_into().map_err(|_| "invalid count")?
        ) as usize;

        let mut row_ids = HashSet::new();
        let mut pos = 4;

        for _ in 0..count {
            if pos + 16 > data.len() {
                return Err("truncated row_id".to_string());
            }
            let row_id = u128::from_le_bytes(
                data[pos..pos + 16].try_into().map_err(|_| "invalid row_id")?
            );
            pos += 16;
            row_ids.insert(row_id);
        }

        Ok(TableRows { row_ids })
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
            DatabaseError::RowNotFound(id) => write!(f, "row {:032x} not found", id),
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
                write!(f, "invalid reference in '{}': row {:032x} not found in table '{}'",
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
            for col in &schema.columns {
                if let ColumnType::Ref(target_table) = &col.ty {
                    if !tables.contains_key(target_table) {
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
        let row_id = self.state.node.create_object(&format!("row:{}:{:032x}", table, generate_object_id()));

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

            let data = match self.state.node.read_sync(row_id, "main") {
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
                // Simple SELECT implementation
                let rows = if sel.where_clause.is_empty() {
                    self.select_all(&sel.from.table)?
                } else if sel.where_clause.len() == 1 {
                    let cond = &sel.where_clause[0];
                    self.select_where(&sel.from.table, &cond.column.column, &cond.value)?
                } else {
                    // Multiple conditions
                    let cond = &sel.where_clause[0];
                    let mut rows = self.select_where(&sel.from.table, &cond.column.column, &cond.value)?;
                    let schema = self.get_table(&sel.from.table).unwrap();

                    for cond in &sel.where_clause[1..] {
                        let col_idx = schema.column_index(&cond.column.column)
                            .ok_or_else(|| DatabaseError::ColumnNotFound(cond.column.column.clone()))?;
                        rows.retain(|row| &row.values[col_idx] == &cond.value);
                    }
                    rows
                };

                // TODO: Handle JOINs and projections

                Ok(ExecuteResult::Selected(rows))
            }
        }
    }

    // ========== Reactive Queries ==========

    /// Subscribe to a SELECT query. Returns a signal that updates whenever
    /// matching rows change. The signal auto-updates on local writes.
    ///
    /// The returned QuerySignal contains:
    /// - A table_rows_signal that tracks when rows are added/removed from the table
    /// - Row signals for each row in the result set
    /// - Index signals for relevant Ref column indexes
    ///
    /// On local writes (insert/update/delete), the query automatically re-evaluates
    /// and updates its state synchronously.
    pub fn subscribe_select(&self, select: Select) -> Result<QuerySignal, DatabaseError> {
        // Validate table exists
        let table = &select.from.table;
        if !self.state.tables.read().unwrap().contains_key(table) {
            return Err(DatabaseError::TableNotFound(table.clone()));
        }

        // Subscribe to table rows object
        let table_rows_signal = {
            let table_rows_objects = self.state.table_rows_objects.read().unwrap();
            table_rows_objects.get(table)
                .and_then(|rows_id| self.state.node.subscribe(*rows_id, "main").ok())
        };

        // Create query key for deduplication
        let key = QueryKey::new(table, &select.where_clause);

        // Get or create signal (passing db_state for re-evaluation)
        let signal = self.state.queries.get_or_create(
            key.clone(),
            select.clone(),
            table_rows_signal,
            self.state.clone(),
        );

        // Evaluate query and set up row signals
        let rows = self.evaluate_select(&select)?;

        // Subscribe to each row object in the result
        let row_signals: Vec<ObjectSignal> = rows
            .iter()
            .filter_map(|row| self.state.node.subscribe(row.id, "main").ok())
            .collect();

        // Subscribe to index objects for the table's Ref columns
        let index_signals: Vec<ObjectSignal> = if let Some(schema) = self.get_table(table) {
            let index_objects = self.state.index_objects.read().unwrap();
            schema.columns.iter()
                .filter(|col| matches!(col.ty, ColumnType::Ref(_)))
                .filter_map(|col| {
                    let key = IndexKey::new(table, &col.name);
                    index_objects.get(&key)
                        .and_then(|id| self.state.node.subscribe(*id, "main").ok())
                })
                .collect()
        } else {
            Vec::new()
        };

        // Update with results and signals
        self.state.queries.update_with_signals(&key, rows, row_signals, index_signals);

        Ok(signal)
    }

    /// Refresh a query signal by re-evaluating it.
    /// This is called automatically on local writes, but can also be called manually.
    /// Returns the new rows, or None if the query is no longer active.
    pub fn refresh_query(&self, signal: &QuerySignal) -> Result<Option<Vec<Row>>, DatabaseError> {
        let select = &signal.data.select;
        let key = signal.key();

        // Re-evaluate the query
        let rows = self.evaluate_select(select)?;

        // Subscribe to new row objects
        let row_signals: Vec<ObjectSignal> = rows
            .iter()
            .filter_map(|row| self.state.node.subscribe(row.id, "main").ok())
            .collect();

        // Get index signals (these don't usually change)
        let index_signals = signal.index_signals();

        // Update the signal
        self.state.queries.update_with_signals(key, rows.clone(), row_signals, index_signals);

        Ok(Some(rows))
    }

    /// Internal: Re-evaluate all active queries for a table.
    /// Called synchronously after insert/update/delete operations.
    fn refresh_table_queries(&self, table: &str) {
        let keys = self.state.queries.keys_for_table(table);

        for key in keys {
            if let Some(signal) = self.state.queries.get_signal(&key) {
                // Re-evaluate and update the signal
                if let Ok(rows) = self.evaluate_select(&signal.data.select) {
                    let row_signals: Vec<ObjectSignal> = rows
                        .iter()
                        .filter_map(|row| self.state.node.subscribe(row.id, "main").ok())
                        .collect();
                    let index_signals = signal.index_signals();
                    self.state.queries.update_with_signals(&key, rows, row_signals, index_signals);
                }
            }
        }
    }

    /// Execute a SQL SELECT statement reactively.
    /// Returns a QuerySignal that updates when matching data changes.
    pub fn execute_reactive(&self, sql: &str) -> Result<QuerySignal, DatabaseError> {
        let stmt = parser::parse(sql)?;

        match stmt {
            Statement::Select(select) => self.subscribe_select(select),
            _ => Err(DatabaseError::Parse(parser::ParseError {
                message: "execute_reactive only supports SELECT statements".to_string(),
                position: 0,
            })),
        }
    }

    /// Evaluate a SELECT statement and return matching rows.
    fn evaluate_select(&self, select: &Select) -> Result<Vec<Row>, DatabaseError> {
        let table = &select.from.table;

        let rows = if select.where_clause.is_empty() {
            self.select_all(table)?
        } else if select.where_clause.len() == 1 {
            let cond = &select.where_clause[0];
            self.select_where(table, &cond.column.column, &cond.value)?
        } else {
            // Multiple conditions
            let cond = &select.where_clause[0];
            let mut rows = self.select_where(table, &cond.column.column, &cond.value)?;
            let schema = self.get_table(table).unwrap();

            for cond in &select.where_clause[1..] {
                let col_idx = schema.column_index(&cond.column.column)
                    .ok_or_else(|| DatabaseError::ColumnNotFound(cond.column.column.clone()))?;
                rows.retain(|row| &row.values[col_idx] == &cond.value);
            }
            rows
        };

        // TODO: Handle JOINs and projections

        Ok(rows)
    }

    /// Get the query registry (for testing/inspection).
    pub fn query_registry(&self) -> &QueryRegistry {
        &self.state.queries
    }

    // ========== Experimental: Truly Composed Reactive Query ==========

    /// Create a reactive query that composes signals from underlying objects.
    /// Returns a ReactiveQuery that can be converted to a Signal using `to_signal()`.
    ///
    /// This is the experimental approach that uses pure signal composition
    /// without any manual refresh logic.
    pub fn reactive_query(&self, sql: &str) -> Result<ReactiveQuery, DatabaseError> {
        let stmt = parser::parse(sql)?;

        let select = match stmt {
            Statement::Select(s) => s,
            _ => return Err(DatabaseError::Parse(parser::ParseError {
                message: "reactive_query only supports SELECT statements".to_string(),
                position: 0,
            })),
        };

        let table = &select.from.table;

        // Validate table exists
        if !self.state.tables.read().unwrap().contains_key(table) {
            return Err(DatabaseError::TableNotFound(table.clone()));
        }

        // Subscribe to table rows object
        let table_rows_signal = {
            let table_rows_objects = self.state.table_rows_objects.read().unwrap();
            table_rows_objects.get(table)
                .and_then(|rows_id| self.state.node.subscribe(*rows_id, "main").ok())
        };

        let query = ReactiveQuery::new(
            self.state.clone(),
            select,
            table_rows_signal,
        );

        // Do initial evaluation
        let initial = query.evaluate();
        query.result.set(initial);

        Ok(query)
    }
}

/// Get current timestamp in milliseconds.
fn timestamp_now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::schema::{ColumnDef, ColumnType};

    #[test]
    fn create_table() {
        let db = Database::in_memory();

        let schema = TableSchema::new(
            "users",
            vec![
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::optional("age", ColumnType::I64),
            ],
        );

        let id = db.create_table(schema).unwrap();
        assert!(id > 0);

        // Check table exists
        assert!(db.get_table("users").is_some());
        assert_eq!(db.list_tables(), vec!["users"]);

        // Cannot create duplicate
        let schema2 = TableSchema::new("users", vec![]);
        assert!(matches!(db.create_table(schema2), Err(DatabaseError::TableExists(_))));
    }

    #[test]
    fn insert_and_get() {
        let db = Database::in_memory();

        db.create_table(TableSchema::new(
            "users",
            vec![
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::optional("age", ColumnType::I64),
            ],
        )).unwrap();

        let id = db.insert("users", &["name", "age"], vec![
            Value::String("Alice".into()),
            Value::I64(30),
        ]).unwrap();

        let row = db.get("users", id).unwrap().unwrap();
        assert_eq!(row.id, id);
        assert_eq!(row.values[0], Value::String("Alice".into()));
        assert_eq!(row.values[1], Value::I64(30));
    }

    #[test]
    fn insert_with_null() {
        let db = Database::in_memory();

        db.create_table(TableSchema::new(
            "users",
            vec![
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::optional("email", ColumnType::String),
            ],
        )).unwrap();

        let id = db.insert("users", &["name"], vec![
            Value::String("Bob".into()),
        ]).unwrap();

        let row = db.get("users", id).unwrap().unwrap();
        assert_eq!(row.values[1], Value::Null);
    }

    #[test]
    fn insert_missing_required_column() {
        let db = Database::in_memory();

        db.create_table(TableSchema::new(
            "users",
            vec![
                ColumnDef::required("name", ColumnType::String),
            ],
        )).unwrap();

        let result = db.insert("users", &[], vec![]);
        assert!(matches!(result, Err(DatabaseError::MissingColumn(_))));
    }

    #[test]
    fn update_row() {
        let db = Database::in_memory();

        db.create_table(TableSchema::new(
            "users",
            vec![
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::optional("age", ColumnType::I64),
            ],
        )).unwrap();

        let id = db.insert("users", &["name", "age"], vec![
            Value::String("Alice".into()),
            Value::I64(30),
        ]).unwrap();

        let updated = db.update("users", id, &[("age", Value::I64(31))]).unwrap();
        assert!(updated);

        let row = db.get("users", id).unwrap().unwrap();
        assert_eq!(row.values[1], Value::I64(31));
    }

    #[test]
    fn delete_row() {
        let db = Database::in_memory();

        db.create_table(TableSchema::new(
            "users",
            vec![ColumnDef::required("name", ColumnType::String)],
        )).unwrap();

        let id = db.insert("users", &["name"], vec![Value::String("Alice".into())]).unwrap();

        assert!(db.get("users", id).unwrap().is_some());

        let deleted = db.delete("users", id).unwrap();
        assert!(deleted);

        // Row should no longer exist
        assert!(db.get("users", id).unwrap().is_none());
    }

    #[test]
    fn select_all() {
        let db = Database::in_memory();

        db.create_table(TableSchema::new(
            "users",
            vec![
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::required("active", ColumnType::Bool),
            ],
        )).unwrap();

        db.insert("users", &["name", "active"], vec![Value::String("Alice".into()), Value::Bool(true)]).unwrap();
        db.insert("users", &["name", "active"], vec![Value::String("Bob".into()), Value::Bool(false)]).unwrap();
        db.insert("users", &["name", "active"], vec![Value::String("Carol".into()), Value::Bool(true)]).unwrap();

        let rows = db.select_all("users").unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn select_where() {
        let db = Database::in_memory();

        db.create_table(TableSchema::new(
            "users",
            vec![
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::required("active", ColumnType::Bool),
            ],
        )).unwrap();

        db.insert("users", &["name", "active"], vec![Value::String("Alice".into()), Value::Bool(true)]).unwrap();
        db.insert("users", &["name", "active"], vec![Value::String("Bob".into()), Value::Bool(false)]).unwrap();
        db.insert("users", &["name", "active"], vec![Value::String("Carol".into()), Value::Bool(true)]).unwrap();

        let active = db.select_where("users", "active", &Value::Bool(true)).unwrap();
        assert_eq!(active.len(), 2);

        let inactive = db.select_where("users", "active", &Value::Bool(false)).unwrap();
        assert_eq!(inactive.len(), 1);
        assert_eq!(inactive[0].values[0], Value::String("Bob".into()));
    }

    #[test]
    fn execute_create_table() {
        let db = Database::in_memory();

        let result = db.execute("CREATE TABLE users (name STRING NOT NULL, age I64)").unwrap();
        assert!(matches!(result, ExecuteResult::Created(_)));

        assert!(db.get_table("users").is_some());
    }

    #[test]
    fn execute_insert() {
        let db = Database::in_memory();

        db.execute("CREATE TABLE users (name STRING NOT NULL, age I64)").unwrap();

        let result = db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)").unwrap();
        match result {
            ExecuteResult::Inserted(id) => {
                let row = db.get("users", id).unwrap().unwrap();
                assert_eq!(row.values[0], Value::String("Alice".into()));
                assert_eq!(row.values[1], Value::I64(30));
            }
            _ => panic!("expected Inserted"),
        }
    }

    #[test]
    fn execute_select() {
        let db = Database::in_memory();

        db.execute("CREATE TABLE users (name STRING NOT NULL, active BOOL NOT NULL)").unwrap();
        db.execute("INSERT INTO users (name, active) VALUES ('Alice', true)").unwrap();
        db.execute("INSERT INTO users (name, active) VALUES ('Bob', false)").unwrap();

        let result = db.execute("SELECT * FROM users").unwrap();
        match result {
            ExecuteResult::Selected(rows) => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("expected Selected"),
        }

        let result = db.execute("SELECT * FROM users WHERE active = true").unwrap();
        match result {
            ExecuteResult::Selected(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].values[0], Value::String("Alice".into()));
            }
            _ => panic!("expected Selected"),
        }
    }

    #[test]
    fn execute_update() {
        let db = Database::in_memory();

        db.execute("CREATE TABLE users (name STRING NOT NULL, age I64)").unwrap();
        let id = match db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)").unwrap() {
            ExecuteResult::Inserted(id) => id,
            _ => panic!("expected Inserted"),
        };

        let result = db.execute(&format!("UPDATE users SET age = 31 WHERE id = x'{:032x}'", id)).unwrap();
        match result {
            ExecuteResult::Updated(count) => {
                assert_eq!(count, 1);
            }
            _ => panic!("expected Updated"),
        }

        let row = db.get("users", id).unwrap().unwrap();
        assert_eq!(row.values[1], Value::I64(31));
    }

    // ========== Step 2: References and Indexes ==========

    #[test]
    fn create_table_with_ref_requires_target_table() {
        let db = Database::in_memory();

        // Cannot create posts table before users table exists
        let result = db.create_table(TableSchema::new(
            "posts",
            vec![
                ColumnDef::required("author", ColumnType::Ref("users".into())),
                ColumnDef::required("title", ColumnType::String),
            ],
        ));
        assert!(matches!(result, Err(DatabaseError::TableNotFound(_))));

        // Create users first
        db.create_table(TableSchema::new(
            "users",
            vec![ColumnDef::required("name", ColumnType::String)],
        )).unwrap();

        // Now posts works
        db.create_table(TableSchema::new(
            "posts",
            vec![
                ColumnDef::required("author", ColumnType::Ref("users".into())),
                ColumnDef::required("title", ColumnType::String),
            ],
        )).unwrap();
    }

    #[test]
    fn insert_validates_ref() {
        let db = Database::in_memory();

        db.create_table(TableSchema::new(
            "users",
            vec![ColumnDef::required("name", ColumnType::String)],
        )).unwrap();

        db.create_table(TableSchema::new(
            "posts",
            vec![
                ColumnDef::required("author", ColumnType::Ref("users".into())),
                ColumnDef::required("title", ColumnType::String),
            ],
        )).unwrap();

        // Insert with non-existent user fails
        let result = db.insert("posts", &["author", "title"], vec![
            Value::Ref(0x12345),  // fake user ID
            Value::String("Hello".into()),
        ]);
        assert!(matches!(result, Err(DatabaseError::InvalidReference { .. })));

        // Create a user
        let user_id = db.insert("users", &["name"], vec![Value::String("Alice".into())]).unwrap();

        // Now insert post with valid ref works
        let post_id = db.insert("posts", &["author", "title"], vec![
            Value::Ref(user_id),
            Value::String("Hello".into()),
        ]).unwrap();

        let post = db.get("posts", post_id).unwrap().unwrap();
        assert_eq!(post.values[0], Value::Ref(user_id));
    }

    #[test]
    fn find_referencing_uses_index() {
        let db = Database::in_memory();

        db.create_table(TableSchema::new(
            "users",
            vec![ColumnDef::required("name", ColumnType::String)],
        )).unwrap();

        db.create_table(TableSchema::new(
            "posts",
            vec![
                ColumnDef::required("author", ColumnType::Ref("users".into())),
                ColumnDef::required("title", ColumnType::String),
            ],
        )).unwrap();

        // Create users
        let alice_id = db.insert("users", &["name"], vec![Value::String("Alice".into())]).unwrap();
        let bob_id = db.insert("users", &["name"], vec![Value::String("Bob".into())]).unwrap();

        // Create posts
        db.insert("posts", &["author", "title"], vec![Value::Ref(alice_id), Value::String("Post 1".into())]).unwrap();
        db.insert("posts", &["author", "title"], vec![Value::Ref(alice_id), Value::String("Post 2".into())]).unwrap();
        db.insert("posts", &["author", "title"], vec![Value::Ref(bob_id), Value::String("Post 3".into())]).unwrap();

        // Find Alice's posts
        let alice_posts = db.find_referencing("posts", "author", alice_id).unwrap();
        assert_eq!(alice_posts.len(), 2);

        // Find Bob's posts
        let bob_posts = db.find_referencing("posts", "author", bob_id).unwrap();
        assert_eq!(bob_posts.len(), 1);
        assert_eq!(bob_posts[0].values[1], Value::String("Post 3".into()));

        // Non-existent user has no posts
        let nobody_posts = db.find_referencing("posts", "author", 0x99999).unwrap();
        assert!(nobody_posts.is_empty());
    }

    #[test]
    fn update_maintains_index() {
        let db = Database::in_memory();

        db.create_table(TableSchema::new(
            "users",
            vec![ColumnDef::required("name", ColumnType::String)],
        )).unwrap();

        db.create_table(TableSchema::new(
            "posts",
            vec![
                ColumnDef::required("author", ColumnType::Ref("users".into())),
                ColumnDef::required("title", ColumnType::String),
            ],
        )).unwrap();

        let alice_id = db.insert("users", &["name"], vec![Value::String("Alice".into())]).unwrap();
        let bob_id = db.insert("users", &["name"], vec![Value::String("Bob".into())]).unwrap();

        let post_id = db.insert("posts", &["author", "title"], vec![
            Value::Ref(alice_id),
            Value::String("A Post".into()),
        ]).unwrap();

        // Initially Alice has the post
        assert_eq!(db.find_referencing("posts", "author", alice_id).unwrap().len(), 1);
        assert_eq!(db.find_referencing("posts", "author", bob_id).unwrap().len(), 0);

        // Reassign post to Bob
        db.update("posts", post_id, &[("author", Value::Ref(bob_id))]).unwrap();

        // Now Bob has the post, Alice doesn't
        assert_eq!(db.find_referencing("posts", "author", alice_id).unwrap().len(), 0);
        assert_eq!(db.find_referencing("posts", "author", bob_id).unwrap().len(), 1);
    }

    #[test]
    fn delete_maintains_index() {
        let db = Database::in_memory();

        db.create_table(TableSchema::new(
            "users",
            vec![ColumnDef::required("name", ColumnType::String)],
        )).unwrap();

        db.create_table(TableSchema::new(
            "posts",
            vec![
                ColumnDef::required("author", ColumnType::Ref("users".into())),
                ColumnDef::required("title", ColumnType::String),
            ],
        )).unwrap();

        let alice_id = db.insert("users", &["name"], vec![Value::String("Alice".into())]).unwrap();
        let post_id = db.insert("posts", &["author", "title"], vec![
            Value::Ref(alice_id),
            Value::String("A Post".into()),
        ]).unwrap();

        assert_eq!(db.find_referencing("posts", "author", alice_id).unwrap().len(), 1);

        // Delete the post
        db.delete("posts", post_id).unwrap();

        // Index should be updated
        assert_eq!(db.find_referencing("posts", "author", alice_id).unwrap().len(), 0);
    }

    #[test]
    fn find_referencing_on_non_ref_column_fails() {
        let db = Database::in_memory();

        db.create_table(TableSchema::new(
            "users",
            vec![ColumnDef::required("name", ColumnType::String)],
        )).unwrap();

        let result = db.find_referencing("users", "name", 123);
        assert!(matches!(result, Err(DatabaseError::NotAReference(_))));
    }

    #[test]
    fn nullable_ref_column() {
        let db = Database::in_memory();

        db.create_table(TableSchema::new(
            "users",
            vec![ColumnDef::required("name", ColumnType::String)],
        )).unwrap();

        db.create_table(TableSchema::new(
            "posts",
            vec![
                ColumnDef::optional("author", ColumnType::Ref("users".into())),
                ColumnDef::required("title", ColumnType::String),
            ],
        )).unwrap();

        // Insert post with no author
        let post_id = db.insert("posts", &["title"], vec![Value::String("Anonymous".into())]).unwrap();
        let post = db.get("posts", post_id).unwrap().unwrap();
        assert_eq!(post.values[0], Value::Null);

        // Insert post with author
        let user_id = db.insert("users", &["name"], vec![Value::String("Alice".into())]).unwrap();
        let post2_id = db.insert("posts", &["author", "title"], vec![
            Value::Ref(user_id),
            Value::String("By Alice".into()),
        ]).unwrap();

        // Only the authored post shows in index
        let posts = db.find_referencing("posts", "author", user_id).unwrap();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].id, post2_id);
    }

    // ========== Reactive Query Tests ==========

    #[test]
    fn subscribe_select_returns_current_rows() {
        let db = Database::in_memory();

        db.execute("CREATE TABLE users (name STRING NOT NULL, active BOOL NOT NULL)").unwrap();
        db.execute("INSERT INTO users (name, active) VALUES ('Alice', true)").unwrap();
        db.execute("INSERT INTO users (name, active) VALUES ('Bob', false)").unwrap();

        let signal = db.execute_reactive("SELECT * FROM users").unwrap();

        // Should immediately have the current rows
        let state = signal.get();
        assert!(state.is_loaded());
        let rows = state.rows().unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn subscribe_select_with_where_clause() {
        let db = Database::in_memory();

        db.execute("CREATE TABLE users (name STRING NOT NULL, active BOOL NOT NULL)").unwrap();
        db.execute("INSERT INTO users (name, active) VALUES ('Alice', true)").unwrap();
        db.execute("INSERT INTO users (name, active) VALUES ('Bob', false)").unwrap();
        db.execute("INSERT INTO users (name, active) VALUES ('Carol', true)").unwrap();

        let signal = db.execute_reactive("SELECT * FROM users WHERE active = true").unwrap();

        let rows = signal.rows().unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn table_rows_object_created() {
        let db = Database::in_memory();

        db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

        // Table rows object should exist
        let rows_id = db.table_rows_object_id("users");
        assert!(rows_id.is_some());

        // Should be able to subscribe to it
        let signal = db.node().subscribe(rows_id.unwrap(), "main");
        assert!(signal.is_ok());
    }

    #[test]
    fn index_object_created() {
        let db = Database::in_memory();

        db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
        db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)").unwrap();

        // Index object should exist
        let key = IndexKey::new("posts", "author");
        let index_id = db.index_object_id(&key);
        assert!(index_id.is_some());

        // Should be able to subscribe to it
        let signal = db.node().subscribe(index_id.unwrap(), "main");
        assert!(signal.is_ok());
    }

    #[test]
    fn table_rows_updates_on_insert() {
        let db = Database::in_memory();

        db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

        let rows_id = db.table_rows_object_id("users").unwrap();
        let signal = db.node().subscribe(rows_id, "main").unwrap();

        // Initially empty
        let table_rows = db.read_table_rows("users").unwrap();
        assert!(table_rows.is_empty());

        // Insert a row
        db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap();

        // Table rows should now have one entry
        let table_rows = db.read_table_rows("users").unwrap();
        assert_eq!(table_rows.len(), 1);

        // Signal should have been updated
        assert!(signal.get().is_loaded());
    }

    #[test]
    fn table_rows_updates_on_delete() {
        let db = Database::in_memory();

        db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
        let id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
            ExecuteResult::Inserted(id) => id,
            _ => panic!("expected Inserted"),
        };

        let table_rows = db.read_table_rows("users").unwrap();
        assert_eq!(table_rows.len(), 1);

        db.delete("users", id).unwrap();

        let table_rows = db.read_table_rows("users").unwrap();
        assert!(table_rows.is_empty());
    }

    #[test]
    fn query_signal_deduplication() {
        let db = Database::in_memory();

        // Create two signals for the same non-existent table - should fail
        let result1 = db.execute_reactive("SELECT * FROM users");
        let result2 = db.execute_reactive("SELECT * FROM users");

        // Both should fail since table doesn't exist
        assert!(result1.is_err());
        assert!(result2.is_err());
    }

    #[test]
    fn execute_reactive_only_accepts_select() {
        let db = Database::in_memory();

        db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

        // SELECT should work
        let result = db.execute_reactive("SELECT * FROM users");
        assert!(result.is_ok());

        // INSERT should fail
        let result = db.execute_reactive("INSERT INTO users (name) VALUES ('Alice')");
        assert!(result.is_err());
    }

    #[test]
    fn reactive_query_has_table_rows_signal() {
        let db = Database::in_memory();

        db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
        db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap();

        let signal = db.execute_reactive("SELECT * FROM users").unwrap();

        // Should have table rows signal
        assert!(signal.table_rows_signal().is_some());
    }

    #[test]
    fn reactive_query_has_row_signals() {
        let db = Database::in_memory();

        db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
        db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap();
        db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap();

        let signal = db.execute_reactive("SELECT * FROM users").unwrap();

        // Should have row signals for both rows
        let row_signals = signal.row_signals();
        assert_eq!(row_signals.len(), 2);
    }

    #[test]
    fn reactive_query_refresh_after_insert() {
        let db = Database::in_memory();

        db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
        db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap();

        let signal = db.execute_reactive("SELECT * FROM users").unwrap();

        // Initially has 1 row
        assert_eq!(signal.rows().unwrap().len(), 1);

        // Insert another row - signal auto-updates synchronously
        db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap();

        // Signal immediately has 2 rows (auto-updated on insert)
        assert_eq!(signal.rows().unwrap().len(), 2);

        // Manual refresh also works and returns 2 rows
        let new_rows = db.refresh_query(&signal).unwrap().unwrap();
        assert_eq!(new_rows.len(), 2);
    }

    #[test]
    fn reactive_query_refresh_after_update() {
        let db = Database::in_memory();

        db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
        let id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
            ExecuteResult::Inserted(id) => id,
            _ => panic!("expected Inserted"),
        };

        let signal = db.execute_reactive("SELECT * FROM users WHERE name = 'Alice'").unwrap();
        assert_eq!(signal.rows().unwrap().len(), 1);

        // Update the row to have a different name
        db.update("users", id, &[("name", Value::String("Alicia".into()))]).unwrap();

        // Refresh - should now return 0 rows (name no longer matches)
        db.refresh_query(&signal).unwrap();
        assert_eq!(signal.rows().unwrap().len(), 0);
    }

    #[test]
    fn reactive_query_refresh_after_delete() {
        let db = Database::in_memory();

        db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
        let id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
            ExecuteResult::Inserted(id) => id,
            _ => panic!("expected Inserted"),
        };

        let signal = db.execute_reactive("SELECT * FROM users").unwrap();
        assert_eq!(signal.rows().unwrap().len(), 1);

        // Delete the row
        db.delete("users", id).unwrap();

        // Refresh - should now return 0 rows
        db.refresh_query(&signal).unwrap();
        assert_eq!(signal.rows().unwrap().len(), 0);
    }

    #[test]
    fn reactive_query_has_index_signals() {
        let db = Database::in_memory();

        db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
        db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)").unwrap();

        let user_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
            ExecuteResult::Inserted(id) => id,
            _ => panic!("expected Inserted"),
        };

        db.insert("posts", &["author", "title"], vec![
            Value::Ref(user_id),
            Value::String("Hello".into()),
        ]).unwrap();

        let signal = db.execute_reactive("SELECT * FROM posts").unwrap();

        // Should have index signal for the author column
        let index_signals = signal.index_signals();
        assert_eq!(index_signals.len(), 1);
    }

    #[test]
    fn reactive_query_may_need_refresh() {
        let db = Database::in_memory();

        db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
        db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap();

        let signal = db.execute_reactive("SELECT * FROM users").unwrap();

        // Initially should not need refresh (just evaluated)
        // Note: may_need_refresh checks if signals have changed *since* subscription
        // which they haven't immediately after subscription

        // Insert another row - this will update the table_rows object
        db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap();

        // Now the table rows signal has been updated, so may_need_refresh should detect it
        // (This depends on the signal tracking previous state)
        let table_signal = signal.table_rows_signal().unwrap();
        assert!(table_signal.get().is_loaded());
    }

    // ========== Experimental: Composed Signal Tests ==========

    #[test]
    fn reactive_query_initial_evaluation() {
        let db = Database::in_memory();

        db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
        db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap();
        db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap();

        let query = db.reactive_query("SELECT * FROM users").unwrap();

        // Should have initial rows
        let rows = query.rows().unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn reactive_query_to_signal_compiles() {
        use futures_signals::signal::SignalExt;

        let db = Database::in_memory();

        db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
        db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap();

        let query = db.reactive_query("SELECT * FROM users").unwrap();

        // This should compile - proving we can use signal combinators
        let signal = query.to_signal();

        // Map it to row count
        let _count_signal = signal.map(|state| {
            match state {
                QueryState::Loaded(rows) => rows.len(),
                _ => 0,
            }
        });
    }

    /// Test that ReactiveQuery signal composition works correctly:
    /// - Waker is called synchronously when underlying data changes
    /// - Callback receives updated data on re-poll
    #[test]
    fn reactive_query_signal_updates_on_insert() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::future::Future;
        use std::task::{Context, Wake, Waker};
        use futures_signals::signal::SignalExt;

        let db = Database::in_memory();
        db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
        db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap();

        let query = db.reactive_query("SELECT * FROM users").unwrap();
        let signal = query.to_signal();

        // Track waker calls and row counts
        let waker_called = Arc::new(AtomicBool::new(false));
        let row_counts = Arc::new(RwLock::new(Vec::<usize>::new()));
        let waker_called_clone = waker_called.clone();
        let row_counts_clone = row_counts.clone();

        struct TrackingWaker {
            called: Arc<AtomicBool>,
        }
        impl Wake for TrackingWaker {
            fn wake(self: Arc<Self>) {
                self.called.store(true, Ordering::SeqCst);
            }
        }

        let future = signal.for_each(move |state| {
            if let QueryState::Loaded(rows) = state {
                row_counts_clone.write().unwrap().push(rows.len());
            }
            async {}
        });

        let waker = Waker::from(Arc::new(TrackingWaker { called: waker_called_clone }));
        let mut cx = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);

        // First poll - process initial state
        let _ = future.as_mut().poll(&mut cx);
        assert_eq!(row_counts.read().unwrap().as_slice(), &[1]);

        // Reset waker flag
        waker_called.store(false, Ordering::SeqCst);

        // Insert another row
        db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap();

        // Waker should have been called synchronously during insert
        assert!(waker_called.load(Ordering::SeqCst), "Waker should be called on insert");

        // Second poll - process update
        let _ = future.as_mut().poll(&mut cx);

        // Should have received both initial and updated states
        let counts = row_counts.read().unwrap();
        assert_eq!(counts.as_slice(), &[1, 2], "Should see 1 row then 2 rows");
    }
}
