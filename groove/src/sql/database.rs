use std::collections::HashMap;
use std::sync::Arc;

use crate::node::{generate_object_id, LocalNode};
use crate::sql::parser::{self, Statement};
use crate::sql::row::{decode_row, encode_row, Row, RowError, Value};
use crate::sql::schema::{SchemaError, TableSchema};
use crate::storage::Environment;

/// Object ID type alias.
pub type ObjectId = u128;

/// Schema ID type alias (object ID of schema object).
pub type SchemaId = u128;

/// Database providing SQL operations on top of LocalNode.
pub struct Database {
    node: LocalNode,
    /// Map from table name to schema object ID.
    tables: HashMap<String, SchemaId>,
    /// Cached schemas by ID.
    schemas: HashMap<SchemaId, TableSchema>,
    /// Map from row object ID to its table name.
    row_table: HashMap<ObjectId, String>,
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
            node: LocalNode::new(env),
            tables: HashMap::new(),
            schemas: HashMap::new(),
            row_table: HashMap::new(),
        }
    }

    /// Create a new in-memory database (for testing).
    pub fn in_memory() -> Self {
        Database {
            node: LocalNode::in_memory(),
            tables: HashMap::new(),
            schemas: HashMap::new(),
            row_table: HashMap::new(),
        }
    }

    /// Get the underlying LocalNode.
    pub fn node(&self) -> &LocalNode {
        &self.node
    }

    /// Get mutable reference to underlying LocalNode.
    pub fn node_mut(&mut self) -> &mut LocalNode {
        &mut self.node
    }

    /// Create a new table from schema.
    pub fn create_table(&mut self, schema: TableSchema) -> Result<SchemaId, DatabaseError> {
        if self.tables.contains_key(&schema.name) {
            return Err(DatabaseError::TableExists(schema.name.clone()));
        }

        // Create object for schema
        let schema_id = self.node.create_object(&format!("schema:{}", schema.name));

        // Serialize and store schema
        let schema_bytes = schema.to_bytes();
        self.node
            .write_sync(schema_id, "main", &schema_bytes, "system", timestamp_now())
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

        // Cache schema
        self.tables.insert(schema.name.clone(), schema_id);
        self.schemas.insert(schema_id, schema);

        Ok(schema_id)
    }

    /// Get table schema by name.
    pub fn get_table(&self, name: &str) -> Option<&TableSchema> {
        let schema_id = self.tables.get(name)?;
        self.schemas.get(schema_id)
    }

    /// List all table names.
    pub fn list_tables(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }

    /// Insert a new row into a table.
    pub fn insert(&mut self, table: &str, columns: &[&str], values: Vec<Value>) -> Result<ObjectId, DatabaseError> {
        let schema = self.get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?
            .clone();

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

        // Encode row
        let row_bytes = encode_row(&row_values, &schema)?;

        // Create object for row
        let row_id = self.node.create_object(&format!("row:{}:{:032x}", table, generate_object_id()));

        // Store row data
        self.node
            .write_sync(row_id, "main", &row_bytes, "system", timestamp_now())
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

        // Track row -> table mapping
        self.row_table.insert(row_id, table.to_string());

        Ok(row_id)
    }

    /// Get a row by ID.
    pub fn get(&self, table: &str, id: ObjectId) -> Result<Option<Row>, DatabaseError> {
        let schema = self.get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

        // Check if row belongs to this table
        match self.row_table.get(&id) {
            Some(t) if t == table => {}
            Some(_) => return Ok(None), // Row exists but in different table
            None => return Ok(None),    // Row doesn't exist
        }

        // Read row data
        let data = match self.node.read_sync(id, "main") {
            Ok(Some(data)) => data,
            Ok(None) => return Ok(None),
            Err(e) => return Err(DatabaseError::Storage(format!("{:?}", e))),
        };

        // Decode row
        let values = decode_row(&data, schema)?;

        Ok(Some(Row::new(id, values)))
    }

    /// Update a row by ID.
    pub fn update(
        &mut self,
        table: &str,
        id: ObjectId,
        assignments: &[(&str, Value)],
    ) -> Result<bool, DatabaseError> {
        let schema = self.get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?
            .clone();

        // Check row exists and belongs to table
        match self.row_table.get(&id) {
            Some(t) if t == table => {}
            _ => return Ok(false),
        }

        // Read current row data
        let data = match self.node.read_sync(id, "main") {
            Ok(Some(data)) => data,
            Ok(None) => return Ok(false),
            Err(e) => return Err(DatabaseError::Storage(format!("{:?}", e))),
        };

        // Decode current values
        let mut values = decode_row(&data, &schema)?;

        // Apply assignments
        for (col_name, value) in assignments {
            let idx = schema.column_index(col_name)
                .ok_or_else(|| DatabaseError::ColumnNotFound(col_name.to_string()))?;
            values[idx] = value.clone();
        }

        // Re-encode row
        let row_bytes = encode_row(&values, &schema)?;

        // Write updated row
        self.node
            .write_sync(id, "main", &row_bytes, "system", timestamp_now())
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

        Ok(true)
    }

    /// Delete a row by ID (tombstone).
    pub fn delete(&mut self, table: &str, id: ObjectId) -> Result<bool, DatabaseError> {
        // Check row exists and belongs to table
        match self.row_table.get(&id) {
            Some(t) if t == table => {}
            _ => return Ok(false),
        }

        // Write tombstone marker (empty content)
        self.node
            .write_sync(id, "main", &[], "system", timestamp_now())
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

        // Remove from row_table (logically deleted)
        self.row_table.remove(&id);

        Ok(true)
    }

    /// Simple select - returns all rows from a table.
    /// For now, implements only basic scans.
    pub fn select_all(&self, table: &str) -> Result<Vec<Row>, DatabaseError> {
        let schema = self.get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

        let mut rows = Vec::new();

        // Find all rows for this table
        for (&row_id, row_table) in &self.row_table {
            if row_table != table {
                continue;
            }

            // Read row data
            let data = match self.node.read_sync(row_id, "main") {
                Ok(Some(data)) if !data.is_empty() => data,
                _ => continue, // Skip deleted or missing rows
            };

            // Decode row
            match decode_row(&data, schema) {
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
        for (&row_id, row_table) in &self.row_table {
            if row_table != table {
                continue;
            }

            let data = match self.node.read_sync(row_id, "main") {
                Ok(Some(data)) if !data.is_empty() => data,
                _ => continue,
            };

            match decode_row(&data, schema) {
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

    /// Execute a SQL statement.
    pub fn execute(&mut self, sql: &str) -> Result<ExecuteResult, DatabaseError> {
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
        let mut db = Database::in_memory();

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
        let mut db = Database::in_memory();

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
        let mut db = Database::in_memory();

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
        let mut db = Database::in_memory();

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
        let mut db = Database::in_memory();

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
        let mut db = Database::in_memory();

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
        let mut db = Database::in_memory();

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
        let mut db = Database::in_memory();

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
        let mut db = Database::in_memory();

        let result = db.execute("CREATE TABLE users (name STRING NOT NULL, age I64)").unwrap();
        assert!(matches!(result, ExecuteResult::Created(_)));

        assert!(db.get_table("users").is_some());
    }

    #[test]
    fn execute_insert() {
        let mut db = Database::in_memory();

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
        let mut db = Database::in_memory();

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
        let mut db = Database::in_memory();

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
}
