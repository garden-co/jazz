use std::collections::{HashMap, HashSet};

use internment::Intern;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::*;

/// Interned name identifying a table in the schema.
/// Pointer-sized (8 bytes), Copy, fast equality via pointer comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableName(pub Intern<String>);

impl Serialize for TableName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.as_str().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TableName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(TableName::new(s))
    }
}

impl TableName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(Intern::new(name.into()))
    }

    /// Get the underlying string reference.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<T: Into<String>> From<T> for TableName {
    fn from(s: T) -> Self {
        Self(Intern::new(s.into()))
    }
}

impl std::fmt::Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl PartialEq<str> for TableName {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for TableName {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for TableName {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

/// Column data type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    /// 4-byte signed integer (i32), like PostgreSQL INTEGER.
    Integer,
    /// 8-byte signed integer (i64), like PostgreSQL BIGINT.
    BigInt,
    /// 1-byte boolean.
    Boolean,
    /// Variable-length UTF-8 text.
    Text,
    /// 8-byte unsigned timestamp (microseconds since Unix epoch).
    Timestamp,
    /// 16-byte UUID (ObjectId).
    Uuid,
    /// Homogeneous array of values.
    Array(Box<ColumnType>),
    /// Heterogeneous row/tuple of values with a known schema.
    /// Used for nested rows (e.g., array of rows from subquery).
    Row(Box<RowDescriptor>),
}

impl ColumnType {
    /// Returns the fixed byte size for this type, or None for variable-length types.
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            ColumnType::Integer => Some(4),
            ColumnType::BigInt => Some(8),
            ColumnType::Boolean => Some(1),
            ColumnType::Timestamp => Some(8),
            ColumnType::Uuid => Some(16),
            ColumnType::Text => None,
            ColumnType::Array(_) => None, // Arrays are variable-length
            ColumnType::Row(_) => None,   // Rows are variable-length
        }
    }

    /// Returns true if this type is variable-length.
    pub fn is_variable(&self) -> bool {
        self.fixed_size().is_none()
    }

    /// Returns the element type if this is an array, None otherwise.
    pub fn element_type(&self) -> Option<&ColumnType> {
        match self {
            ColumnType::Array(elem) => Some(elem),
            _ => None,
        }
    }

    /// Returns the row descriptor if this is a Row type, None otherwise.
    pub fn row_descriptor(&self) -> Option<&RowDescriptor> {
        match self {
            ColumnType::Row(desc) => Some(desc),
            _ => None,
        }
    }
}

/// Interned column name type.
/// Pointer-sized (8 bytes), Copy, fast equality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnName(pub Intern<String>);

impl Serialize for ColumnName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.as_str().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ColumnName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(ColumnName::new(s))
    }
}

impl ColumnName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(Intern::new(name.into()))
    }

    /// Get the underlying string reference.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<T: Into<String>> From<T> for ColumnName {
    fn from(s: T) -> Self {
        Self(Intern::new(s.into()))
    }
}

impl std::fmt::Display for ColumnName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl PartialEq<str> for ColumnName {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for ColumnName {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for ColumnName {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

/// Descriptor for a single column in a row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDescriptor {
    pub name: ColumnName,
    pub column_type: ColumnType,
    pub nullable: bool,
    /// Optional foreign key reference to another table.
    pub references: Option<TableName>,
}

impl ColumnDescriptor {
    pub fn new(name: impl Into<ColumnName>, column_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            column_type,
            nullable: false,
            references: None,
        }
    }

    /// Get the column name as a string slice.
    pub fn name_str(&self) -> &str {
        self.name.as_str()
    }

    pub fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }

    pub fn references(mut self, table: impl Into<TableName>) -> Self {
        self.references = Some(table.into());
        self
    }
}

/// Descriptor for a row's schema, defining column order and types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowDescriptor {
    pub columns: Vec<ColumnDescriptor>,
}

impl RowDescriptor {
    pub fn new(columns: Vec<ColumnDescriptor>) -> Self {
        Self { columns }
    }

    /// Find column index by name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name.as_str() == name)
    }

    /// Get column descriptor by name.
    pub fn column(&self, name: &str) -> Option<&ColumnDescriptor> {
        self.columns.iter().find(|c| c.name.as_str() == name)
    }

    /// Count of fixed-size columns.
    pub fn fixed_column_count(&self) -> usize {
        self.columns
            .iter()
            .filter(|c| !c.column_type.is_variable())
            .count()
    }

    /// Count of variable-length columns.
    pub fn variable_column_count(&self) -> usize {
        self.columns
            .iter()
            .filter(|c| c.column_type.is_variable())
            .count()
    }

    /// Combine multiple descriptors into one (for join outputs).
    /// Column names from later descriptors are preserved as-is.
    /// Use with table-qualified names to avoid ambiguity.
    pub fn combine(descriptors: &[RowDescriptor]) -> Self {
        let columns: Vec<ColumnDescriptor> =
            descriptors.iter().flat_map(|d| d.columns.clone()).collect();
        Self { columns }
    }

    /// Compute a content hash of this descriptor (column-order-independent).
    pub fn content_hash(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        super::branch::hash_row_descriptor(&mut hasher, self);
        *hasher.finalize().as_bytes()
    }
}

/// Schema for a single table, including row structure and policies.
#[derive(Debug, Clone, PartialEq)]
pub struct TableSchema {
    /// Row structure definition.
    pub descriptor: RowDescriptor,
    /// Access control policies.
    pub policies: TablePolicies,
}

impl TableSchema {
    /// Create a new table schema with no policies (allow all).
    pub fn new(descriptor: RowDescriptor) -> Self {
        Self {
            descriptor,
            policies: TablePolicies::default(),
        }
    }

    /// Create a table schema with policies.
    pub fn with_policies(descriptor: RowDescriptor, policies: TablePolicies) -> Self {
        Self {
            descriptor,
            policies,
        }
    }

    /// Start building a new table schema.
    pub fn builder(name: &str) -> TableSchemaBuilder {
        TableSchemaBuilder::new(name)
    }
}

impl From<RowDescriptor> for TableSchema {
    fn from(descriptor: RowDescriptor) -> Self {
        Self::new(descriptor)
    }
}

/// Builder for creating TableSchema with a fluent API.
#[derive(Debug, Clone)]
pub struct TableSchemaBuilder {
    name: String,
    columns: Vec<ColumnDescriptor>,
    policies: TablePolicies,
}

impl TableSchemaBuilder {
    /// Create a new builder for a table with the given name.
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            columns: Vec::new(),
            policies: TablePolicies::default(),
        }
    }

    /// Add a column to the table.
    pub fn column(mut self, name: &str, column_type: ColumnType) -> Self {
        self.columns.push(ColumnDescriptor::new(name, column_type));
        self
    }

    /// Add a nullable column to the table.
    pub fn nullable_column(mut self, name: &str, column_type: ColumnType) -> Self {
        self.columns
            .push(ColumnDescriptor::new(name, column_type).nullable());
        self
    }

    /// Add a foreign key column.
    pub fn fk_column(mut self, name: &str, references: &str) -> Self {
        self.columns
            .push(ColumnDescriptor::new(name, ColumnType::Uuid).references(references));
        self
    }

    /// Add a nullable foreign key column.
    pub fn nullable_fk_column(mut self, name: &str, references: &str) -> Self {
        self.columns.push(
            ColumnDescriptor::new(name, ColumnType::Uuid)
                .nullable()
                .references(references),
        );
        self
    }

    /// Set policies for the table.
    pub fn policies(mut self, policies: TablePolicies) -> Self {
        self.policies = policies;
        self
    }

    /// Get the table name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Build the TableSchema (returns just the schema, not the name).
    pub fn build(self) -> TableSchema {
        TableSchema {
            descriptor: RowDescriptor::new(self.columns),
            policies: self.policies,
        }
    }

    /// Build and return both name and schema (for inserting into Schema map).
    pub fn build_named(self) -> (TableName, TableSchema) {
        let name = TableName::new(&self.name);
        let schema = TableSchema {
            descriptor: RowDescriptor::new(self.columns),
            policies: self.policies,
        };
        (name, schema)
    }
}

/// Builder for creating a complete Schema with multiple tables.
#[derive(Debug, Clone, Default)]
pub struct SchemaBuilder {
    tables: Vec<TableSchemaBuilder>,
}

impl SchemaBuilder {
    /// Create a new empty schema builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a table builder to the schema.
    pub fn table(mut self, builder: TableSchemaBuilder) -> Self {
        self.tables.push(builder);
        self
    }

    /// Build the complete schema.
    pub fn build(self) -> Schema {
        self.tables.into_iter().map(|t| t.build_named()).collect()
    }

    /// Compute the schema hash.
    pub fn hash(&self) -> SchemaHash {
        let schema = self.clone().build();
        SchemaHash::compute(&schema)
    }
}

/// Schema mapping table names to their table schemas.
pub type Schema = HashMap<TableName, TableSchema>;

/// Validate that no INHERITS cycles exist in the schema.
///
/// Cycles include: A→A (self-reference), A→B→A (direct cycle), A→B→C→A (indirect cycle).
/// Cycles in INHERITS can cause infinite loops during policy evaluation.
///
/// Returns Ok(()) if no cycles found, Err with a description of the cycle otherwise.
pub fn validate_no_inherits_cycles(schema: &Schema) -> Result<(), String> {
    use crate::query_manager::policy::Operation;

    for (table_name, table_schema) in schema {
        // Check all policies that might have INHERITS
        let policies_to_check = [
            (&table_schema.policies.select.using, Operation::Select),
            (&table_schema.policies.update.using, Operation::Update),
        ];

        for (policy_opt, _operation) in policies_to_check.iter() {
            if let Some(policy) = policy_opt {
                let mut visited = HashSet::new();
                visited.insert(*table_name);
                validate_policy_no_cycles(
                    table_name,
                    policy,
                    &table_schema.descriptor,
                    schema,
                    &mut visited,
                )?;
            }
        }

        // Also check DELETE's effective policy (falls back to UPDATE)
        if let Some(policy) = table_schema.policies.effective_delete_using() {
            let mut visited = HashSet::new();
            visited.insert(*table_name);
            validate_policy_no_cycles(
                table_name,
                policy,
                &table_schema.descriptor,
                schema,
                &mut visited,
            )?;
        }
    }
    Ok(())
}

/// Recursively validate that a policy expression has no INHERITS cycles.
pub fn validate_policy_no_cycles(
    current_table: &TableName,
    policy: &PolicyExpr,
    descriptor: &RowDescriptor,
    schema: &Schema,
    visited: &mut HashSet<TableName>,
) -> Result<(), String> {
    use crate::query_manager::policy::PolicyExpr;

    match policy {
        PolicyExpr::Inherits {
            via_column,
            operation,
        } => {
            // Get target table from FK column
            let col_idx = descriptor.column_index(via_column).ok_or_else(|| {
                format!(
                    "INHERITS via_column '{}' not found in table '{}'",
                    via_column, current_table.0
                )
            })?;

            let target_table =
                descriptor.columns[col_idx]
                    .references
                    .as_ref()
                    .ok_or_else(|| {
                        format!(
                            "INHERITS via_column '{}' in table '{}' has no FK reference",
                            via_column, current_table.0
                        )
                    })?;

            // Cycle check
            if visited.contains(target_table) {
                let path: Vec<_> = visited.iter().map(|t| t.0.as_str()).collect();
                return Err(format!(
                    "INHERITS cycle detected: {} → {} (path: {})",
                    current_table.0,
                    target_table.0,
                    path.join(" → ")
                ));
            }

            // Recurse into target table's policy
            if let Some(target_schema) = schema.get(target_table) {
                let target_policy = match operation {
                    crate::query_manager::policy::Operation::Select => {
                        target_schema.policies.select.using.as_ref()
                    }
                    crate::query_manager::policy::Operation::Update => {
                        target_schema.policies.update.using.as_ref()
                    }
                    crate::query_manager::policy::Operation::Delete => {
                        target_schema.policies.effective_delete_using()
                    }
                    crate::query_manager::policy::Operation::Insert => {
                        target_schema.policies.insert.with_check.as_ref()
                    }
                };
                if let Some(p) = target_policy {
                    visited.insert(*target_table);
                    validate_policy_no_cycles(
                        target_table,
                        p,
                        &target_schema.descriptor,
                        schema,
                        visited,
                    )?;
                    visited.remove(target_table);
                }
            }
        }
        PolicyExpr::And(exprs) | PolicyExpr::Or(exprs) => {
            for e in exprs {
                validate_policy_no_cycles(current_table, e, descriptor, schema, visited)?;
            }
        }
        PolicyExpr::Not(inner) => {
            validate_policy_no_cycles(current_table, inner, descriptor, schema, visited)?;
        }
        _ => {} // Simple expressions don't have cycles
    }
    Ok(())
}
