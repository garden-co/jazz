use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ColumnType {
    /// 4-byte signed integer (i32), like PostgreSQL INTEGER.
    Integer,
    /// 8-byte signed integer (i64), like PostgreSQL BIGINT.
    BigInt,
    /// 1-byte boolean.
    Boolean,
    /// Variable-length UTF-8 text.
    Text,
    /// Enumerated text constrained to a closed set of variants.
    Enum { variants: Vec<String> },
    /// 8-byte unsigned timestamp (microseconds since Unix epoch).
    Timestamp,
    /// 8-byte IEEE 754 double-precision float (f64).
    Double,
    /// 16-byte UUID (ObjectId).
    Uuid,
    /// 16-byte batch/version identity.
    BatchId,
    /// Variable-length binary payload.
    Bytea,
    /// JSON payload stored as UTF-8 text, optionally constrained by JSON Schema.
    Json {
        #[serde(skip_serializing_if = "Option::is_none")]
        schema: Option<serde_json::Value>,
    },
    /// Homogeneous array of values.
    Array { element: Box<ColumnType> },
    /// Heterogeneous row/tuple of values with a known schema.
    /// Used for nested rows (e.g., array of rows from subquery).
    Row { columns: Box<RowDescriptor> },
}

impl ColumnType {
    /// Returns the fixed byte size for this type, or None for variable-length types.
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            ColumnType::Integer => Some(4),
            ColumnType::BigInt => Some(8),
            ColumnType::Double => Some(8),
            ColumnType::Boolean => Some(1),
            ColumnType::Timestamp => Some(8),
            ColumnType::Uuid => Some(16),
            ColumnType::BatchId => Some(16),
            ColumnType::Text => None,
            ColumnType::Bytea => None,
            ColumnType::Json { .. } => None,
            ColumnType::Enum { variants } if variants.len() <= u8::MAX as usize + 1 => Some(1),
            ColumnType::Enum { .. } => None,
            ColumnType::Array { .. } => None, // Arrays are variable-length
            ColumnType::Row { .. } => None,   // Rows are variable-length
        }
    }

    /// Returns true if this type is variable-length.
    pub fn is_variable(&self) -> bool {
        self.fixed_size().is_none()
    }

    /// Returns the element type if this is an array, None otherwise.
    pub fn element_type(&self) -> Option<&ColumnType> {
        match self {
            ColumnType::Array { element } => Some(element),
            _ => None,
        }
    }

    /// Returns the row descriptor if this is a Row type, None otherwise.
    pub fn row_descriptor(&self) -> Option<&RowDescriptor> {
        match self {
            ColumnType::Row { columns } => Some(columns),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnMergeStrategy {
    Counter,
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDescriptor {
    pub name: ColumnName,
    pub column_type: ColumnType,
    pub nullable: bool,
    /// Optional foreign key reference to another table.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub references: Option<TableName>,
    /// Optional schema-level default used for omitted values on insert.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
    /// Optional per-column merge strategy. Absence means MRCA-relative LWW.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_strategy: Option<ColumnMergeStrategy>,
}

impl ColumnDescriptor {
    pub fn new(name: impl Into<ColumnName>, column_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            column_type,
            nullable: false,
            references: None,
            default: None,
            merge_strategy: None,
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

    pub fn default(mut self, value: Value) -> Self {
        self.default = Some(value);
        self
    }

    pub fn merge_strategy(mut self, strategy: ColumnMergeStrategy) -> Self {
        self.merge_strategy = Some(strategy);
        self
    }

    pub fn validate_merge_strategy(&self) -> Result<(), String> {
        match self.merge_strategy {
            None => Ok(()),
            Some(ColumnMergeStrategy::Counter) => {
                if self.nullable || self.column_type != ColumnType::Integer {
                    Err(format!(
                        "counter merge strategy is only supported on non-nullable INTEGER columns, got {} ({:?}, nullable={})",
                        self.name_str(),
                        self.column_type,
                        self.nullable
                    ))
                } else {
                    Ok(())
                }
            }
        }
    }
}

/// Descriptor for a row's schema, defining column order and types.
#[derive(Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RowDescriptor {
    pub columns: Vec<ColumnDescriptor>,
    #[serde(skip)]
    content_hash_cache: OnceLock<[u8; 32]>,
}

impl Clone for RowDescriptor {
    fn clone(&self) -> Self {
        Self::new(self.columns.clone())
    }
}

impl PartialEq for RowDescriptor {
    fn eq(&self, other: &Self) -> bool {
        self.columns == other.columns
    }
}

impl Eq for RowDescriptor {}

impl From<Vec<ColumnDescriptor>> for RowDescriptor {
    fn from(columns: Vec<ColumnDescriptor>) -> Self {
        Self::new(columns)
    }
}

impl RowDescriptor {
    pub fn new(columns: Vec<ColumnDescriptor>) -> Self {
        Self {
            columns,
            content_hash_cache: OnceLock::new(),
        }
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
        Self::new(columns)
    }

    /// Compute a content hash of this descriptor, preserving declared column order.
    pub fn content_hash(&self) -> [u8; 32] {
        *self.content_hash_cache.get_or_init(|| {
            let mut hasher = blake3::Hasher::new();
            super::branch::hash_row_descriptor(&mut hasher, self);
            *hasher.finalize().as_bytes()
        })
    }
}

/// Schema for a single table, including row structure and policies.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableSchema {
    /// Row structure definition.
    pub columns: RowDescriptor,
    /// User columns that should have secondary indexes.
    ///
    /// `None` preserves historical behavior and indexes every declared user
    /// column. `Some(columns)` opts into indexing only that explicit subset.
    /// Internal `_id` and `_id_deleted` indexes are always maintained.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub indexed_columns: Option<Vec<ColumnName>>,
    /// Access control policies.
    #[serde(default, skip_serializing_if = "table_policies_are_default")]
    pub policies: TablePolicies,
}

fn table_policies_are_default(policies: &TablePolicies) -> bool {
    *policies == TablePolicies::default()
}

impl TableSchema {
    /// Create a new table schema with no explicit policies.
    ///
    /// Runtime behavior depends on whether a compiled policy bundle is loaded.
    pub fn new(columns: RowDescriptor) -> Self {
        Self {
            columns,
            indexed_columns: None,
            policies: TablePolicies::default(),
        }
    }

    /// Create a table schema with policies.
    pub fn with_policies(columns: RowDescriptor, policies: TablePolicies) -> Self {
        Self {
            columns,
            indexed_columns: None,
            policies,
        }
    }

    /// Start building a new table schema.
    pub fn builder(name: &str) -> TableSchemaBuilder {
        TableSchemaBuilder::new(name)
    }

    /// Return true when the given user column has a maintained secondary index.
    ///
    /// The implicit object-id indexes are always available and are handled here
    /// too so query planning can use one predicate path.
    pub fn is_indexed_column(&self, column: &str) -> bool {
        if column == "_id" || column == "_id_deleted" {
            return true;
        }
        self.indexed_columns
            .as_ref()
            .is_none_or(|columns| columns.iter().any(|name| name.as_str() == column))
    }
}

impl From<RowDescriptor> for TableSchema {
    fn from(columns: RowDescriptor) -> Self {
        Self::new(columns)
    }
}

/// Builder for creating TableSchema with a fluent API.
#[derive(Debug, Clone)]
pub struct TableSchemaBuilder {
    name: String,
    columns: Vec<ColumnDescriptor>,
    indexed_columns: Option<Vec<ColumnName>>,
    policies: TablePolicies,
}

impl TableSchemaBuilder {
    /// Create a new builder for a table with the given name.
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            columns: Vec::new(),
            indexed_columns: None,
            policies: TablePolicies::default(),
        }
    }

    /// Add a column to the table.
    pub fn column(mut self, name: &str, column_type: ColumnType) -> Self {
        self.columns.push(ColumnDescriptor::new(name, column_type));
        self
    }

    /// Add a column with a schema-level default to the table.
    pub fn column_with_default(
        mut self,
        name: &str,
        column_type: ColumnType,
        default: Value,
    ) -> Self {
        self.columns
            .push(ColumnDescriptor::new(name, column_type).default(default));
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

    /// Index only this explicit user-column subset.
    ///
    /// Internal `_id` and `_id_deleted` indexes are always maintained.
    pub fn index_only<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<ColumnName>,
    {
        self.indexed_columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Get the table name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Build the TableSchema (returns just the schema, not the name).
    pub fn build(self) -> TableSchema {
        TableSchema {
            columns: RowDescriptor::new(self.columns),
            indexed_columns: self.indexed_columns,
            policies: self.policies,
        }
    }

    /// Build and return both name and schema (for inserting into Schema map).
    pub fn build_named(self) -> (TableName, TableSchema) {
        let name = TableName::new(&self.name);
        let schema = TableSchema {
            columns: RowDescriptor::new(self.columns),
            indexed_columns: self.indexed_columns,
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
                    &table_schema.columns,
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
                &table_schema.columns,
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
            max_depth,
        } => {
            if let Some(requested_depth) = max_depth
                && crate::query_manager::policy::normalize_recursive_max_depth(Some(
                    *requested_depth,
                ))
                .is_none()
            {
                return Err(format!(
                    "INHERITS max_depth {} exceeds hard cap {} for table '{}'",
                    requested_depth,
                    crate::query_manager::policy::RECURSIVE_POLICY_MAX_DEPTH_HARD_CAP,
                    current_table.0
                ));
            }

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

            let bounded = max_depth.is_some();

            // Cycle check
            if visited.contains(target_table) {
                if bounded {
                    return Ok(());
                }
                let path: Vec<_> = visited.iter().map(|t| t.0.as_str()).collect();
                return Err(format!(
                    "INHERITS cycle detected: {} → {} (path: {})",
                    current_table.0,
                    target_table.0,
                    path.join(" → ")
                ));
            }

            // Bounded INHERITS is cycle-safe by runtime depth limit.
            if bounded {
                return Ok(());
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
                        &target_schema.columns,
                        schema,
                        visited,
                    )?;
                    visited.remove(target_table);
                }
            }
        }
        PolicyExpr::InheritsReferencing {
            source_table,
            via_column,
            operation,
            max_depth,
        } => {
            if let Some(requested_depth) = max_depth
                && crate::query_manager::policy::normalize_recursive_max_depth(Some(
                    *requested_depth,
                ))
                .is_none()
            {
                return Err(format!(
                    "INHERITS REFERENCING max_depth {} exceeds hard cap {} for table '{}'",
                    requested_depth,
                    crate::query_manager::policy::RECURSIVE_POLICY_MAX_DEPTH_HARD_CAP,
                    current_table.0
                ));
            }

            let source_table_name = TableName::new(source_table);
            let source_schema = schema.get(&source_table_name).ok_or_else(|| {
                format!(
                    "INHERITS REFERENCING source table '{}' not found (from table '{}')",
                    source_table, current_table.0
                )
            })?;

            let source_col_idx =
                source_schema
                    .columns
                    .column_index(via_column)
                    .ok_or_else(|| {
                        format!(
                            "INHERITS REFERENCING via_column '{}' not found in source table '{}'",
                            via_column, source_table
                        )
                    })?;

            let referenced_table = source_schema.columns.columns[source_col_idx]
                .references
                .as_ref()
                .ok_or_else(|| {
                    format!(
                        "INHERITS REFERENCING via_column '{}' in source table '{}' has no FK reference",
                        via_column, source_table
                    )
                })?;

            if referenced_table != current_table {
                return Err(format!(
                    "INHERITS REFERENCING {}.{} must reference table '{}', found '{}'",
                    source_table, via_column, current_table.0, referenced_table.0
                ));
            }

            let bounded = max_depth.is_some();
            if visited.contains(&source_table_name) {
                if bounded {
                    return Ok(());
                }
                let path: Vec<_> = visited.iter().map(|t| t.0.as_str()).collect();
                return Err(format!(
                    "INHERITS REFERENCING cycle detected: {} ← {} (path: {})",
                    current_table.0,
                    source_table,
                    path.join(" → ")
                ));
            }

            if bounded {
                return Ok(());
            }

            let source_policy = match operation {
                crate::query_manager::policy::Operation::Select => {
                    source_schema.policies.select.using.as_ref()
                }
                crate::query_manager::policy::Operation::Update => {
                    source_schema.policies.update.using.as_ref()
                }
                crate::query_manager::policy::Operation::Delete => {
                    source_schema.policies.effective_delete_using()
                }
                crate::query_manager::policy::Operation::Insert => {
                    source_schema.policies.insert.with_check.as_ref()
                }
            };
            if let Some(p) = source_policy {
                visited.insert(source_table_name);
                validate_policy_no_cycles(
                    &source_table_name,
                    p,
                    &source_schema.columns,
                    schema,
                    visited,
                )?;
                visited.remove(&source_table_name);
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
