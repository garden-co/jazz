use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use blake3;
use internment::Intern;

use crate::commit::CommitId;
use crate::object::ObjectId;

use super::encoding::{decode_row, encode_row};

// ============================================================================
// Schema Hashing - Content-addressed schema identification
// ============================================================================

/// Content-addressed hash of a schema's structural elements.
/// Uses BLAKE3 over canonicalized (sorted) schema representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SchemaHash(pub [u8; 32]);

impl SchemaHash {
    /// Create a SchemaHash from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Get the raw bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Get an 8-character hex prefix for display/branch naming.
    pub fn short(&self) -> String {
        hex::encode(&self.0[..4])
    }

    /// Compute hash for a complete schema (HashMap<TableName, TableSchema>).
    pub fn compute(schema: &Schema) -> Self {
        let mut hasher = blake3::Hasher::new();

        // Sort tables by name for deterministic ordering
        let mut table_names: Vec<_> = schema.keys().collect();
        table_names.sort_by_key(|t| t.as_str());

        for table_name in table_names {
            let table_schema = &schema[table_name];

            // Hash table name
            hasher.update(table_name.as_str().as_bytes());
            hasher.update(&[0]); // delimiter

            // Hash row descriptor (columns sorted by name)
            hash_row_descriptor(&mut hasher, &table_schema.descriptor);
        }

        Self(*hasher.finalize().as_bytes())
    }
}

impl std::fmt::Display for SchemaHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}

/// Hash a RowDescriptor into a hasher, sorting columns by name for order-independence.
fn hash_row_descriptor(hasher: &mut blake3::Hasher, descriptor: &RowDescriptor) {
    // Sort columns by name
    let mut columns: Vec<_> = descriptor.columns.iter().collect();
    columns.sort_by_key(|c| c.name.as_str());

    for col in columns {
        hash_column_descriptor(hasher, col);
    }
}

/// Hash a single ColumnDescriptor.
fn hash_column_descriptor(hasher: &mut blake3::Hasher, col: &ColumnDescriptor) {
    // Name
    hasher.update(col.name.as_str().as_bytes());
    hasher.update(&[0]);

    // Type
    hash_column_type(hasher, &col.column_type);

    // Nullable flag
    hasher.update(&[col.nullable as u8]);

    // References (FK)
    if let Some(ref table) = col.references {
        hasher.update(&[1]);
        hasher.update(table.as_str().as_bytes());
    } else {
        hasher.update(&[0]);
    }
    hasher.update(&[0]); // delimiter
}

/// Hash a ColumnType recursively (for Array and Row types).
fn hash_column_type(hasher: &mut blake3::Hasher, col_type: &ColumnType) {
    match col_type {
        ColumnType::Integer => {
            hasher.update(&[1]);
        }
        ColumnType::BigInt => {
            hasher.update(&[2]);
        }
        ColumnType::Boolean => {
            hasher.update(&[3]);
        }
        ColumnType::Text => {
            hasher.update(&[4]);
        }
        ColumnType::Timestamp => {
            hasher.update(&[5]);
        }
        ColumnType::Uuid => {
            hasher.update(&[6]);
        }
        ColumnType::Array(elem) => {
            hasher.update(&[7]);
            hash_column_type(hasher, elem);
        }
        ColumnType::Row(desc) => {
            hasher.update(&[8]);
            hash_row_descriptor(hasher, desc);
        }
    }
}

/// Simple hex encoding (avoiding external crate).
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

// ============================================================================
// Composed Branch Name - Environment-qualified branch naming
// ============================================================================

use crate::object::BranchName;

/// A branch name composed of environment, schema hash, and user branch.
/// Format: `{env}-{schemaHash8}-{userBranch}`
/// Example: `dev-a1b2c3d4-main`, `prod-f9e8d7c6-feature-x`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ComposedBranchName {
    pub env: String,
    pub schema_hash: SchemaHash,
    pub user_branch: String,
}

impl ComposedBranchName {
    /// Create a new composed branch name.
    pub fn new(env: &str, schema_hash: SchemaHash, user_branch: &str) -> Self {
        Self {
            env: env.to_string(),
            schema_hash,
            user_branch: user_branch.to_string(),
        }
    }

    /// Create from a schema, computing the hash automatically.
    pub fn from_schema(env: &str, schema: &Schema, user_branch: &str) -> Self {
        Self::new(env, SchemaHash::compute(schema), user_branch)
    }

    /// Convert to a BranchName string: "{env}-{hash8}-{userBranch}"
    pub fn to_branch_name(&self) -> BranchName {
        BranchName::new(format!(
            "{}-{}-{}",
            self.env,
            self.schema_hash.short(),
            self.user_branch
        ))
    }

    /// Parse a BranchName back into its components.
    /// Returns None if the format doesn't match.
    pub fn parse(name: &BranchName) -> Option<Self> {
        let s = name.as_str();
        let parts: Vec<&str> = s.splitn(3, '-').collect();
        if parts.len() != 3 {
            return None;
        }

        let env = parts[0].to_string();
        let hash_str = parts[1];
        let user_branch = parts[2].to_string();

        // Validate hash is 8 hex chars
        if hash_str.len() != 8 || !hash_str.chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }

        // We can't fully reconstruct the hash from just 8 chars,
        // so we store a partial hash. For matching purposes, we use a zeroed hash
        // with the short portion filled in.
        let mut hash_bytes = [0u8; 32];
        if let Ok(bytes) = hex_decode(hash_str) {
            hash_bytes[..4].copy_from_slice(&bytes);
        }

        Some(Self {
            env,
            schema_hash: SchemaHash::from_bytes(hash_bytes),
            user_branch,
        })
    }

    /// Check if this branch matches an environment and user branch,
    /// ignoring the schema hash (for finding related branches).
    pub fn matches_env_and_branch(&self, env: &str, user_branch: &str) -> bool {
        self.env == env && self.user_branch == user_branch
    }
}

impl std::fmt::Display for ComposedBranchName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{}-{}",
            self.env,
            self.schema_hash.short(),
            self.user_branch
        )
    }
}

/// Decode a hex string to bytes.
fn hex_decode(s: &str) -> Result<Vec<u8>, ()> {
    if !s.len().is_multiple_of(2) {
        return Err(());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| ()))
        .collect()
}

/// Interned name identifying a table in the schema.
/// Pointer-sized (8 bytes), Copy, fast equality via pointer comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableName(pub Intern<String>);

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
        hash_row_descriptor(&mut hasher, self);
        *hasher.finalize().as_bytes()
    }
}

use super::policy::PolicyExpr;

/// Policy for a specific operation (SELECT, INSERT, UPDATE, DELETE).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct OperationPolicy {
    /// USING clause - filters rows for SELECT/UPDATE/DELETE.
    /// For SELECT: rows not matching are silently filtered out.
    /// For UPDATE/DELETE: rows not matching cannot be modified.
    pub using: Option<PolicyExpr>,
    /// WITH CHECK clause - validates new row data for INSERT/UPDATE.
    /// For INSERT: new row must satisfy this expression.
    /// For UPDATE: updated row must satisfy this expression.
    pub with_check: Option<PolicyExpr>,
}

impl OperationPolicy {
    /// Create a policy with just a USING clause.
    pub fn using(expr: PolicyExpr) -> Self {
        Self {
            using: Some(expr),
            with_check: None,
        }
    }

    /// Create a policy with just a WITH CHECK clause.
    pub fn with_check(expr: PolicyExpr) -> Self {
        Self {
            using: None,
            with_check: Some(expr),
        }
    }

    /// Create a policy with both USING and WITH CHECK clauses.
    pub fn using_and_check(using: PolicyExpr, check: PolicyExpr) -> Self {
        Self {
            using: Some(using),
            with_check: Some(check),
        }
    }
}

/// Policies for all operations on a table.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TablePolicies {
    pub select: OperationPolicy,
    pub insert: OperationPolicy,
    pub update: OperationPolicy,
    pub delete: OperationPolicy,
}

impl TablePolicies {
    /// Create empty policies (allow all by default).
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the SELECT policy (USING only).
    pub fn with_select(mut self, using: PolicyExpr) -> Self {
        self.select = OperationPolicy::using(using);
        self
    }

    /// Set the INSERT policy (WITH CHECK only).
    pub fn with_insert(mut self, with_check: PolicyExpr) -> Self {
        self.insert = OperationPolicy::with_check(with_check);
        self
    }

    /// Set the UPDATE policy (USING and/or WITH CHECK).
    pub fn with_update(mut self, using: Option<PolicyExpr>, with_check: PolicyExpr) -> Self {
        self.update = OperationPolicy {
            using,
            with_check: Some(with_check),
        };
        self
    }

    /// Set the DELETE policy (USING only).
    /// If not set, defaults to UPDATE's USING policy.
    pub fn with_delete(mut self, using: PolicyExpr) -> Self {
        self.delete = OperationPolicy::using(using);
        self
    }

    /// Get the effective DELETE USING policy.
    /// Falls back to UPDATE's USING if DELETE has none.
    pub fn effective_delete_using(&self) -> Option<&PolicyExpr> {
        self.delete.using.as_ref().or(self.update.using.as_ref())
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
    use super::policy::Operation;

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
fn validate_policy_no_cycles(
    current_table: &TableName,
    policy: &PolicyExpr,
    descriptor: &RowDescriptor,
    schema: &Schema,
    visited: &mut HashSet<TableName>,
) -> Result<(), String> {
    use super::policy::PolicyExpr;

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
                    super::policy::Operation::Select => {
                        target_schema.policies.select.using.as_ref()
                    }
                    super::policy::Operation::Update => {
                        target_schema.policies.update.using.as_ref()
                    }
                    super::policy::Operation::Delete => {
                        target_schema.policies.effective_delete_using()
                    }
                    super::policy::Operation::Insert => {
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

/// Value type for API boundary (insert input, query output).
/// Internally, rows are stored as binary.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Integer(i32),
    BigInt(i64),
    Boolean(bool),
    Text(String),
    Timestamp(u64),
    Uuid(ObjectId),
    /// Homogeneous array of values.
    Array(Vec<Value>),
    /// Heterogeneous row/tuple of values (for nested rows in arrays).
    /// The schema is external (from ColumnType::Row).
    Row(Vec<Value>),
    Null,
}

impl Value {
    /// Returns the column type this value represents, or None for Null/Row.
    /// Row returns None because its schema is external.
    pub fn column_type(&self) -> Option<ColumnType> {
        match self {
            Value::Integer(_) => Some(ColumnType::Integer),
            Value::BigInt(_) => Some(ColumnType::BigInt),
            Value::Boolean(_) => Some(ColumnType::Boolean),
            Value::Text(_) => Some(ColumnType::Text),
            Value::Timestamp(_) => Some(ColumnType::Timestamp),
            Value::Uuid(_) => Some(ColumnType::Uuid),
            Value::Array(elements) => {
                // Infer element type from first element; empty arrays have no inferable type
                elements
                    .iter()
                    .find_map(|v| v.column_type())
                    .map(|elem_type| ColumnType::Array(Box::new(elem_type)))
            }
            // Row type requires external schema, can't be inferred
            Value::Row(_) => None,
            Value::Null => None,
        }
    }

    /// Returns true if this is a Null value.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Returns true if this is an Array value.
    pub fn is_array(&self) -> bool {
        matches!(self, Value::Array(_))
    }

    /// Returns true if this is a Row value.
    pub fn is_row(&self) -> bool {
        matches!(self, Value::Row(_))
    }

    /// Returns the array elements if this is an Array, None otherwise.
    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            Value::Array(elements) => Some(elements),
            _ => None,
        }
    }

    /// Returns the row values if this is a Row, None otherwise.
    pub fn as_row(&self) -> Option<&[Value]> {
        match self {
            Value::Row(values) => Some(values),
            _ => None,
        }
    }
}

/// A row with its object ID, binary data, and commit reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub id: ObjectId,
    /// Binary encoded row data.
    pub data: Vec<u8>,
    pub commit_id: CommitId,
}

impl Row {
    pub fn new(id: ObjectId, data: Vec<u8>, commit_id: CommitId) -> Self {
        Self {
            id,
            data,
            commit_id,
        }
    }
}

/// Delta for row-level changes (after materialization).
/// Contains full row data for processing by filter/sort/output nodes.
#[derive(Debug, Clone, Default)]
pub struct RowDelta {
    pub added: Vec<Row>,
    pub removed: Vec<Row>,
    /// Updated rows as (old, new) pairs.
    pub updated: Vec<(Row, Row)>,
    /// True if some rows are still loading (hold back results until ready).
    pub pending: bool,
}

impl RowDelta {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.updated.is_empty()
    }
}

// ============================================================================
// Unified Tuple Model - For JOIN support and progressive materialization
// ============================================================================

/// A single element in a tuple - either just an ID or a fully loaded row.
/// Used for progressive materialization: start with IDs, load data on demand.
#[derive(Clone, Debug)]
pub enum TupleElement {
    /// Just the ID - row data not yet loaded.
    Id(ObjectId),
    /// Fully materialized row with ID, content, and commit reference.
    Row {
        id: ObjectId,
        content: Vec<u8>,
        commit_id: CommitId,
    },
}

impl TupleElement {
    /// Get the object ID regardless of materialization state.
    pub fn id(&self) -> ObjectId {
        match self {
            TupleElement::Id(id) => *id,
            TupleElement::Row { id, .. } => *id,
        }
    }

    /// Check if this element has been fully materialized (row data loaded).
    pub fn is_materialized(&self) -> bool {
        matches!(self, TupleElement::Row { .. })
    }

    /// Get the row content if materialized.
    pub fn content(&self) -> Option<&[u8]> {
        match self {
            TupleElement::Id(_) => None,
            TupleElement::Row { content, .. } => Some(content),
        }
    }

    /// Get the commit ID if materialized.
    pub fn commit_id(&self) -> Option<CommitId> {
        match self {
            TupleElement::Id(_) => None,
            TupleElement::Row { commit_id, .. } => Some(*commit_id),
        }
    }

    /// Create a TupleElement from a Row.
    pub fn from_row(row: &Row) -> Self {
        TupleElement::Row {
            id: row.id,
            content: row.data.clone(),
            commit_id: row.commit_id,
        }
    }

    /// Convert to a Row if materialized.
    pub fn to_row(&self) -> Option<Row> {
        match self {
            TupleElement::Id(_) => None,
            TupleElement::Row {
                id,
                content,
                commit_id,
            } => Some(Row::new(*id, content.clone(), *commit_id)),
        }
    }
}

/// A tuple of elements with identity based on IDs only.
/// Length corresponds to number of tables in query (1 for single-table, 2 for join, etc.)
#[derive(Clone, Debug)]
pub struct Tuple(pub Vec<TupleElement>);

impl Tuple {
    /// Create a new tuple from elements.
    pub fn new(elements: Vec<TupleElement>) -> Self {
        Self(elements)
    }

    /// Create a single-element tuple from an ObjectId.
    pub fn from_id(id: ObjectId) -> Self {
        Self(vec![TupleElement::Id(id)])
    }

    /// Create a single-element tuple from a Row.
    pub fn from_row(row: &Row) -> Self {
        Self(vec![TupleElement::from_row(row)])
    }

    /// Get all IDs in the tuple.
    pub fn ids(&self) -> Vec<ObjectId> {
        self.0.iter().map(|e| e.id()).collect()
    }

    /// Get the first ID (convenience for single-table queries).
    pub fn first_id(&self) -> Option<ObjectId> {
        self.0.first().map(|e| e.id())
    }

    /// Get the number of elements (tables) in this tuple.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the tuple is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Get an element by index.
    pub fn get(&self, index: usize) -> Option<&TupleElement> {
        self.0.get(index)
    }

    /// Get a mutable element by index.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut TupleElement> {
        self.0.get_mut(index)
    }

    /// Check if all elements are fully materialized.
    pub fn is_fully_materialized(&self) -> bool {
        self.0.iter().all(|e| e.is_materialized())
    }

    /// Get the first element as a Row (for single-table queries).
    pub fn to_single_row(&self) -> Option<Row> {
        self.0.first().and_then(|e| e.to_row())
    }

    /// Flatten a multi-element tuple into a single-element tuple.
    ///
    /// Decodes each element using its descriptor, combines all values, and re-encodes
    /// with a combined descriptor. The result is a single-element tuple that can be
    /// converted to a Row.
    ///
    /// Arguments:
    /// - `descriptors`: One descriptor per element in the tuple
    /// - `combined_descriptor`: The combined descriptor for encoding the merged row
    ///
    /// Returns None if any element is not materialized or if encoding fails.
    pub fn flatten_with_descriptors(
        &self,
        descriptors: &[RowDescriptor],
        combined_descriptor: &RowDescriptor,
    ) -> Option<Tuple> {
        if descriptors.len() != self.0.len() {
            return None;
        }

        // Collect all values from all elements
        let mut all_values = Vec::new();
        let mut first_commit_id = None;

        for (elem, desc) in self.0.iter().zip(descriptors.iter()) {
            let content = elem.content()?;
            let values = decode_row(desc, content).ok()?;
            all_values.extend(values);

            if first_commit_id.is_none() {
                first_commit_id = elem.commit_id();
            }
        }

        // Encode with combined descriptor
        let combined_content = encode_row(combined_descriptor, &all_values).ok()?;

        // Use first element's ID as the "primary" ID for the flattened row
        let first_id = self.first_id()?;
        let commit_id = first_commit_id.unwrap_or(CommitId([0; 32]));

        Some(Tuple::new(vec![TupleElement::Row {
            id: first_id,
            content: combined_content,
            commit_id,
        }]))
    }

    /// Iterate over elements.
    pub fn iter(&self) -> impl Iterator<Item = &TupleElement> {
        self.0.iter()
    }

    /// Iterate mutably over elements.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut TupleElement> {
        self.0.iter_mut()
    }
}

// Hash and Eq based on IDs only (not content).
// This allows tuples with the same IDs but different content to be treated as equal
// for set membership, while updates track content changes separately.
impl Hash for Tuple {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for element in &self.0 {
            element.id().hash(state);
        }
    }
}

impl PartialEq for Tuple {
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }
        self.0
            .iter()
            .zip(other.0.iter())
            .all(|(a, b)| a.id() == b.id())
    }
}

impl Eq for Tuple {}

/// Delta for tuple-level changes with progressive materialization.
/// Replaces both IdDelta (for unmaterialized) and RowDelta (for materialized).
#[derive(Clone, Debug, Default)]
pub struct TupleDelta {
    /// Tuples added to the result set.
    pub added: Vec<Tuple>,
    /// Tuples removed from the result set.
    pub removed: Vec<Tuple>,
    /// Updated tuples as (old, new) pairs - same IDs, different content.
    pub updated: Vec<(Tuple, Tuple)>,
    /// True if any elements are still loading (hold back results until ready).
    pub pending: bool,
}

impl TupleDelta {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.updated.is_empty()
    }

    /// Convert to a RowDelta (for single-table queries where all tuples are length-1).
    /// Returns None if any tuple has multiple elements or is not fully materialized.
    pub fn to_row_delta(&self) -> Option<RowDelta> {
        let added: Option<Vec<Row>> = self
            .added
            .iter()
            .map(|t| {
                if t.len() == 1 {
                    t.to_single_row()
                } else {
                    None
                }
            })
            .collect();
        let removed: Option<Vec<Row>> = self
            .removed
            .iter()
            .map(|t| {
                if t.len() == 1 {
                    t.to_single_row()
                } else {
                    None
                }
            })
            .collect();
        let updated: Option<Vec<(Row, Row)>> = self
            .updated
            .iter()
            .map(|(old, new)| {
                if old.len() == 1 && new.len() == 1 {
                    Some((old.to_single_row()?, new.to_single_row()?))
                } else {
                    None
                }
            })
            .collect();

        Some(RowDelta {
            added: added?,
            removed: removed?,
            updated: updated?,
            pending: self.pending,
        })
    }

    /// Convert to a RowDelta, flattening multi-element tuples using descriptors.
    ///
    /// This handles join results by merging all elements into single rows.
    /// For single-element tuples, this is equivalent to `to_row_delta()`.
    ///
    /// Arguments:
    /// - `descriptors`: One descriptor per element in each tuple
    /// - `combined_descriptor`: The combined descriptor for encoding merged rows
    ///
    /// Returns None if flattening fails for any tuple.
    pub fn flatten_to_row_delta(
        &self,
        descriptors: &[RowDescriptor],
        combined_descriptor: &RowDescriptor,
    ) -> Option<RowDelta> {
        let added: Option<Vec<Row>> = self
            .added
            .iter()
            .map(|t| {
                if t.len() == 1 {
                    t.to_single_row()
                } else {
                    t.flatten_with_descriptors(descriptors, combined_descriptor)?
                        .to_single_row()
                }
            })
            .collect();
        let removed: Option<Vec<Row>> = self
            .removed
            .iter()
            .map(|t| {
                if t.len() == 1 {
                    t.to_single_row()
                } else {
                    t.flatten_with_descriptors(descriptors, combined_descriptor)?
                        .to_single_row()
                }
            })
            .collect();
        let updated: Option<Vec<(Row, Row)>> = self
            .updated
            .iter()
            .map(|(old, new)| {
                let old_row = if old.len() == 1 {
                    old.to_single_row()
                } else {
                    old.flatten_with_descriptors(descriptors, combined_descriptor)?
                        .to_single_row()
                }?;
                let new_row = if new.len() == 1 {
                    new.to_single_row()
                } else {
                    new.flatten_with_descriptors(descriptors, combined_descriptor)?
                        .to_single_row()
                }?;
                Some((old_row, new_row))
            })
            .collect();

        Some(RowDelta {
            added: added?,
            removed: removed?,
            updated: updated?,
            pending: self.pending,
        })
    }

    /// Merge another TupleDelta into this one.
    pub fn merge(&mut self, other: TupleDelta) {
        self.added.extend(other.added);
        self.removed.extend(other.removed);
        self.updated.extend(other.updated);
        self.pending = self.pending || other.pending;
    }
}

// ============================================================================
// MaterializationState - Per-element materialization tracking
// ============================================================================

/// Per-element materialization tracking.
/// materialized[i] == true means element i has row content loaded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializationState {
    materialized: Vec<bool>,
}

impl MaterializationState {
    /// Create state where all elements are ID-only (not materialized).
    pub fn all_ids(element_count: usize) -> Self {
        Self {
            materialized: vec![false; element_count],
        }
    }

    /// Create state where all elements are materialized.
    pub fn all_materialized(element_count: usize) -> Self {
        Self {
            materialized: vec![true; element_count],
        }
    }

    /// Check if a specific element is materialized.
    pub fn is_materialized(&self, element_index: usize) -> bool {
        self.materialized
            .get(element_index)
            .copied()
            .unwrap_or(false)
    }

    /// Check if all specified elements are materialized.
    pub fn are_all_materialized(&self, elements: &HashSet<usize>) -> bool {
        elements.iter().all(|&i| self.is_materialized(i))
    }

    /// Check if all elements are materialized.
    pub fn is_fully_materialized(&self) -> bool {
        self.materialized.iter().all(|&m| m)
    }

    /// Return a new state with the specified element marked as materialized.
    pub fn with_materialized(mut self, element_index: usize) -> Self {
        if element_index < self.materialized.len() {
            self.materialized[element_index] = true;
        }
        self
    }

    /// Return a new state with all specified elements marked as materialized.
    pub fn with_all_materialized(mut self, elements: &HashSet<usize>) -> Self {
        for &i in elements {
            if i < self.materialized.len() {
                self.materialized[i] = true;
            }
        }
        self
    }

    /// Return a new state with ALL elements marked as materialized.
    pub fn materialize_all(mut self) -> Self {
        for m in &mut self.materialized {
            *m = true;
        }
        self
    }

    /// Concatenate two states (for join output).
    pub fn concat(&self, other: &Self) -> Self {
        let mut m = self.materialized.clone();
        m.extend(&other.materialized);
        Self { materialized: m }
    }

    /// Get the number of elements tracked.
    pub fn len(&self) -> usize {
        self.materialized.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.materialized.is_empty()
    }

    /// Get iterator over (element_index, is_materialized) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (usize, bool)> + '_ {
        self.materialized.iter().enumerate().map(|(i, &m)| (i, m))
    }

    /// Get indices of unmaterialized elements.
    pub fn unmaterialized_elements(&self) -> HashSet<usize> {
        self.materialized
            .iter()
            .enumerate()
            .filter_map(|(i, &m)| if !m { Some(i) } else { None })
            .collect()
    }
}

// ============================================================================
// TupleDescriptor - Describes structure of tuples in a node's output
// ============================================================================

/// Describes which element of a tuple contains a given set of columns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ElementDescriptor {
    /// Table name or alias for this element.
    pub table: String,
    /// Row descriptor for this element's columns.
    pub descriptor: RowDescriptor,
    /// Starting global column index for this element.
    pub column_offset: usize,
}

/// Describes the structure of tuples in a node's output.
///
/// Maps global column indices to (element_index, local_column_index) pairs,
/// enabling FilterNode to find data in multi-element tuples (e.g., after joins).
///
/// Also tracks per-element materialization state to enable lazy materialization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TupleDescriptor {
    /// Descriptors for each element in the tuple.
    elements: Vec<ElementDescriptor>,
    /// Total columns across all elements.
    total_columns: usize,
    /// Per-element materialization state.
    materialization: MaterializationState,
}

impl TupleDescriptor {
    /// Create a single-element tuple descriptor (ID-only by default).
    pub fn single(table: &str, descriptor: RowDescriptor) -> Self {
        let total_columns = descriptor.columns.len();
        Self {
            elements: vec![ElementDescriptor {
                table: table.to_string(),
                descriptor,
                column_offset: 0,
            }],
            total_columns,
            materialization: MaterializationState::all_ids(1),
        }
    }

    /// Create a single-element tuple descriptor with explicit materialization state.
    pub fn single_with_materialization(
        table: &str,
        descriptor: RowDescriptor,
        materialized: bool,
    ) -> Self {
        let total_columns = descriptor.columns.len();
        Self {
            elements: vec![ElementDescriptor {
                table: table.to_string(),
                descriptor,
                column_offset: 0,
            }],
            total_columns,
            materialization: if materialized {
                MaterializationState::all_materialized(1)
            } else {
                MaterializationState::all_ids(1)
            },
        }
    }

    /// Create a tuple descriptor from multiple element descriptors (all ID-only).
    pub fn from_elements(elements: Vec<ElementDescriptor>) -> Self {
        let element_count = elements.len();
        let total_columns = elements
            .last()
            .map_or(0, |e| e.column_offset + e.descriptor.columns.len());
        Self {
            elements,
            total_columns,
            materialization: MaterializationState::all_ids(element_count),
        }
    }

    /// Create a tuple descriptor from table names and their descriptors (all ID-only).
    /// Computes column_offset for each element automatically.
    pub fn from_tables(tables: &[(String, RowDescriptor)]) -> Self {
        let mut elements = Vec::with_capacity(tables.len());
        let mut offset = 0;

        for (table, descriptor) in tables {
            elements.push(ElementDescriptor {
                table: table.clone(),
                descriptor: descriptor.clone(),
                column_offset: offset,
            });
            offset += descriptor.columns.len();
        }

        Self {
            total_columns: offset,
            materialization: MaterializationState::all_ids(elements.len()),
            elements,
        }
    }

    /// Concatenate two descriptors (for join output).
    /// Combines elements from both and concatenates materialization states.
    pub fn concat(left: &Self, right: &Self) -> Self {
        let mut elements = left.elements.clone();
        let left_cols = left.total_columns;
        for elem in &right.elements {
            elements.push(ElementDescriptor {
                table: elem.table.clone(),
                descriptor: elem.descriptor.clone(),
                column_offset: elem.column_offset + left_cols,
            });
        }
        Self {
            total_columns: left.total_columns + right.total_columns,
            materialization: left.materialization.concat(&right.materialization),
            elements,
        }
    }

    /// Get the materialization state.
    pub fn materialization(&self) -> &MaterializationState {
        &self.materialization
    }

    /// Return a new descriptor with specified elements marked as materialized.
    pub fn with_materialized(self, elements: &HashSet<usize>) -> Self {
        Self {
            materialization: self.materialization.with_all_materialized(elements),
            ..self
        }
    }

    /// Return a new descriptor with all elements marked as materialized.
    pub fn with_all_materialized(self) -> Self {
        Self {
            materialization: self.materialization.materialize_all(),
            ..self
        }
    }

    /// Validate that all required elements are materialized.
    /// Returns Ok if all are materialized, Err with message otherwise.
    pub fn assert_materialized(&self, elements: &HashSet<usize>) -> Result<(), String> {
        let unmaterialized: Vec<_> = elements
            .iter()
            .filter(|&&i| !self.materialization.is_materialized(i))
            .collect();
        if unmaterialized.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "Elements {:?} are not materialized but required",
                unmaterialized
            ))
        }
    }

    /// Get column index by name, searching all elements.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        let mut offset = 0;
        for elem in &self.elements {
            if let Some(local_idx) = elem.descriptor.column_index(name) {
                return Some(offset + local_idx);
            }
            offset += elem.descriptor.columns.len();
        }
        None
    }

    /// Get column index by qualified name (table.column).
    pub fn qualified_column_index(&self, table: &str, column: &str) -> Option<usize> {
        for elem in &self.elements {
            if elem.table == table
                && let Some(local_idx) = elem.descriptor.column_index(column)
            {
                return Some(elem.column_offset + local_idx);
            }
        }
        None
    }

    /// Map global column index to (element_index, local_column_index).
    ///
    /// Given a global column index from the combined descriptor, returns
    /// which tuple element contains that column and the local index within
    /// that element.
    pub fn resolve_column(&self, global_index: usize) -> Option<(usize, usize)> {
        for (elem_idx, elem) in self.elements.iter().enumerate() {
            let elem_end = elem.column_offset + elem.descriptor.columns.len();
            if global_index >= elem.column_offset && global_index < elem_end {
                let local_idx = global_index - elem.column_offset;
                return Some((elem_idx, local_idx));
            }
        }
        None
    }

    /// Get all element indices needed for a set of global column indices.
    pub fn elements_for_columns(&self, columns: &HashSet<usize>) -> HashSet<usize> {
        columns
            .iter()
            .filter_map(|&col| self.resolve_column(col).map(|(elem_idx, _)| elem_idx))
            .collect()
    }

    /// Get the number of elements in the tuple.
    pub fn element_count(&self) -> usize {
        self.elements.len()
    }

    /// Get an element descriptor by index.
    pub fn element(&self, index: usize) -> Option<&ElementDescriptor> {
        self.elements.get(index)
    }

    /// Get the total number of columns across all elements.
    pub fn total_columns(&self) -> usize {
        self.total_columns
    }

    /// Create a combined RowDescriptor with all columns from all elements.
    pub fn combined_descriptor(&self) -> RowDescriptor {
        let columns: Vec<ColumnDescriptor> = self
            .elements
            .iter()
            .flat_map(|e| e.descriptor.columns.clone())
            .collect();
        RowDescriptor { columns }
    }

    /// Get iterator over elements.
    pub fn iter(&self) -> impl Iterator<Item = &ElementDescriptor> {
        self.elements.iter()
    }
}

/// Combined row descriptor for queries spanning multiple tables (joins).
/// Maps qualified column names (table.column) to table index and column index.
#[derive(Debug, Clone)]
pub struct CombinedRowDescriptor {
    /// Table names/aliases in order.
    pub tables: Vec<String>,
    /// Per-table descriptors.
    pub descriptors: Vec<RowDescriptor>,
    /// Map from (table, column) to (table_index, column_index).
    column_map: HashMap<(String, String), (usize, usize)>,
}

impl CombinedRowDescriptor {
    /// Create a new combined descriptor from table names and their descriptors.
    pub fn new(tables: Vec<String>, descriptors: Vec<RowDescriptor>) -> Self {
        let mut column_map = HashMap::new();

        for (table_idx, (table_name, descriptor)) in
            tables.iter().zip(descriptors.iter()).enumerate()
        {
            for (col_idx, col) in descriptor.columns.iter().enumerate() {
                column_map.insert(
                    (table_name.clone(), col.name.to_string()),
                    (table_idx, col_idx),
                );
            }
        }

        Self {
            tables,
            descriptors,
            column_map,
        }
    }

    /// Create a single-table descriptor (for non-join queries).
    pub fn single(table: impl Into<String>, descriptor: RowDescriptor) -> Self {
        let table_name = table.into();
        Self::new(vec![table_name], vec![descriptor])
    }

    /// Resolve a qualified column reference to (table_index, column_index).
    pub fn resolve_column(&self, table: &str, column: &str) -> Option<(usize, usize)> {
        self.column_map
            .get(&(table.to_string(), column.to_string()))
            .copied()
    }

    /// Resolve an unqualified column reference (searches all tables, first match wins).
    pub fn resolve_unqualified(&self, column: &str) -> Option<(usize, usize)> {
        for (table_idx, descriptor) in self.descriptors.iter().enumerate() {
            if let Some(col_idx) = descriptor.column_index(column) {
                return Some((table_idx, col_idx));
            }
        }
        None
    }

    /// Get the descriptor for a specific table index.
    pub fn table_descriptor(&self, table_idx: usize) -> Option<&RowDescriptor> {
        self.descriptors.get(table_idx)
    }

    /// Get the table name for a specific index.
    pub fn table_name(&self, table_idx: usize) -> Option<&str> {
        self.tables.get(table_idx).map(|s| s.as_str())
    }

    /// Get total number of tables.
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// Get total number of columns across all tables.
    pub fn total_column_count(&self) -> usize {
        self.descriptors.iter().map(|d| d.columns.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use uuid::Uuid;

    #[test]
    fn column_type_fixed_sizes() {
        assert_eq!(ColumnType::Integer.fixed_size(), Some(4));
        assert_eq!(ColumnType::BigInt.fixed_size(), Some(8));
        assert_eq!(ColumnType::Boolean.fixed_size(), Some(1));
        assert_eq!(ColumnType::Timestamp.fixed_size(), Some(8));
        assert_eq!(ColumnType::Uuid.fixed_size(), Some(16));
        assert_eq!(ColumnType::Text.fixed_size(), None);
    }

    #[test]
    fn column_descriptor_builder() {
        let col = ColumnDescriptor::new("email", ColumnType::Text)
            .nullable()
            .references("users");

        assert_eq!(col.name, "email");
        assert_eq!(col.column_type, ColumnType::Text);
        assert!(col.nullable);
        assert_eq!(col.references, Some(TableName::new("users")));
    }

    #[test]
    fn row_descriptor_column_lookup() {
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Uuid),
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("age", ColumnType::Integer),
        ]);

        assert_eq!(descriptor.column_index("id"), Some(0));
        assert_eq!(descriptor.column_index("name"), Some(1));
        assert_eq!(descriptor.column_index("age"), Some(2));
        assert_eq!(descriptor.column_index("unknown"), None);

        assert_eq!(descriptor.fixed_column_count(), 2); // id (uuid) + age (integer)
        assert_eq!(descriptor.variable_column_count(), 1); // name (text)
    }

    #[test]
    fn value_column_type() {
        assert_eq!(Value::Integer(42).column_type(), Some(ColumnType::Integer));
        assert_eq!(Value::BigInt(42).column_type(), Some(ColumnType::BigInt));
        assert_eq!(
            Value::Boolean(true).column_type(),
            Some(ColumnType::Boolean)
        );
        assert_eq!(
            Value::Text("hello".into()).column_type(),
            Some(ColumnType::Text)
        );
        assert_eq!(
            Value::Timestamp(123).column_type(),
            Some(ColumnType::Timestamp)
        );
        assert_eq!(
            Value::Uuid(ObjectId::from_uuid(Uuid::nil())).column_type(),
            Some(ColumnType::Uuid)
        );
        assert_eq!(Value::Null.column_type(), None);
    }

    // ========================================================================
    // Tuple Model Tests
    // ========================================================================

    fn make_commit_id(n: u8) -> CommitId {
        CommitId([n; 32])
    }

    #[test]
    fn tuple_element_id() {
        let id = ObjectId::from_uuid(Uuid::from_u128(42));
        let elem = TupleElement::Id(id);

        assert_eq!(elem.id(), id);
        assert!(!elem.is_materialized());
        assert!(elem.content().is_none());
        assert!(elem.commit_id().is_none());
    }

    #[test]
    fn tuple_element_row() {
        let id = ObjectId::from_uuid(Uuid::from_u128(42));
        let content = vec![1, 2, 3];
        let commit_id = make_commit_id(1);
        let elem = TupleElement::Row {
            id,
            content: content.clone(),
            commit_id,
        };

        assert_eq!(elem.id(), id);
        assert!(elem.is_materialized());
        assert_eq!(elem.content(), Some(content.as_slice()));
        assert_eq!(elem.commit_id(), Some(commit_id));
    }

    #[test]
    fn tuple_element_from_row() {
        let id = ObjectId::from_uuid(Uuid::from_u128(42));
        let row = Row::new(id, vec![1, 2, 3], make_commit_id(1));
        let elem = TupleElement::from_row(&row);

        assert_eq!(elem.id(), id);
        assert!(elem.is_materialized());
        assert_eq!(elem.content(), Some(&[1u8, 2, 3][..]));
    }

    #[test]
    fn tuple_from_id() {
        let id = ObjectId::from_uuid(Uuid::from_u128(42));
        let tuple = Tuple::from_id(id);

        assert_eq!(tuple.len(), 1);
        assert_eq!(tuple.first_id(), Some(id));
        assert!(!tuple.is_fully_materialized());
    }

    #[test]
    fn tuple_from_row() {
        let id = ObjectId::from_uuid(Uuid::from_u128(42));
        let row = Row::new(id, vec![1, 2, 3], make_commit_id(1));
        let tuple = Tuple::from_row(&row);

        assert_eq!(tuple.len(), 1);
        assert_eq!(tuple.first_id(), Some(id));
        assert!(tuple.is_fully_materialized());
    }

    #[test]
    fn tuple_equality_based_on_ids() {
        let id = ObjectId::from_uuid(Uuid::from_u128(42));

        // Two tuples with same ID but different content should be equal
        let tuple1 = Tuple::from_id(id);
        let tuple2 = Tuple::new(vec![TupleElement::Row {
            id,
            content: vec![1, 2, 3],
            commit_id: make_commit_id(1),
        }]);

        assert_eq!(tuple1, tuple2);
    }

    #[test]
    fn tuple_hash_based_on_ids() {
        use std::collections::hash_map::DefaultHasher;

        let id = ObjectId::from_uuid(Uuid::from_u128(42));

        let tuple1 = Tuple::from_id(id);
        let tuple2 = Tuple::new(vec![TupleElement::Row {
            id,
            content: vec![1, 2, 3],
            commit_id: make_commit_id(1),
        }]);

        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        tuple1.hash(&mut hasher1);
        tuple2.hash(&mut hasher2);

        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn tuple_in_hashset() {
        let id1 = ObjectId::from_uuid(Uuid::from_u128(1));
        let id2 = ObjectId::from_uuid(Uuid::from_u128(2));

        let mut set = HashSet::new();
        set.insert(Tuple::from_id(id1));
        set.insert(Tuple::from_id(id2));

        // Same ID with different content should be found
        let tuple_with_content = Tuple::new(vec![TupleElement::Row {
            id: id1,
            content: vec![1, 2, 3],
            commit_id: make_commit_id(1),
        }]);
        assert!(set.contains(&tuple_with_content));
    }

    #[test]
    fn tuple_delta_to_row_delta() {
        let id = ObjectId::from_uuid(Uuid::from_u128(42));
        let row = Row::new(id, vec![1, 2, 3], make_commit_id(1));
        let tuple = Tuple::from_row(&row);

        let tuple_delta = TupleDelta {
            added: vec![tuple],
            removed: vec![],
            updated: vec![],
            pending: true,
        };

        let row_delta = tuple_delta.to_row_delta().unwrap();
        assert_eq!(row_delta.added.len(), 1);
        assert_eq!(row_delta.added[0].id, id);
        assert!(row_delta.pending);
    }

    #[test]
    fn combined_row_descriptor_single() {
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Uuid),
            ColumnDescriptor::new("name", ColumnType::Text),
        ]);

        let combined = CombinedRowDescriptor::single("users", descriptor);

        assert_eq!(combined.table_count(), 1);
        assert_eq!(combined.resolve_column("users", "id"), Some((0, 0)));
        assert_eq!(combined.resolve_column("users", "name"), Some((0, 1)));
        assert_eq!(combined.resolve_unqualified("name"), Some((0, 1)));
    }

    #[test]
    fn combined_row_descriptor_join() {
        let users_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Uuid),
            ColumnDescriptor::new("name", ColumnType::Text),
        ]);
        let posts_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Uuid),
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("author_id", ColumnType::Uuid),
        ]);

        let combined = CombinedRowDescriptor::new(
            vec!["users".to_string(), "posts".to_string()],
            vec![users_desc, posts_desc],
        );

        assert_eq!(combined.table_count(), 2);
        assert_eq!(combined.total_column_count(), 5);

        // Qualified lookups
        assert_eq!(combined.resolve_column("users", "id"), Some((0, 0)));
        assert_eq!(combined.resolve_column("users", "name"), Some((0, 1)));
        assert_eq!(combined.resolve_column("posts", "id"), Some((1, 0)));
        assert_eq!(combined.resolve_column("posts", "title"), Some((1, 1)));
        assert_eq!(combined.resolve_column("posts", "author_id"), Some((1, 2)));

        // Unqualified lookup (first match wins)
        // "id" exists in both tables, should return users.id
        assert_eq!(combined.resolve_unqualified("id"), Some((0, 0)));
        // "title" only exists in posts
        assert_eq!(combined.resolve_unqualified("title"), Some((1, 1)));
    }

    // ========================================================================
    // TupleDescriptor Tests
    // ========================================================================

    #[test]
    fn tuple_descriptor_single_table() {
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ]);

        let td = TupleDescriptor::single("users", descriptor);

        assert_eq!(td.element_count(), 1);
        assert_eq!(td.total_columns(), 2);
        assert_eq!(td.resolve_column(0), Some((0, 0))); // column 0 -> element 0, local 0
        assert_eq!(td.resolve_column(1), Some((0, 1))); // column 1 -> element 0, local 1
        assert_eq!(td.resolve_column(2), None); // out of range
    }

    #[test]
    fn tuple_descriptor_join() {
        let users_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ]);
        let posts_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("author_id", ColumnType::Integer),
        ]);

        let td = TupleDescriptor::from_tables(&[
            ("users".to_string(), users_desc),
            ("posts".to_string(), posts_desc),
        ]);

        assert_eq!(td.element_count(), 2);
        assert_eq!(td.total_columns(), 5);

        // users columns (0-1)
        assert_eq!(td.resolve_column(0), Some((0, 0))); // users.id
        assert_eq!(td.resolve_column(1), Some((0, 1))); // users.name

        // posts columns (2-4)
        assert_eq!(td.resolve_column(2), Some((1, 0))); // posts.id
        assert_eq!(td.resolve_column(3), Some((1, 1))); // posts.title
        assert_eq!(td.resolve_column(4), Some((1, 2))); // posts.author_id

        assert_eq!(td.resolve_column(5), None); // out of range
    }

    #[test]
    fn tuple_descriptor_elements_for_columns() {
        let users_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ]);
        let posts_desc = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("title", ColumnType::Text),
        ]);

        let td = TupleDescriptor::from_tables(&[
            ("users".to_string(), users_desc),
            ("posts".to_string(), posts_desc),
        ]);

        // Only need users.id (column 0) -> need element 0 only
        let cols: HashSet<usize> = [0].into_iter().collect();
        let elements = td.elements_for_columns(&cols);
        assert_eq!(elements, [0].into_iter().collect());

        // Only need posts.title (column 3) -> need element 1 only
        let cols: HashSet<usize> = [3].into_iter().collect();
        let elements = td.elements_for_columns(&cols);
        assert_eq!(elements, [1].into_iter().collect());

        // Need both users.name and posts.title -> need both elements
        let cols: HashSet<usize> = [1, 3].into_iter().collect();
        let elements = td.elements_for_columns(&cols);
        assert_eq!(elements, [0, 1].into_iter().collect());
    }

    #[test]
    fn tuple_descriptor_combined_descriptor() {
        let users_desc = RowDescriptor::new(vec![ColumnDescriptor::new("id", ColumnType::Integer)]);
        let posts_desc = RowDescriptor::new(vec![ColumnDescriptor::new("title", ColumnType::Text)]);

        let td = TupleDescriptor::from_tables(&[
            ("users".to_string(), users_desc),
            ("posts".to_string(), posts_desc),
        ]);

        let combined = td.combined_descriptor();
        assert_eq!(combined.columns.len(), 2);
        assert_eq!(combined.columns[0].name, "id");
        assert_eq!(combined.columns[1].name, "title");
    }

    // ========================================================================
    // Schema Hash Tests
    // ========================================================================

    #[test]
    fn schema_hash_deterministic() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();

        let hash1 = SchemaHash::compute(&schema);
        let hash2 = SchemaHash::compute(&schema);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn schema_hash_column_order_independent() {
        // Schema with columns in different order should have same hash
        let schema1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .column("age", ColumnType::Integer),
            )
            .build();

        // Build same schema with different column order
        let schema2: Schema = [(
            TableName::new("users"),
            TableSchema::new(RowDescriptor::new(vec![
                ColumnDescriptor::new("age", ColumnType::Integer),
                ColumnDescriptor::new("id", ColumnType::Uuid),
                ColumnDescriptor::new("name", ColumnType::Text),
            ])),
        )]
        .into_iter()
        .collect();

        let hash1 = SchemaHash::compute(&schema1);
        let hash2 = SchemaHash::compute(&schema2);
        assert_eq!(hash1, hash2, "Column order should not affect hash");
    }

    #[test]
    fn schema_hash_table_order_independent() {
        // Build with tables in different orders
        let schema1 = SchemaBuilder::new()
            .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
            .table(TableSchema::builder("posts").column("id", ColumnType::Uuid))
            .build();

        let schema2 = SchemaBuilder::new()
            .table(TableSchema::builder("posts").column("id", ColumnType::Uuid))
            .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
            .build();

        let hash1 = SchemaHash::compute(&schema1);
        let hash2 = SchemaHash::compute(&schema2);
        assert_eq!(hash1, hash2, "Table order should not affect hash");
    }

    #[test]
    fn schema_hash_different_schemas() {
        let schema1 = SchemaBuilder::new()
            .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
            .build();

        let schema2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();

        let hash1 = SchemaHash::compute(&schema1);
        let hash2 = SchemaHash::compute(&schema2);
        assert_ne!(
            hash1, hash2,
            "Different schemas should have different hashes"
        );
    }

    #[test]
    fn schema_hash_short() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
            .build();

        let hash = SchemaHash::compute(&schema);
        let short = hash.short();

        assert_eq!(short.len(), 8, "Short hash should be 8 hex chars");
        assert!(short.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn table_schema_builder() {
        let (name, schema) = TableSchema::builder("users")
            .column("id", ColumnType::Uuid)
            .nullable_column("email", ColumnType::Text)
            .fk_column("org_id", "orgs")
            .nullable_fk_column("manager_id", "users")
            .build_named();

        assert_eq!(name.as_str(), "users");
        assert_eq!(schema.descriptor.columns.len(), 4);

        let id_col = schema.descriptor.column("id").unwrap();
        assert_eq!(id_col.column_type, ColumnType::Uuid);
        assert!(!id_col.nullable);

        let email_col = schema.descriptor.column("email").unwrap();
        assert_eq!(email_col.column_type, ColumnType::Text);
        assert!(email_col.nullable);

        let org_col = schema.descriptor.column("org_id").unwrap();
        assert_eq!(org_col.column_type, ColumnType::Uuid);
        assert!(!org_col.nullable);
        assert_eq!(org_col.references, Some(TableName::new("orgs")));

        let manager_col = schema.descriptor.column("manager_id").unwrap();
        assert!(manager_col.nullable);
        assert_eq!(manager_col.references, Some(TableName::new("users")));
    }

    #[test]
    fn row_descriptor_content_hash() {
        let desc1 = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Uuid),
            ColumnDescriptor::new("name", ColumnType::Text),
        ]);

        let desc2 = RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("id", ColumnType::Uuid),
        ]);

        // Same columns, different order -> same hash (order-independent)
        assert_eq!(desc1.content_hash(), desc2.content_hash());

        // Different columns -> different hash
        let desc3 = RowDescriptor::new(vec![ColumnDescriptor::new("id", ColumnType::Uuid)]);
        assert_ne!(desc1.content_hash(), desc3.content_hash());
    }

    // ========================================================================
    // ComposedBranchName Tests
    // ========================================================================

    #[test]
    fn composed_branch_name_format() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
            .build();

        let composed = ComposedBranchName::from_schema("dev", &schema, "main");
        let branch_name = composed.to_branch_name();
        let s = branch_name.as_str();

        // Should be in format: dev-XXXXXXXX-main
        assert!(s.starts_with("dev-"));
        assert!(s.ends_with("-main"));
        assert_eq!(s.matches('-').count(), 2);
    }

    #[test]
    fn composed_branch_name_parse() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
            .build();

        let original = ComposedBranchName::from_schema("prod", &schema, "feature-x");
        let branch_name = original.to_branch_name();
        let parsed = ComposedBranchName::parse(&branch_name).unwrap();

        assert_eq!(parsed.env, "prod");
        assert_eq!(parsed.user_branch, "feature-x");
        // Note: full hash can't be recovered from 8 chars, but short() should match
        assert_eq!(parsed.schema_hash.short(), original.schema_hash.short());
    }

    #[test]
    fn composed_branch_name_parse_invalid() {
        // Too few parts
        let name = BranchName::new("just-one");
        assert!(ComposedBranchName::parse(&name).is_none());

        // Hash not 8 chars
        let name = BranchName::new("dev-abc-main");
        assert!(ComposedBranchName::parse(&name).is_none());

        // Hash not hex
        let name = BranchName::new("dev-gggggggg-main");
        assert!(ComposedBranchName::parse(&name).is_none());
    }

    #[test]
    fn composed_branch_name_matches() {
        let hash = SchemaHash::from_bytes([0xab; 32]);
        let composed = ComposedBranchName::new("dev", hash, "main");

        assert!(composed.matches_env_and_branch("dev", "main"));
        assert!(!composed.matches_env_and_branch("prod", "main"));
        assert!(!composed.matches_env_and_branch("dev", "feature"));
    }
}
