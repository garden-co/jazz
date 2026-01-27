//! SchemaManager - Coordinates schema evolution with query execution.
//!
//! This provides the top-level API for schema-aware queries, combining:
//! - SchemaContext for tracking current/live schema versions
//! - Lens management for migrations
//! - Schema-aware branch naming
//! - Integrated QueryManager for query/insert/update/delete operations

use crate::object::{BranchName, ObjectId};
use crate::query_manager::manager::{DeleteHandle, InsertHandle, QueryError, QueryManager};
use crate::query_manager::query::{Query, QueryBuilder};
use crate::query_manager::session::Session;
use crate::query_manager::types::{ComposedBranchName, Schema, SchemaHash, Value};
use crate::sync_manager::SyncManager;

use super::auto_lens::generate_lens;
use super::context::{SchemaContext, SchemaError};
use super::lens::Lens;

/// SchemaManager coordinates schema evolution with query execution.
///
/// It manages:
/// - Current schema and environment
/// - Live schema versions reachable via lenses
/// - Lens registration and auto-generation
/// - Schema-aware branch naming
/// - Query execution with automatic lens transforms
///
/// # Example
///
/// ```ignore
/// let mut manager = SchemaManager::new(
///     SyncManager::new(),
///     schema,
///     "dev",
///     "main",
/// )?;
///
/// // Add a previous schema version as "live"
/// manager.add_live_schema(old_schema)?;
///
/// // Insert data
/// let handle = manager.insert("users", &[id, name])?;
///
/// // Query across all schema versions
/// let results = manager.execute(manager.query("users").build())?;
/// ```
pub struct SchemaManager {
    context: SchemaContext,
    query_manager: QueryManager,
}

impl SchemaManager {
    /// Create a new SchemaManager with integrated QueryManager.
    pub fn new(
        sync_manager: SyncManager,
        schema: Schema,
        env: &str,
        user_branch: &str,
    ) -> Result<Self, SchemaError> {
        let context = SchemaContext::new(schema.clone(), env, user_branch);
        let query_manager =
            QueryManager::new_with_schema_context(sync_manager, schema, context.clone());
        Ok(Self {
            context,
            query_manager,
        })
    }

    /// Create with default environment ("dev").
    pub fn with_defaults(
        sync_manager: SyncManager,
        schema: Schema,
        user_branch: &str,
    ) -> Result<Self, SchemaError> {
        Self::new(sync_manager, schema, "dev", user_branch)
    }

    /// Get the current schema.
    pub fn current_schema(&self) -> &Schema {
        &self.context.current_schema
    }

    /// Get the current schema hash.
    pub fn current_hash(&self) -> SchemaHash {
        self.context.current_hash
    }

    /// Get the composed branch name for the current schema.
    pub fn branch_name(&self) -> BranchName {
        self.context.branch_name()
    }

    /// Get branch names for all live schemas (current + live).
    pub fn all_branches(&self) -> Vec<BranchName> {
        self.context.all_branch_names()
    }

    /// Get the environment.
    pub fn env(&self) -> &str {
        &self.context.env
    }

    /// Get the user branch.
    pub fn user_branch(&self) -> &str {
        &self.context.user_branch
    }

    /// Add a live schema version with auto-generated lens.
    ///
    /// The lens is automatically generated from the schema diff.
    /// Returns error if the generated lens is a draft (needs manual review).
    ///
    /// Note: Call `sync_context()` after adding live schemas to update QueryManager.
    pub fn add_live_schema(&mut self, old_schema: Schema) -> Result<&Lens, SchemaError> {
        let lens = generate_lens(&old_schema, &self.context.current_schema);

        if lens.is_draft() {
            return Err(SchemaError::DraftLensInPath {
                source: lens.source_hash,
                target: lens.target_hash,
            });
        }

        let source_hash = lens.source_hash;
        self.context.add_live_schema(old_schema, lens);

        // Return reference to the registered lens
        self.context
            .get_lens(&source_hash, &self.context.current_hash)
            .ok_or(SchemaError::LensNotFound {
                source: source_hash,
                target: self.context.current_hash,
            })
    }

    /// Add a live schema version with explicit lens.
    ///
    /// Use this when auto-generated lens needs customization or
    /// when adding a schema with a manual migration.
    ///
    /// Note: Call `sync_context()` after adding live schemas to update QueryManager.
    pub fn add_live_schema_with_lens(
        &mut self,
        old_schema: Schema,
        lens: Lens,
    ) -> Result<(), SchemaError> {
        if lens.is_draft() {
            return Err(SchemaError::DraftLensInPath {
                source: lens.source_hash,
                target: lens.target_hash,
            });
        }
        self.context.add_live_schema(old_schema, lens);
        Ok(())
    }

    /// Register a lens between two schemas.
    pub fn register_lens(&mut self, lens: Lens) -> Result<(), SchemaError> {
        if lens.is_draft() {
            return Err(SchemaError::DraftLensInPath {
                source: lens.source_hash,
                target: lens.target_hash,
            });
        }
        self.context.register_lens(lens);
        Ok(())
    }

    /// Get lens between two schemas if it exists.
    pub fn get_lens(&self, source: &SchemaHash, target: &SchemaHash) -> Option<&Lens> {
        self.context.get_lens(source, target)
    }

    /// Generate a lens between two schemas (may be draft).
    ///
    /// This doesn't register the lens - use `register_lens` after review.
    pub fn generate_lens(&self, old_schema: &Schema, new_schema: &Schema) -> Lens {
        generate_lens(old_schema, new_schema)
    }

    /// Get the lens path from a live schema to the current schema.
    pub fn lens_path(&self, from: &SchemaHash) -> Result<Vec<&Lens>, SchemaError> {
        self.context.lens_path(from)
    }

    /// Validate that all live schemas are reachable via non-draft lenses.
    pub fn validate(&self) -> Result<(), SchemaError> {
        self.context.validate()
    }

    /// Check if a schema hash is live (current or in live_schemas).
    pub fn is_live(&self, hash: &SchemaHash) -> bool {
        self.context.is_live(hash)
    }

    /// Get all live schema hashes.
    pub fn all_live_hashes(&self) -> Vec<SchemaHash> {
        self.context.all_live_hashes()
    }

    /// Get access to the underlying context.
    pub fn context(&self) -> &SchemaContext {
        &self.context
    }

    /// Get mutable access to the underlying context.
    pub fn context_mut(&mut self) -> &mut SchemaContext {
        &mut self.context
    }

    /// Get reference to the internal QueryManager.
    pub fn query_manager(&self) -> &QueryManager {
        &self.query_manager
    }

    /// Get mutable reference to the internal QueryManager.
    pub fn query_manager_mut(&mut self) -> &mut QueryManager {
        &mut self.query_manager
    }

    // =========================================================================
    // Multi-Schema Query Support
    // =========================================================================

    /// Get branch names as strings for use with QueryBuilder.
    pub fn all_branch_strings(&self) -> Vec<String> {
        self.context
            .all_branch_names()
            .into_iter()
            .map(|b| b.as_str().to_string())
            .collect()
    }

    /// Build a mapping from branch name to schema hash.
    pub fn branch_schema_map(&self) -> std::collections::HashMap<String, SchemaHash> {
        let mut map = std::collections::HashMap::new();

        // Current schema branch
        map.insert(
            self.context.branch_name().as_str().to_string(),
            self.context.current_hash,
        );

        // Live schema branches
        for hash in self.context.live_schemas.keys() {
            let branch =
                ComposedBranchName::new(&self.context.env, *hash, &self.context.user_branch)
                    .to_branch_name();
            map.insert(branch.as_str().to_string(), *hash);
        }

        map
    }

    /// Create a LensTransformer for a specific table.
    pub fn transformer(&self, table: &str) -> super::transformer::LensTransformer<'_> {
        super::transformer::LensTransformer::new(&self.context, table)
    }

    /// Translate a column name for index lookup on a specific schema version.
    pub fn translate_column_for_schema(
        &self,
        table: &str,
        column: &str,
        target_hash: &SchemaHash,
    ) -> Option<String> {
        super::transformer::translate_column_for_index(&self.context, table, column, target_hash)
    }

    /// Get the descriptor for a table in a specific schema version.
    pub fn get_table_descriptor(
        &self,
        table: &str,
        schema_hash: &SchemaHash,
    ) -> Option<&crate::query_manager::types::RowDescriptor> {
        let schema = self.context.get_schema(schema_hash)?;
        let table_schema = schema.get(&crate::query_manager::types::TableName::new(table))?;
        Some(&table_schema.descriptor)
    }

    // =========================================================================
    // Query/Write Operations (delegated to QueryManager)
    // =========================================================================

    /// Create a query builder for a table.
    pub fn query(&self, table: &str) -> QueryBuilder {
        QueryBuilder::new(table)
    }

    /// Execute a query and return results (one-shot).
    ///
    /// Automatically queries across all live schema versions.
    /// Rows from old schemas are transformed via lens to current schema format.
    pub fn execute(&mut self, query: Query) -> Result<Vec<Vec<Value>>, QueryError> {
        self.query_manager.execute(query)
    }

    /// Insert a row into the current schema's branch.
    pub fn insert(&mut self, table: &str, values: &[Value]) -> Result<InsertHandle, QueryError> {
        self.insert_with_session(table, values, None)
    }

    /// Insert with session-based policy checking.
    pub fn insert_with_session(
        &mut self,
        table: &str,
        values: &[Value],
        session: Option<&Session>,
    ) -> Result<InsertHandle, QueryError> {
        self.query_manager.insert_on_branch_with_session(
            table,
            self.context.branch_name().as_str(),
            values,
            session,
        )
    }

    /// Delete a row (soft delete) from current schema's branch.
    pub fn delete(&mut self, table: &str, object_id: ObjectId) -> Result<DeleteHandle, QueryError> {
        self.query_manager
            .delete_on_branch(table, self.context.branch_name().as_str(), object_id)
    }

    /// Process pending operations (drives SyncManager).
    pub fn process(&mut self) {
        self.query_manager.process();
    }

    /// Rebuild QueryManager's schema context after adding live schemas.
    ///
    /// Call this after `add_live_schema` to update QueryManager's schema awareness.
    pub fn sync_context(&mut self) {
        let sync_manager = std::mem::take(self.query_manager.sync_manager_mut());
        let schema = (*self.query_manager.schema()).clone();
        self.query_manager =
            QueryManager::new_with_schema_context(sync_manager, schema, self.context.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::types::{ColumnType, SchemaBuilder, TableSchema};

    fn make_schema_v1() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build()
    }

    fn make_schema_v2() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .nullable_column("email", ColumnType::Text),
            )
            .build()
    }

    #[test]
    fn schema_manager_new() {
        let schema = make_schema_v1();
        let manager = SchemaManager::new(SyncManager::new(), schema, "dev", "main").unwrap();

        assert_eq!(manager.env(), "dev");
        assert_eq!(manager.user_branch(), "main");
    }

    #[test]
    fn schema_manager_branch_name() {
        let schema = make_schema_v1();
        let manager = SchemaManager::new(SyncManager::new(), schema, "prod", "feature").unwrap();

        let branch = manager.branch_name();
        let s = branch.as_str();

        assert!(s.starts_with("prod-"));
        assert!(s.ends_with("-feature"));
    }

    #[test]
    fn schema_manager_add_live_schema() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();

        let mut manager = SchemaManager::new(SyncManager::new(), v2, "dev", "main").unwrap();
        let lens = manager.add_live_schema(v1).unwrap();

        assert!(!lens.is_draft());
        assert_eq!(manager.all_branches().len(), 2);
    }

    #[test]
    fn schema_manager_add_live_schema_draft_fails() {
        let v1 = make_schema_v1();
        // Add non-nullable UUID column - creates draft lens
        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .column("org_id", ColumnType::Uuid), // non-nullable UUID = draft
            )
            .build();

        let mut manager = SchemaManager::new(SyncManager::new(), v2, "dev", "main").unwrap();
        let result = manager.add_live_schema(v1);

        assert!(matches!(result, Err(SchemaError::DraftLensInPath { .. })));
    }

    #[test]
    fn schema_manager_explicit_lens() {
        use crate::schema_manager::lens::{LensOp, LensTransform};

        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        // Create explicit lens
        let mut transform = LensTransform::new();
        transform.push(
            LensOp::AddColumn {
                table: "users".into(),
                column: "email".into(),
                column_type: ColumnType::Text,
                default: crate::query_manager::types::Value::Null,
            },
            false, // not draft
        );
        let lens = Lens::new(v1_hash, v2_hash, transform);

        let mut manager = SchemaManager::new(SyncManager::new(), v2, "dev", "main").unwrap();
        manager.add_live_schema_with_lens(v1, lens).unwrap();

        assert_eq!(manager.all_branches().len(), 2);
    }

    #[test]
    fn schema_manager_validate() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();

        let mut manager = SchemaManager::new(SyncManager::new(), v2, "dev", "main").unwrap();
        manager.add_live_schema(v1).unwrap();

        // Should pass - no draft lenses
        assert!(manager.validate().is_ok());
    }

    #[test]
    fn schema_manager_lens_path() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v1_hash = SchemaHash::compute(&v1);

        let mut manager = SchemaManager::new(SyncManager::new(), v2, "dev", "main").unwrap();
        manager.add_live_schema(v1).unwrap();

        let path = manager.lens_path(&v1_hash).unwrap();
        assert_eq!(path.len(), 1);
    }

    #[test]
    fn schema_manager_generate_lens_without_register() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();

        let manager = SchemaManager::new(SyncManager::new(), v2.clone(), "dev", "main").unwrap();
        let lens = manager.generate_lens(&v1, &v2);

        // Generated but not registered
        assert!(!lens.is_draft());
        assert_eq!(manager.all_branches().len(), 1); // Only current
    }

    #[test]
    fn schema_manager_branch_schema_map() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        let mut manager = SchemaManager::new(SyncManager::new(), v2, "dev", "main").unwrap();
        manager.add_live_schema(v1).unwrap();

        let map = manager.branch_schema_map();
        assert_eq!(map.len(), 2);

        // Should contain both schema hashes
        let hashes: std::collections::HashSet<_> = map.values().collect();
        assert!(hashes.contains(&v1_hash));
        assert!(hashes.contains(&v2_hash));
    }

    #[test]
    fn schema_manager_all_branch_strings() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();

        let mut manager = SchemaManager::new(SyncManager::new(), v2, "dev", "main").unwrap();
        manager.add_live_schema(v1).unwrap();

        let branches = manager.all_branch_strings();
        assert_eq!(branches.len(), 2);

        // All should have correct format
        for branch in &branches {
            assert!(branch.starts_with("dev-"));
            assert!(branch.ends_with("-main"));
        }
    }

    #[test]
    fn schema_manager_get_table_descriptor() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        let mut manager = SchemaManager::new(SyncManager::new(), v2, "dev", "main").unwrap();
        manager.add_live_schema(v1).unwrap();

        // V1 has 2 columns (id, name)
        let v1_desc = manager.get_table_descriptor("users", &v1_hash).unwrap();
        assert_eq!(v1_desc.columns.len(), 2);

        // V2 has 3 columns (id, name, email)
        let v2_desc = manager.get_table_descriptor("users", &v2_hash).unwrap();
        assert_eq!(v2_desc.columns.len(), 3);
    }

    #[test]
    fn schema_manager_translate_column() {
        use crate::schema_manager::lens::{LensOp, LensTransform};

        // Create schemas where a column was renamed
        let v1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .build();

        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email_address", ColumnType::Text),
            )
            .build();

        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        // Create explicit rename lens
        let mut transform = LensTransform::new();
        transform.push(
            LensOp::RenameColumn {
                table: "users".to_string(),
                old_name: "email".to_string(),
                new_name: "email_address".to_string(),
            },
            false,
        );
        let lens = Lens::new(v1_hash, v2_hash, transform);

        let mut manager = SchemaManager::new(SyncManager::new(), v2, "dev", "main").unwrap();
        manager.add_live_schema_with_lens(v1, lens).unwrap();

        // Current schema uses "email_address"
        // For v1 index, we need "email"
        let translated = manager
            .translate_column_for_schema("users", "email_address", &v1_hash)
            .unwrap();
        assert_eq!(translated, "email");

        // For v2 (current), no translation needed
        let current = manager
            .translate_column_for_schema("users", "email_address", &v2_hash)
            .unwrap();
        assert_eq!(current, "email_address");
    }

    #[test]
    fn schema_manager_insert_and_query() {
        use crate::object::ObjectId;

        let schema = make_schema_v2();
        let mut manager = SchemaManager::new(SyncManager::new(), schema, "dev", "main").unwrap();

        // Insert a row
        let id = ObjectId::new();
        let id_val = Value::Uuid(id);
        let name = Value::Text("Alice".into());
        let email = Value::Text("alice@example.com".into());

        let _handle = manager
            .insert("users", &[id_val.clone(), name, email])
            .unwrap();
        manager.process();

        // Query
        let results = manager.execute(manager.query("users").build()).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], id_val);
    }
}
