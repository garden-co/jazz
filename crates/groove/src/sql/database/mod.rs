use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

use crate::listener::ListenerId;
use crate::node::{LocalNode, generate_object_id};
use crate::object::ObjectId;
use crate::sql::catalog::{Catalog, DescriptorId, TableDescriptor};
use crate::sql::index::RefIndex;
use crate::sql::lens::{
    Lens, LensError, LensGenerationOptions, LensWarning, diff_schemas, generate_lens,
};
use crate::sql::parser::{
    self, Condition, ConditionValue, Projection, Select, SelectExpr, Statement,
};
use crate::sql::policy::{
    Policy, PolicyAction, PolicyError, PolicyExpr, PolicyValue, TablePolicies,
};
use crate::sql::query_graph::registry::{GraphRegistry, OutputCallback};
use crate::sql::query_graph::{
    DeltaBatch, GraphId, Predicate, PredicateValue, QueryGraphBuilder, RowDelta,
};
use crate::sql::row::RowError;
use crate::sql::row_buffer::{OwnedRow, RowBuilder, RowDescriptor, RowValue};
use crate::sql::schema::{ColumnType, SchemaError, TableSchema};
use crate::sql::table_rows::TableRows;
use crate::sql::types::{IndexKey, SchemaId};
use crate::storage::Environment;

/// Coerce a PredicateValue to match the expected ColumnType.
/// This converts String values to Ref(ObjectId) when the column type is Ref.
fn coerce_predicate_value(value: &PredicateValue, ty: &ColumnType) -> PredicateValue {
    match (value, ty) {
        // String to Ref coercion: parse the string as ObjectId
        (PredicateValue::String(s), ColumnType::Ref(_)) => {
            if let Ok(id) = s.parse::<ObjectId>() {
                PredicateValue::Ref(id)
            } else {
                value.clone() // Keep as string if not a valid ObjectId
            }
        }
        // No coercion needed
        _ => value.clone(),
    }
}

#[cfg(test)]
mod tests;

// ========== JOIN Support ==========

// ========== Incremental Query (Query Graph based) ==========

/// A handle to an incremental query graph.
///
/// Uses incremental computation - only processing the delta from each
/// change and propagating it through the computation graph.
///
/// The query is automatically cleaned up when this handle is dropped.
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
    /// Get the current query output rows in buffer format.
    pub fn rows(&self) -> Vec<(ObjectId, OwnedRow)> {
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
    pub fn subscribe(&self, callback: OutputCallback) -> Option<ListenerId> {
        // Get current state and send as initial "Added" deltas
        let initial_rows = self.rows();

        let initial_deltas: DeltaBatch = initial_rows
            .into_iter()
            .map(|(id, row)| RowDelta::Added { id, row })
            .collect();
        callback(&initial_deltas);

        // Subscribe for future changes
        self.db_state
            .graph_registry
            .subscribe(self.graph_id, callback)
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
        self.db_state.graph_registry.unregister(self.graph_id);
    }
}

/// Shared database state that can be held by queries for re-evaluation.
/// This is the core data that queries need access to.
pub struct DatabaseState {
    /// Shared reference to the underlying object store.
    /// This can be shared with SyncedNode for sync operations.
    node: Arc<LocalNode>,
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
    /// Get a reference to the underlying LocalNode.
    pub fn node(&self) -> &LocalNode {
        &self.node
    }

    /// Get the underlying LocalNode as an Arc (for sharing with SyncedNode).
    pub fn node_arc(&self) -> Arc<LocalNode> {
        Arc::clone(&self.node)
    }

    /// Get the catalog object ID.
    pub fn catalog_object_id(&self) -> ObjectId {
        self.catalog_object_id
    }

    /// Get the table rows object ID for a table.
    pub fn table_rows_object_id(&self, table: &str) -> Option<ObjectId> {
        self.table_rows_objects.read().unwrap().get(table).copied()
    }

    /// Get the descriptor object ID for a table.
    pub fn descriptor_object_id(&self, table: &str) -> Option<ObjectId> {
        self.descriptor_objects.read().unwrap().get(table).copied()
    }

    /// Reload the catalog from storage.
    ///
    /// This re-reads the catalog object and all table descriptors,
    /// refreshing the in-memory schema cache.
    pub fn reload_catalog(&self) -> Result<(), DatabaseError> {
        // Read the catalog object
        let catalog_bytes = self
            .node
            .read(self.catalog_object_id, "main")
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?
            .ok_or_else(|| DatabaseError::Storage("catalog not found".to_string()))?;

        let catalog = Catalog::from_bytes(&catalog_bytes)
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

        // Clear existing schema cache
        self.tables.write().unwrap().clear();
        self.schemas.write().unwrap().clear();
        self.table_rows_objects.write().unwrap().clear();
        self.descriptor_objects.write().unwrap().clear();
        self.policies.write().unwrap().clear();

        // Reload each table from the catalog
        for (table_name, descriptor_id) in &catalog.tables {
            let desc_object_id = descriptor_id.as_object_id();

            // Try to read the descriptor from "main" branch
            // TODO: For schema migrations, we may need to read from version branches
            // and sync them properly. For now, "main" always has the latest descriptor.
            if let Ok(Some(desc_bytes)) = self.node.read(desc_object_id, "main") {
                if let Ok(descriptor) = TableDescriptor::from_bytes(&desc_bytes) {
                    let schema_id: SchemaId = desc_object_id;

                    self.tables
                        .write()
                        .unwrap()
                        .insert(table_name.clone(), schema_id);
                    self.schemas
                        .write()
                        .unwrap()
                        .insert(schema_id, descriptor.schema.clone());
                    self.descriptor_objects
                        .write()
                        .unwrap()
                        .insert(table_name.clone(), desc_object_id);

                    // Load table rows object ID and ensure the object exists locally.
                    // NOTE: table_rows is local state - each node tracks which rows it knows about.
                    // We don't sync the table_rows content, but we use the same object ID
                    // (from the descriptor) so all nodes store their local row set consistently.
                    self.table_rows_objects
                        .write()
                        .unwrap()
                        .insert(table_name.clone(), descriptor.rows_object_id);
                    self.node.ensure_object(
                        descriptor.rows_object_id,
                        &format!("table_rows:{}", table_name),
                    );

                    // Load policies if present
                    if !descriptor.policies.is_empty() {
                        self.policies
                            .write()
                            .unwrap()
                            .insert(table_name.clone(), descriptor.policies.clone());
                    }
                }
            }
        }

        Ok(())
    }

    fn new(env: Arc<dyn Environment>) -> Self {
        let node = Arc::new(LocalNode::new(env));

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
        let node = Arc::new(LocalNode::in_memory());

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

    /// Create in-memory database state with a specific catalog ID.
    /// If the catalog already exists in the node, it will be reused.
    /// This allows multiple clients to share the same catalog via sync.
    fn in_memory_with_catalog(catalog_object_id: ObjectId) -> Self {
        let node = Arc::new(LocalNode::in_memory());

        // Ensure catalog object exists (creates if new, reuses if exists)
        let is_new = node.ensure_object(catalog_object_id, "catalog");

        if is_new {
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
        }

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

    /// Create in-memory database state as a replica waiting for catalog sync.
    ///
    /// Unlike `in_memory_with_catalog`, this does NOT write an initial empty catalog.
    /// The replica expects to receive the catalog via sync from an upstream server.
    /// Use `has_catalog()` or `await_catalog()` to check/wait for catalog arrival.
    fn in_memory_replica(catalog_object_id: ObjectId) -> Self {
        let node = Arc::new(LocalNode::in_memory());

        // Create the catalog object but don't write to it
        node.ensure_object(catalog_object_id, "catalog");

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

    /// Check if the catalog has been synced (has exactly one tip).
    pub fn has_catalog(&self) -> bool {
        self.node
            .read(self.catalog_object_id, "main")
            .ok()
            .flatten()
            .is_some()
    }

    /// Set up the callback for sync-applied commits.
    ///
    /// This must be called after the DatabaseState is wrapped in Arc, passing a clone
    /// of the Arc. The callback will be invoked when commits are applied via
    /// `LocalNode::apply_commits`, which happens during sync.
    ///
    /// The callback:
    /// 1. Looks up the table name from object metadata
    /// 2. Rebuilds column change metadata for proper per-column LWW merge
    /// 3. Notifies query graphs of the change
    ///
    fn setup_sync_callback(state: Arc<DatabaseState>) {
        let state_clone = Arc::clone(&state);
        let callback = Rc::new(
            move |object_id: ObjectId, branch: &str, _commits: &[crate::commit::Commit]| {
                // Look up table name from object metadata
                let table = {
                    if let Some(obj_lock) = state_clone.node.get_object(object_id)
                        && let Ok(obj) = obj_lock.read()
                        && let Some(ref meta) = obj.meta
                        && let Some(table_name) = meta.get("table")
                    {
                        table_name.clone()
                    } else {
                        // Object doesn't have table metadata - not a row object, skip
                        return;
                    }
                };

                // Rebuild column change metadata
                if let Some(schema) = state_clone.get_schema(&table) {
                    let descriptor = RowDescriptor::from_table_schema(&schema);
                    if let Some(obj_lock) = state_clone.node.get_object(object_id)
                        && let Ok(obj) = obj_lock.read()
                        && let Some(branch_ref) = obj.branch_ref(branch)
                        && let Ok(mut branch_guard) = branch_ref.write()
                    {
                        branch_guard.rebuild_column_changes(&descriptor);
                    }
                }

                // Notify query graphs
                state_clone.notify_object_changed_sync(&table, object_id);
            },
        );

        state.node.set_on_commits_applied(Some(callback));
    }

    /// Notify query graphs about an object change (for sync-applied commits).
    ///
    /// This is similar to notify_object_changed_internal but doesn't require
    /// going through Database since we're already in the callback context.
    fn notify_object_changed_sync(&self, table: &str, object_id: ObjectId) {
        if let Some(obj_lock) = self.node.get_object(object_id)
            && let Ok(obj) = obj_lock.read()
        {
            self.graph_registry
                .notify_object_changed(table, object_id, &obj, self);
        }
    }

    /// Get a table schema by name.
    fn get_schema(&self, table: &str) -> Option<TableSchema> {
        let tables = self.tables.read().unwrap();
        let schema_id = tables.get(table)?;
        let schemas = self.schemas.read().unwrap();
        schemas.get(schema_id).cloned()
    }

    /// Get an Object by ID from the underlying LocalNode.
    ///
    /// This provides access to objects for branch-aware queries that need
    /// to read from the object's branches and perform per-column merging.
    pub fn get_object(
        &self,
        id: ObjectId,
    ) -> Option<std::sync::Arc<std::sync::RwLock<crate::object::Object>>> {
        self.node.get_object(id)
    }

    /// Read all rows from a table in buffer format.
    pub fn read_all_rows(&self, table: &str) -> Vec<(ObjectId, OwnedRow)> {
        let schema = match self.get_schema(table) {
            Some(s) => s,
            None => return vec![],
        };
        let descriptor = Arc::new(RowDescriptor::from_table_schema(&schema));

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

            // Create OwnedRow directly from buffer
            let owned = OwnedRow::new(descriptor.clone(), data);
            rows.push((row_id, owned));
        }

        rows
    }

    /// Get a single row by ID in buffer format.
    pub fn get_row(&self, table: &str, id: ObjectId) -> Option<(ObjectId, OwnedRow)> {
        let schema = self.get_schema(table)?;
        let descriptor = Arc::new(RowDescriptor::from_table_schema(&schema));

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

        // Create OwnedRow directly from buffer
        let owned = OwnedRow::new(descriptor, data);
        Some((id, owned))
    }

    /// Find all rows where `column` references `target_id`.
    ///
    /// This is used by JOIN queries to find all left rows referencing a right row.
    /// Returns Vec<(ObjectId, OwnedRow)> in buffer format.
    pub fn find_referencing(
        &self,
        table: &str,
        column: &str,
        target_id: ObjectId,
    ) -> Vec<(ObjectId, OwnedRow)> {
        // Use PredicateValue::Ref for the search
        let search_value = PredicateValue::Ref(target_id);

        // Special case: id column (implicit Ref type)
        if column == "id" {
            return match self.get_row(table, target_id) {
                Some((id, owned)) => vec![(id, owned)],
                None => vec![],
            };
        }

        // Find matching rows using PredicateValue comparison
        self.read_all_rows(table)
            .into_iter()
            .filter(|(_, row)| {
                if let Some(row_value) = row.get_by_name(column) {
                    search_value.matches(&row_value)
                } else {
                    false
                }
            })
            .collect()
    }

    /// Load a TableDescriptor by its DescriptorId.
    ///
    /// The DescriptorId contains both the ObjectId and version string,
    /// so we read from the version branch on the descriptor object.
    pub fn load_descriptor_by_id(&self, descriptor_id: DescriptorId) -> Option<TableDescriptor> {
        let object_id = descriptor_id.as_object_id();
        let version_branch = &descriptor_id.version;
        let data = self.node.read(object_id, version_branch).ok()??;
        TableDescriptor::from_bytes(&data).ok()
    }

    /// Load a RowDescriptor by its DescriptorId.
    ///
    /// Convenience method that loads the TableDescriptor and extracts
    /// a RowDescriptor from its schema.
    pub fn load_row_descriptor_by_id(
        &self,
        descriptor_id: DescriptorId,
    ) -> Option<Arc<RowDescriptor>> {
        let table_descriptor = self.load_descriptor_by_id(descriptor_id)?;
        Some(Arc::new(RowDescriptor::from_table_schema(
            &table_descriptor.schema,
        )))
    }

    /// Build a LensContext containing lenses for transforming between schema versions.
    ///
    /// With branch-based versioning, this iterates through all version branches
    /// on the descriptor object and collects lenses from each version's
    /// `lens_from_parent` field.
    ///
    /// Returns an empty context if the table has no migrations.
    pub fn build_lens_context_for_table(&self, table: &str) -> crate::sql::lens::LensContext {
        use crate::sql::lens::LensContext;

        let mut ctx = LensContext::new();

        // Get descriptor object ID
        let descriptor_objects = self.descriptor_objects.read().unwrap();
        let Some(&desc_object_id) = descriptor_objects.get(table) else {
            return ctx;
        };
        drop(descriptor_objects);

        // Get the object to read all branches
        let Some(object) = self.node.get_object(desc_object_id) else {
            return ctx;
        };

        // Collect all versions with their descriptors
        let object_guard = object.read().unwrap();
        let mut versions: Vec<(String, TableDescriptor)> = Vec::new();

        for branch_name in object_guard.branch_names() {
            // Read descriptor from this branch
            if let Ok(Some(data)) = self.node.read(desc_object_id, branch_name)
                && let Ok(descriptor) = TableDescriptor::from_bytes(&data)
            {
                versions.push((branch_name.to_string(), descriptor));
            }
        }
        drop(object_guard);

        // Sort versions by version number (v1, v2, v3, ...)
        versions.sort_by(|(a, _), (b, _)| {
            let a_num = a.strip_prefix('v').and_then(|n| n.parse::<u32>().ok());
            let b_num = b.strip_prefix('v').and_then(|n| n.parse::<u32>().ok());
            match (a_num, b_num) {
                (Some(an), Some(bn)) => an.cmp(&bn),
                _ => a.cmp(b),
            }
        });

        // Register lenses between consecutive versions
        for (version, descriptor) in &versions {
            if let Some(lens) = &descriptor.lens_from_parent
                && let Some(num) = version.strip_prefix('v')
                && let Ok(n) = num.parse::<u32>()
                && n > 1
            {
                let parent_version = format!("v{}", n - 1);
                let parent_id = DescriptorId::new(desc_object_id, &parent_version);
                let this_id = DescriptorId::new(desc_object_id, version);
                ctx.register_lens(parent_id, this_id, lens.clone());
            }
        }

        ctx
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
    /// INSERT - returns row object ID and table rows object ID (for sync)
    Inserted {
        row_id: ObjectId,
        table_rows_id: ObjectId,
    },
    /// UPDATE - returns number of rows affected
    Updated(usize),
    /// DELETE - returns number of rows affected
    Deleted(usize),
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
    /// Migration error.
    Migration(MigrationError),
}

/// Errors that can occur during schema migration.
#[derive(Debug, Clone)]
pub enum MigrationError {
    /// Lens transformation failed for a row.
    LensError { row_id: ObjectId, error: LensError },
    /// Row was incompatible with the new schema.
    IncompatibleRow { row_id: ObjectId, reason: String },
    /// Catalog error during migration.
    CatalogError(String),
    /// Migration was partially completed.
    PartialFailure {
        migrated_count: usize,
        failed_count: usize,
        first_error: String,
    },
}

impl std::fmt::Display for MigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationError::LensError { row_id, error } => {
                write!(f, "lens error for row {}: {}", row_id, error)
            }
            MigrationError::IncompatibleRow { row_id, reason } => {
                write!(f, "row {} incompatible with new schema: {}", row_id, reason)
            }
            MigrationError::CatalogError(msg) => write!(f, "catalog error: {}", msg),
            MigrationError::PartialFailure {
                migrated_count,
                failed_count,
                first_error,
            } => {
                write!(
                    f,
                    "migration partially failed: {} migrated, {} failed, first error: {}",
                    migrated_count, failed_count, first_error
                )
            }
        }
    }
}

impl std::error::Error for MigrationError {}

/// Result of a successful migration.
#[derive(Debug, Clone)]
pub struct MigrationResult {
    /// The new descriptor ID.
    pub new_descriptor_id: DescriptorId,
    /// The generated lens.
    pub lens: Lens,
    /// Number of rows successfully migrated.
    pub migrated_count: usize,
    /// Number of rows that became invisible (incompatible with new schema).
    pub invisible_count: usize,
    /// Warnings from lens generation.
    pub warnings: Vec<LensWarning>,
}

impl From<MigrationError> for DatabaseError {
    fn from(e: MigrationError) -> Self {
        DatabaseError::Migration(e)
    }
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
            DatabaseError::Migration(e) => write!(f, "migration error: {}", e),
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
    fn get_row(&self, table: &str, id: ObjectId) -> Option<(ObjectId, OwnedRow)> {
        self.state.get_row(table, id)
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
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new(env: Arc<dyn Environment>) -> Self {
        let state = Arc::new(DatabaseState::new(env));
        DatabaseState::setup_sync_callback(Arc::clone(&state));
        Database { state }
    }

    /// Create a new in-memory database (for testing).
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn in_memory() -> Self {
        let state = Arc::new(DatabaseState::in_memory());
        DatabaseState::setup_sync_callback(Arc::clone(&state));
        Database { state }
    }

    /// Create an in-memory database with a specific catalog ID.
    /// All clients using the same catalog ID will share schema definitions
    /// when synced, allowing INSERT/SELECT to work across clients.
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn in_memory_with_catalog(catalog_id: ObjectId) -> Self {
        let state = Arc::new(DatabaseState::in_memory_with_catalog(catalog_id));
        DatabaseState::setup_sync_callback(Arc::clone(&state));
        Database { state }
    }

    /// Create an in-memory database as a replica waiting for catalog sync.
    ///
    /// Unlike `in_memory_with_catalog`, this does NOT write an initial empty catalog.
    /// The replica expects to receive the catalog via sync from an upstream server.
    /// Use `has_catalog()` to check if the catalog has arrived, then `reload_catalog()`.
    pub fn in_memory_replica(catalog_id: ObjectId) -> Self {
        let state = Arc::new(DatabaseState::in_memory_replica(catalog_id));
        DatabaseState::setup_sync_callback(Arc::clone(&state));
        Database { state }
    }

    /// Check if the catalog has been synced (has readable content).
    ///
    /// Returns true if the catalog object has exactly one tip and is readable.
    /// Use this to check if a replica has received its catalog from upstream.
    pub fn has_catalog(&self) -> bool {
        self.state.has_catalog()
    }

    /// Consume the Database and return the underlying Arc<DatabaseState>.
    ///
    /// This is useful when you need to wrap the DatabaseState in a SyncedNode.
    pub fn into_state(self) -> Arc<DatabaseState> {
        self.state
    }

    /// Create a Database from an existing Arc<DatabaseState>.
    ///
    /// This is useful when you have a SyncedNode and want to perform SQL operations.
    pub fn from_state(state: Arc<DatabaseState>) -> Self {
        Database { state }
    }

    /// Reload the catalog from storage, picking up any synced schema changes.
    ///
    /// Call this after schema objects (catalog, descriptors) have been synced
    /// from another node to refresh the in-memory schema cache.
    pub fn reload_catalog(&self) -> Result<(), DatabaseError> {
        self.state.reload_catalog()
    }

    /// Restore database from an existing environment with a known catalog object ID.
    ///
    /// This method loads the catalog and all table descriptors from the environment,
    /// restoring the database to its previous state.
    ///
    /// This is async because loading objects from Environment may involve reading from
    /// IndexedDB or other async storage backends.
    #[allow(clippy::arc_with_non_send_sync)]
    pub async fn from_env(
        env: Arc<dyn Environment>,
        catalog_object_id: ObjectId,
    ) -> Result<Self, DatabaseError> {
        let node = Arc::new(LocalNode::new(env));

        // Load catalog object from Environment
        node.load_object(catalog_object_id, "catalog", "main")
            .await
            .ok_or_else(|| {
                DatabaseError::Storage("catalog not found in environment".to_string())
            })?;

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
        for table_name in catalog.tables.keys() {
            // Derive the ObjectId where the descriptor is stored (deterministic from table name)
            let descriptor_key = format!("descriptor:{}", table_name);
            let descriptor_object_id = crate::ObjectId::from_key(&descriptor_key);

            // Load descriptor object from Environment
            node.load_object(descriptor_object_id, descriptor_key, "main")
                .await
                .ok_or_else(|| {
                    DatabaseError::Storage(format!(
                        "descriptor for {} not found in env",
                        table_name
                    ))
                })?;

            // Read descriptor content
            let descriptor_bytes = node
                .read(descriptor_object_id, "main")
                .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?
                .ok_or_else(|| {
                    DatabaseError::Storage(format!("descriptor for {} not found", table_name))
                })?;

            let descriptor = TableDescriptor::from_bytes(&descriptor_bytes)
                .map_err(|e| DatabaseError::Storage(format!("descriptor parse error: {}", e)))?;

            // Load schema object
            node.load_object(
                descriptor.schema_object_id,
                format!("schema:{}", table_name),
                "main",
            )
            .await;

            // Load rows object
            node.load_object(
                descriptor.rows_object_id,
                format!("rows:{}", table_name),
                "main",
            )
            .await;

            // Load index objects
            for (col_name, index_id) in &descriptor.index_object_ids {
                node.load_object(
                    *index_id,
                    format!("index:{}:{}", table_name, col_name),
                    "main",
                )
                .await;
                let key = IndexKey::new(table_name, col_name);
                index_objects.insert(key, *index_id);
            }

            // Restore table metadata
            tables.insert(table_name.clone(), descriptor.schema_object_id);
            schemas.insert(descriptor.schema_object_id, descriptor.schema.clone());
            table_rows_objects.insert(table_name.clone(), descriptor.rows_object_id);
            descriptor_objects.insert(table_name.clone(), descriptor_object_id);

            // Restore policies
            if !descriptor.policies.is_empty() {
                policies.insert(table_name.clone(), descriptor.policies.clone());
            }

            // Restore row_table mapping by reading table_rows
            let rows_bytes = node
                .read(descriptor.rows_object_id, "main")
                .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

            if let Some(bytes) = rows_bytes {
                let table_rows = TableRows::from_bytes(&bytes).map_err(|e| {
                    DatabaseError::Storage(format!("table_rows parse error: {}", e))
                })?;
                for row_id in table_rows.iter() {
                    // Load row object
                    node.load_object(row_id, format!("row:{}:{}", table_name, row_id), "main")
                        .await;
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

        let state = Arc::new(state);
        DatabaseState::setup_sync_callback(Arc::clone(&state));
        Ok(Database { state })
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

        // Create object for schema with deterministic ID (for multi-client sync)
        let schema_key = format!("schema:{}", schema.name);
        let schema_id = crate::ObjectId::from_key(&schema_key);
        self.state.node.ensure_object(schema_id, &schema_key);

        // Serialize and store schema
        let schema_bytes = schema.to_bytes();
        self.state
            .node
            .write(schema_id, "main", &schema_bytes, "system", timestamp_now())
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

        // Create table rows object with deterministic ID
        // NOTE: table_rows is node_private - each node tracks its own set of known rows
        let rows_key = format!("rows:{}", schema.name);
        let rows_id = crate::ObjectId::from_key(&rows_key);
        let mut rows_meta = std::collections::BTreeMap::new();
        rows_meta.insert("node_private".to_string(), "true".to_string());
        self.state
            .node
            .ensure_object_with_meta(rows_id, &rows_key, rows_meta);
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

        // Create index objects for Ref columns with deterministic IDs
        // NOTE: index objects are node_private - each node maintains its own indexes
        let mut index_object_ids: HashMap<String, ObjectId> = HashMap::new();
        for col in &schema.columns {
            if matches!(col.ty, ColumnType::Ref(_)) {
                let key = IndexKey::new(&schema.name, &col.name);
                let index_key = format!("index:{}:{}", schema.name, col.name);
                let index_id = crate::ObjectId::from_key(&index_key);
                let mut index_meta = std::collections::BTreeMap::new();
                index_meta.insert("node_private".to_string(), "true".to_string());
                self.state
                    .node
                    .ensure_object_with_meta(index_id, &index_key, index_meta);

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

        // Create table descriptor object with deterministic ID
        // Each schema version is a branch on this object (v1, v2, etc.)
        let descriptor_key = format!("descriptor:{}", schema.name);
        let descriptor_object_id = crate::ObjectId::from_key(&descriptor_key);
        let desc_id = DescriptorId::new_v1(descriptor_object_id);

        self.state
            .node
            .ensure_object(descriptor_object_id, &descriptor_key);

        let descriptor = TableDescriptor {
            schema: schema.clone(),
            policies: TablePolicies::default(),
            lens_from_parent: None, // Initial schema (v1) has no parent
            rows_object_id: rows_id,
            schema_object_id: schema_id,
            index_object_ids,
        };

        // Write initial schema to "main" branch first (required to create initial commit)
        let initial_commit = self
            .state
            .node
            .write(
                descriptor_object_id,
                "main",
                &descriptor.to_bytes(),
                "system",
                timestamp_now(),
            )
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

        // Create "v1" branch from main at the initial commit
        {
            let object = self
                .state
                .node
                .get_object(descriptor_object_id)
                .ok_or_else(|| DatabaseError::Storage("descriptor object not found".to_string()))?;
            let mut obj = object.write().unwrap();
            obj.create_branch("v1", "main", &initial_commit)
                .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;
        }

        self.state
            .descriptor_objects
            .write()
            .unwrap()
            .insert(schema.name.clone(), descriptor_object_id);

        // Update catalog with the same descriptor ID
        self.update_catalog_add_table(&schema.name, desc_id)?;

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
        descriptor_id: DescriptorId,
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
            let table_policies = policies.entry(table_name.clone()).or_default();

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
        descriptor.policies = policies.get(table_name).cloned().unwrap_or_default();

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

    /// Insert a row into a table.
    ///
    /// The row should be built using `RowBuilder` with a descriptor from the table schema:
    /// ```ignore
    /// let schema = db.get_table("users")?;
    /// let desc = Arc::new(RowDescriptor::from_table_schema(&schema));
    /// let row = RowBuilder::new(desc)
    ///     .set_string_by_name("name", "Alice")
    ///     .set_i32_by_name("age", 30)
    ///     .build();
    /// db.insert_row("users", row)?;
    /// ```
    pub fn insert_row(&self, table: &str, row: OwnedRow) -> Result<ObjectId, DatabaseError> {
        let (row_id, _table_rows_id) = self.insert_row_returning_both(table, row)?;
        Ok(row_id)
    }

    /// Insert a row into a table, returning both row_id and table_rows_id.
    /// Used internally by execute() for sync purposes.
    fn insert_row_returning_both(
        &self,
        table: &str,
        row: OwnedRow,
    ) -> Result<(ObjectId, ObjectId), DatabaseError> {
        let schema = self
            .get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

        // Validate references: check that referenced rows exist
        {
            let row_table = self.state.row_table.read().unwrap();
            for col in schema.columns.iter() {
                if let ColumnType::Ref(target_table) = &col.ty {
                    // Get the value by schema column name
                    if let Some(RowValue::Ref(target_id)) = row.get_by_name(&col.name) {
                        // Check target row exists
                        if !row_table.contains_key(&target_id) {
                            return Err(DatabaseError::InvalidReference {
                                column: col.name.clone(),
                                target_table: target_table.clone(),
                                target_id,
                            });
                        }
                        // Also verify target row is in the correct table
                        if row_table.get(&target_id) != Some(target_table) {
                            return Err(DatabaseError::InvalidReference {
                                column: col.name.clone(),
                                target_table: target_table.clone(),
                                target_id,
                            });
                        }
                    }
                    // Null refs are ok if column is nullable
                }
            }
        }

        // Create object for row (generate id first so we can inject it)
        let row_id =
            self.state
                .node
                .create_object(format!("row:{}:{}", table, generate_object_id()));

        // Set object metadata to identify this as a row object
        // - `type: row` identifies this as a row object
        // - `table: name` identifies which table (used by database callbacks)
        // The sync layer passes through metadata without interpreting it
        {
            let mut meta = std::collections::BTreeMap::new();
            meta.insert("type".to_string(), "row".to_string());
            meta.insert("table".to_string(), table.to_string());
            if let Some(obj) = self.state.node.get_object(row_id)
                && let Ok(mut obj_write) = obj.write()
            {
                obj_write.set_meta(meta);
            }
        }

        // Inject the id into the row buffer at column 0
        let row_with_id = row.with_id(row_id);

        // Create descriptor for per-column change tracking
        let descriptor = RowDescriptor::from_table_schema(&schema);

        // Store row data with per-column change tracking
        // This enables proper per-column LWW merge for concurrent writes
        self.state
            .node
            .write_with_tracking(
                row_id,
                "main",
                &row_with_id.buffer,
                "system",
                timestamp_now(),
                &descriptor,
            )
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
        for col in schema.columns.iter() {
            if matches!(col.ty, ColumnType::Ref(_))
                && let Some(RowValue::Ref(target_id)) = row_with_id.get_by_name(&col.name)
            {
                let key = IndexKey::new(table, &col.name);
                if self.state.index_objects.read().unwrap().contains_key(&key) {
                    let mut index = self.read_index(&key)?;
                    index.add(target_id, row_id);
                    self.write_index(&key, &index)?;
                }
            }
        }

        // Notify query graphs of the change
        self.notify_object_changed_internal(table, row_id);

        // Get the table rows object ID for sync purposes
        let table_rows_id = self
            .table_rows_object_id(table)
            .expect("table_rows_object_id should exist for table");

        Ok((row_id, table_rows_id))
    }

    /// Insert a row using a builder function.
    ///
    /// This is a convenience method that creates the descriptor and builds the row
    /// in one step. The builder function receives a `RowBuilder` pre-configured
    /// with the table's schema.
    ///
    /// # Example
    /// ```ignore
    /// db.insert_with("users", |b| b
    ///     .set_string_by_name("name", "Alice")
    ///     .set_i32_by_name("age", 30)
    ///     .build()
    /// )?;
    /// ```
    pub fn insert_with<F>(&self, table: &str, f: F) -> Result<ObjectId, DatabaseError>
    where
        F: FnOnce(RowBuilder) -> OwnedRow,
    {
        let schema = self
            .get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;
        let descriptor = Arc::new(RowDescriptor::from_table_schema(&schema));
        let row = f(RowBuilder::new(descriptor));
        self.insert_row(table, row)
    }

    /// Update a row using a builder pattern.
    ///
    /// The closure receives a RowBuilder initialized with the existing row's data,
    /// allowing you to modify specific columns while preserving others.
    ///
    /// Returns false if the row doesn't exist.
    ///
    /// # Example
    /// ```ignore
    /// db.update_with("users", id, |b| {
    ///     b.set_string_by_name("name", "Bob")
    ///      .build()
    /// })?;
    /// ```
    pub fn update_with<F>(&self, table: &str, id: ObjectId, f: F) -> Result<bool, DatabaseError>
    where
        F: FnOnce(RowBuilder) -> OwnedRow,
    {
        // Get existing row
        let (_, existing) = match self.get(table, id)? {
            Some(row) => row,
            None => return Ok(false),
        };
        // Create builder from existing row and let user modify
        let builder = RowBuilder::from_owned_row(&existing);
        let new_row = f(builder);
        self.update_row(table, id, new_row)
    }

    /// Get a row by ID in buffer format.
    pub fn get(
        &self,
        table: &str,
        id: ObjectId,
    ) -> Result<Option<(ObjectId, OwnedRow)>, DatabaseError> {
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

        // Create OwnedRow directly from buffer
        let descriptor = Arc::new(RowDescriptor::from_table_schema(&schema));
        let owned = OwnedRow::new(descriptor, data);

        Ok(Some((id, owned)))
    }

    /// Register a synced row from another client.
    ///
    /// When we receive a row via sync, we need to:
    /// 1. Look up the descriptor to find the table name
    /// 2. Add the row to local table_rows
    /// 3. Update the row_table mapping
    pub fn register_synced_row(
        &self,
        row_id: ObjectId,
        descriptor_id_str: &str,
    ) -> Result<(), DatabaseError> {
        // Parse descriptor ID
        let descriptor_id: ObjectId = descriptor_id_str.parse().map_err(|_| {
            DatabaseError::Storage(format!("Invalid descriptor ID: {}", descriptor_id_str))
        })?;

        // Find the table name by looking up which table has this descriptor ID
        let table_name = {
            let descriptor_objects = self.state.descriptor_objects.read().unwrap();
            descriptor_objects
                .iter()
                .find(|(_, id)| **id == descriptor_id)
                .map(|(name, _)| name.clone())
        };

        let table_name = table_name.ok_or_else(|| {
            DatabaseError::Storage(format!(
                "Descriptor {} not found in local catalog",
                descriptor_id_str
            ))
        })?;

        // Add to row_table mapping
        self.state
            .row_table
            .write()
            .unwrap()
            .insert(row_id, table_name.clone());

        // Add to table_rows object
        let mut table_rows = self.read_table_rows(&table_name)?;
        let is_new = !table_rows.contains(row_id);
        if is_new {
            table_rows.add(row_id);
            self.write_table_rows(&table_name, &table_rows)?;
        }

        // Notify query graphs about the row change
        self.notify_object_changed_internal(&table_name, row_id);

        Ok(())
    }

    /// Register a row received via sync, using the table name directly.
    ///
    /// This is the preferred method for sync registration as it doesn't require
    /// descriptor IDs to match between clients. The table name is sent in object
    /// metadata when rows are pushed.
    pub fn register_synced_row_by_table(
        &self,
        row_id: ObjectId,
        table_name: &str,
    ) -> Result<(), DatabaseError> {
        // Verify table exists locally
        if self.get_table(table_name).is_none() {
            return Err(DatabaseError::TableNotFound(table_name.to_string()));
        }

        // Add to row_table mapping
        self.state
            .row_table
            .write()
            .unwrap()
            .insert(row_id, table_name.to_string());

        // Add to table_rows object
        let mut table_rows = self.read_table_rows(table_name)?;
        let is_new = !table_rows.contains(row_id);
        if is_new {
            table_rows.add(row_id);
            self.write_table_rows(table_name, &table_rows)?;
        }

        // Notify query graphs about the row change
        self.notify_object_changed_internal(table_name, row_id);

        Ok(())
    }

    /// Notify query graphs about an update to a row we already know about.
    ///
    /// This is used when we receive synced commits for a row that was already
    /// registered (e.g., an update to an existing row). We don't need the
    /// descriptor since we already have the row in our row_table mapping.
    pub fn notify_synced_row_update(&self, row_id: ObjectId) -> Result<bool, DatabaseError> {
        // Look up the table from row_table
        let table_name = {
            let row_table = self.state.row_table.read().unwrap();
            row_table.get(&row_id).cloned()
        };

        let table_name = match table_name {
            Some(t) => t,
            None => return Ok(false), // Row not known to us
        };

        // Notify query graphs about the update
        self.notify_object_changed_internal(&table_name, row_id);

        Ok(true)
    }

    /// Internal helper to notify the registry after an object change.
    ///
    /// This is the unified notification method that should be called after any
    /// write operation (insert, update, delete, sync) modifies an object.
    fn notify_object_changed_internal(&self, table: &str, object_id: ObjectId) {
        if let Some(obj) = self.state.node.get_object(object_id)
            && let Ok(obj_guard) = obj.read()
        {
            self.state.graph_registry.notify_object_changed(
                table,
                object_id,
                &obj_guard,
                &self.state,
            );
        }
    }

    /// Apply synced commits to an object and notify branch-aware queries.
    ///
    /// This should be used by the sync layer when receiving commits for objects
    /// that have branch-aware queries registered. It applies the commits to the
    /// object's branch and notifies the GraphRegistry.
    ///
    /// # Arguments
    ///
    /// * `table` - The table the object belongs to
    /// * `object_id` - The object to apply commits to
    /// * `branch` - The branch to apply commits to
    /// * `commits` - The commits to apply
    ///
    /// # Returns
    ///
    /// The new frontier after applying commits.
    pub fn apply_synced_commits(
        &self,
        table: &str,
        object_id: ObjectId,
        branch: &str,
        commits: Vec<crate::commit::Commit>,
    ) -> Vec<crate::commit::CommitId> {
        // Apply commits to the object's branch via LocalNode
        let frontier = self.state.node.apply_commits(object_id, branch, commits);

        // Rebuild column change metadata for per-column LWW merge
        // The low-level apply_commits doesn't track column changes, so we need to rebuild
        // the metadata after syncing commits.
        if let Some(schema) = self.get_table(table) {
            let descriptor = RowDescriptor::from_table_schema(&schema);
            if let Some(obj_lock) = self.state.node.get_object(object_id)
                && let Ok(obj) = obj_lock.read()
                && let Some(branch_ref) = obj.branch_ref(branch)
                && let Ok(mut branch_guard) = branch_ref.write()
            {
                branch_guard.rebuild_column_changes(&descriptor);
            }
        }

        // Notify queries about the change
        self.notify_object_changed_internal(table, object_id);

        frontier
    }

    /// Update a row by ID.
    ///
    /// This replaces the entire row with the new values. Build the row using `RowBuilder`:
    /// ```ignore
    /// let schema = db.get_table("users")?;
    /// let desc = Arc::new(RowDescriptor::from_table_schema(&schema));
    /// let row = RowBuilder::new(desc)
    ///     .set_string_by_name("name", "Bob")
    ///     .set_i32_by_name("age", 35)
    ///     .build();
    /// db.update_row("users", row_id, row)?;
    /// ```
    pub fn update_row(
        &self,
        table: &str,
        id: ObjectId,
        new_row: OwnedRow,
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

        // Create descriptor for row operations and per-column change tracking
        let descriptor = RowDescriptor::from_table_schema(&schema);
        let descriptor_arc = Arc::new(descriptor.clone());

        // Read current row data for index updates (directly as OwnedRow)
        let old_row = match self.state.node.read(id, "main") {
            Ok(Some(data)) => OwnedRow::new(descriptor_arc, data),
            Ok(None) => return Ok(false),
            Err(e) => return Err(DatabaseError::Storage(format!("{:?}", e))),
        };

        // Validate new references: check that referenced rows exist
        {
            let row_table = self.state.row_table.read().unwrap();
            for col in schema.columns.iter() {
                if let ColumnType::Ref(target_table) = &col.ty
                    && let Some(RowValue::Ref(target_id)) = new_row.get_by_name(&col.name)
                {
                    if !row_table.contains_key(&target_id) {
                        return Err(DatabaseError::InvalidReference {
                            column: col.name.clone(),
                            target_table: target_table.clone(),
                            target_id,
                        });
                    }
                    if row_table.get(&target_id) != Some(target_table) {
                        return Err(DatabaseError::InvalidReference {
                            column: col.name.clone(),
                            target_table: target_table.clone(),
                            target_id,
                        });
                    }
                }
            }
        }

        // Ensure the id column is set (preserve the original id)
        let new_row_with_id = new_row.with_id(id);

        // Write updated row with per-column change tracking
        // This enables proper per-column LWW merge for concurrent writes
        self.state
            .node
            .write_with_tracking(
                id,
                "main",
                &new_row_with_id.buffer,
                "system",
                timestamp_now(),
                &descriptor,
            )
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;

        // Update indexes for changed Ref columns
        for col in schema.columns.iter() {
            if matches!(col.ty, ColumnType::Ref(_)) {
                let old_ref = match old_row.get_by_name(&col.name) {
                    Some(RowValue::Ref(id)) => Some(id),
                    _ => None,
                };
                let new_ref = match new_row_with_id.get_by_name(&col.name) {
                    Some(RowValue::Ref(id)) => Some(id),
                    _ => None,
                };

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
        self.notify_object_changed_internal(table, id);

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
            let descriptor = Arc::new(RowDescriptor::from_table_schema(&schema));
            let row = OwnedRow::new(descriptor, data);
            for col in schema.columns.iter() {
                if matches!(col.ty, ColumnType::Ref(_))
                    && let Some(RowValue::Ref(target_id)) = row.get_by_name(&col.name)
                {
                    let key = IndexKey::new(table, &col.name);
                    if self.state.index_objects.read().unwrap().contains_key(&key) {
                        let mut index = self.read_index(&key)?;
                        index.remove(target_id, id);
                        self.write_index(&key, &index)?;
                    }
                }
            }
        }

        // Create delete metadata marker
        let mut meta = std::collections::BTreeMap::new();
        meta.insert("deleted".to_string(), "true".to_string());

        // Write soft delete commit with metadata marker
        let commit_id = self
            .state
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
        self.notify_object_changed_internal(table, id);

        Ok(true)
    }

    // ========== Policy-Checked Write Operations ==========

    /// Delete a row, checking DELETE policy for the given viewer.
    pub fn delete_as(
        &self,
        table: &str,
        id: ObjectId,
        viewer: ObjectId,
    ) -> Result<bool, DatabaseError> {
        use crate::sql::policy::{PolicyConfig, PolicyEvaluator, PolicyResult};

        // Get the existing row
        let (row_id, row) = match self.get(table, id)? {
            Some(row) => row,
            None => return Ok(false),
        };

        // Check DELETE policy
        let config = PolicyConfig::default();
        let mut evaluator = PolicyEvaluator::new(self, self, viewer, config);
        let result = evaluator.check_delete(table, row_id, &row);

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

    /// Insert a row, checking INSERT policy for the given viewer.
    ///
    /// This is the buffer-based version that takes an OwnedRow directly.
    pub fn insert_row_as(
        &self,
        table: &str,
        row: OwnedRow,
        viewer: ObjectId,
    ) -> Result<ObjectId, DatabaseError> {
        use crate::sql::policy::{PolicyConfig, PolicyEvaluator, PolicyResult};

        let temp_id = ObjectId::default();

        // Check INSERT policy
        let config = PolicyConfig::default();
        let mut evaluator = PolicyEvaluator::new(self, self, viewer, config);
        let result = evaluator.check_insert(table, temp_id, &row);

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
        self.insert_row(table, row)
    }

    /// Insert a row using builder pattern, checking INSERT policy.
    pub fn insert_with_as<F>(
        &self,
        table: &str,
        f: F,
        viewer: ObjectId,
    ) -> Result<ObjectId, DatabaseError>
    where
        F: FnOnce(RowBuilder) -> OwnedRow,
    {
        let schema = self
            .get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;
        let descriptor = Arc::new(RowDescriptor::from_table_schema(&schema));
        let row = f(RowBuilder::new(descriptor));
        self.insert_row_as(table, row, viewer)
    }

    /// Update a row, checking UPDATE policy for the given viewer.
    ///
    /// This is the buffer-based version that takes an OwnedRow directly.
    pub fn update_row_as(
        &self,
        table: &str,
        id: ObjectId,
        new_row: OwnedRow,
        viewer: ObjectId,
    ) -> Result<bool, DatabaseError> {
        use crate::sql::policy::{PolicyConfig, PolicyEvaluator, PolicyResult};

        // Get the existing row
        let (old_id, old_row) = match self.get(table, id)? {
            Some(row) => row,
            None => return Ok(false),
        };

        // Check UPDATE policy
        let config = PolicyConfig::default();
        let mut evaluator = PolicyEvaluator::new(self, self, viewer, config);
        let result = evaluator.check_update(table, old_id, &old_row, id, &new_row);

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
        self.update_row(table, id, new_row)
    }

    /// Update a row using builder pattern, checking UPDATE policy.
    pub fn update_with_as<F>(
        &self,
        table: &str,
        id: ObjectId,
        f: F,
        viewer: ObjectId,
    ) -> Result<bool, DatabaseError>
    where
        F: FnOnce(RowBuilder) -> OwnedRow,
    {
        // Get existing row
        let (_, existing) = match self.get(table, id)? {
            Some(row) => row,
            None => return Ok(false),
        };
        // Create builder from existing row and let user modify
        let builder = RowBuilder::from_owned_row(&existing);
        let new_row = f(builder);
        self.update_row_as(table, id, new_row, viewer)
    }

    /// Find all rows referencing a target ID via a specific column.
    /// Uses the reverse index for O(1) lookup.
    pub fn find_referencing(
        &self,
        source_table: &str,
        source_column: &str,
        target_id: ObjectId,
    ) -> Result<Vec<(ObjectId, OwnedRow)>, DatabaseError> {
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
            if let Some((id, row)) = self.get(source_table, row_id)? {
                rows.push((id, row));
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
                // Build row using RowBuilder with PredicateValue
                let schema = self
                    .get_table(&ins.table)
                    .ok_or_else(|| DatabaseError::TableNotFound(ins.table.clone()))?;
                let descriptor = Arc::new(RowDescriptor::from_table_schema(&schema));

                if ins.columns.len() != ins.values.len() {
                    return Err(DatabaseError::ColumnMismatch {
                        expected: ins.columns.len(),
                        got: ins.values.len(),
                    });
                }

                let mut builder = RowBuilder::new(descriptor);
                for (col_name, value) in ins.columns.iter().zip(ins.values.iter()) {
                    // Coerce string to Ref if inserting into a Ref column
                    let coerced_value = if let Some(col_def) = schema.column(col_name) {
                        if matches!(col_def.ty, ColumnType::Ref(_)) {
                            if let PredicateValue::String(s) = value {
                                // Parse string as ObjectId
                                match s.parse::<ObjectId>() {
                                    Ok(id) => PredicateValue::Ref(id),
                                    Err(_) => value.clone(),
                                }
                            } else {
                                value.clone()
                            }
                        } else {
                            value.clone()
                        }
                    } else {
                        value.clone()
                    };
                    builder = builder.set_from_predicate_value_by_name(col_name, &coerced_value);
                }
                let row = builder.build();

                let (row_id, table_rows_id) = self.insert_row_returning_both(&ins.table, row)?;
                Ok(ExecuteResult::Inserted {
                    row_id,
                    table_rows_id,
                })
            }
            Statement::Update(upd) => {
                // Verify table exists
                if self.get_table(&upd.table).is_none() {
                    return Err(DatabaseError::TableNotFound(upd.table.clone()));
                }

                // Get all rows and filter by WHERE clause
                let mut rows_to_update = self.state.read_all_rows(&upd.table);
                for cond in &upd.where_clause {
                    let col_name = &cond.column.column;
                    let value = cond.value().ok_or_else(|| {
                        DatabaseError::ColumnNotFound(
                            "column references not supported in UPDATE WHERE".to_string(),
                        )
                    })?;
                    rows_to_update.retain(|(row_id, row)| {
                        // Special case: "id" column is the row's ObjectId
                        if col_name == "id" {
                            let coerced =
                                coerce_predicate_value(value, &ColumnType::Ref("".to_string()));
                            if let PredicateValue::Ref(expected_id) = coerced {
                                return *row_id == expected_id;
                            }
                            return false;
                        }
                        if let Some(v) = row.get_by_name(col_name) {
                            value.matches(&v)
                        } else {
                            false
                        }
                    });
                }

                let count = rows_to_update.len();
                let schema = self
                    .get_table(&upd.table)
                    .ok_or_else(|| DatabaseError::TableNotFound(upd.table.clone()))?;

                for (id, old_row) in rows_to_update {
                    // Build new row by copying existing values and applying updates
                    let descriptor = old_row.descriptor.clone();
                    let mut builder = RowBuilder::new(descriptor.clone());

                    // Copy all existing values
                    for col_idx in 0..descriptor.columns.len() {
                        if let Some(value) = old_row.get(col_idx) {
                            builder = builder.set_from_row_value(col_idx, value);
                        }
                    }

                    // Apply updates
                    for (col_name, value) in &upd.assignments {
                        // Coerce string to Ref if updating a Ref column
                        let coerced_value = if let Some(col_def) = schema.column(col_name) {
                            if matches!(col_def.ty, ColumnType::Ref(_)) {
                                if let PredicateValue::String(s) = value {
                                    match s.parse::<ObjectId>() {
                                        Ok(id) => PredicateValue::Ref(id),
                                        Err(_) => value.clone(),
                                    }
                                } else {
                                    value.clone()
                                }
                            } else {
                                value.clone()
                            }
                        } else {
                            value.clone()
                        };
                        builder =
                            builder.set_from_predicate_value_by_name(col_name, &coerced_value);
                    }

                    let new_row = builder.build();
                    self.update_row(&upd.table, id, new_row)?;
                }

                Ok(ExecuteResult::Updated(count))
            }
            Statement::Delete(del) => {
                // Verify table exists
                if self.get_table(&del.table).is_none() {
                    return Err(DatabaseError::TableNotFound(del.table.clone()));
                }

                // Get all rows and filter by WHERE clause
                let mut rows_to_delete = self.state.read_all_rows(&del.table);
                for cond in &del.where_clause {
                    let col_name = &cond.column.column;
                    let value = cond.value().ok_or_else(|| {
                        DatabaseError::ColumnNotFound(
                            "column references not supported in DELETE WHERE".to_string(),
                        )
                    })?;
                    rows_to_delete.retain(|(row_id, row)| {
                        // Special case: "id" column is the row's ObjectId
                        if col_name == "id" {
                            let coerced =
                                coerce_predicate_value(value, &ColumnType::Ref("".to_string()));
                            if let PredicateValue::Ref(expected_id) = coerced {
                                return *row_id == expected_id;
                            }
                            return false;
                        }
                        if let Some(v) = row.get_by_name(col_name) {
                            value.matches(&v)
                        } else {
                            false
                        }
                    });
                }

                let count = rows_to_delete.len();

                for (id, _) in rows_to_delete {
                    if del.hard {
                        self.delete_hard(&del.table, id)?;
                    } else {
                        self.delete(&del.table, id)?;
                    }
                }

                Ok(ExecuteResult::Deleted(count))
            }
            Statement::Select(_) => Err(DatabaseError::Parse(parser::ParseError {
                message: "SELECT statements should use query() instead of execute()".to_string(),
                position: 0,
            })),
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

    /// Execute a SELECT query and return the results.
    ///
    /// This is a convenience method that creates an incremental query,
    /// gets the current results, and automatically cleans up the query.
    ///
    /// For subscriptions to live updates, use `incremental_query()` instead.
    pub fn query(&self, sql: &str) -> Result<Vec<(ObjectId, OwnedRow)>, DatabaseError> {
        Ok(self.incremental_query(sql)?.rows())
    }

    /// Execute a SELECT query with policy filtering and return the results.
    ///
    /// This is a convenience method that creates an incremental query with
    /// policy filtering, gets the current results, and automatically cleans up.
    ///
    /// For subscriptions to live updates, use `incremental_query_as()` instead.
    pub fn query_as(
        &self,
        sql: &str,
        viewer: ObjectId,
    ) -> Result<Vec<(ObjectId, OwnedRow)>, DatabaseError> {
        Ok(self.incremental_query_as(sql, viewer)?.rows())
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
        if let Some(policy) = select_policy
            && let Some(ref where_expr) = policy.where_clause
            && let Some(inherits_info) = self.extract_inherits(where_expr, table, &schema)?
        {
            if inherits_info.is_self_referential {
                // Self-referential INHERITS: use RecursiveFilter
                return self.build_recursive_filter_graph(select, viewer, &inherits_info, &schema);
            } else {
                // Non-self-referential INHERITS: resolve the full chain
                let mut visited = vec![table.to_string()];
                let chain =
                    self.resolve_inherits_chain(&inherits_info, table, viewer, &mut visited)?;

                if chain.hops.len() == 1 {
                    // Single hop: use existing simple JOIN graph
                    return self.build_inherits_join_graph(select, viewer, &inherits_info);
                } else {
                    // Multi-hop chain: use chain JOIN graph
                    return self.build_chain_join_graph(select, viewer, &chain, &schema);
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
        let limited = builder.limit_offset(after_policy, select.limit, select.offset.unwrap_or(0));

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

        if let Some(policy) = target_select
            && let Some(ref where_expr) = policy.where_clause
        {
            // Check if target policy also has INHERITS
            if let Some(nested_inherits) =
                self.extract_inherits(where_expr, &initial_inherits.target_table, &target_schema)?
            {
                if nested_inherits.is_self_referential {
                    // Self-referential in the chain - not supported yet
                    return Err(DatabaseError::Parse(parser::ParseError {
                        message: "Self-referential INHERITS in chain not yet supported".to_string(),
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
        let mut builder = QueryGraphBuilder::new(left_table, left_schema.clone());

        let join_node = builder.join(
            &inherits.target_table,
            right_schema.clone(),
            &inherits.ref_column,
        );

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

        // Apply the policy predicates:
        // - Source table's base_predicate (the OR sibling of INHERITS): e.g., folders.owner_id = @viewer
        // - Target table's policy predicate (the flattened INHERITS): e.g., workspaces.owner_id = @viewer
        // These are ORed together: a row matches if EITHER condition is satisfied.
        let after_policy = {
            let mut or_predicates: Vec<Predicate> = Vec::new();

            // Add source table's base predicate (if any)
            if let Some(ref base_expr) = inherits.base_predicate {
                let base_pred =
                    self.policy_expr_to_predicate_qualified(base_expr, viewer, left_table)?;
                or_predicates.push(base_pred);
            }

            // Add target table's policy predicate (if any)
            if let Some(ref target_expr) = inherits.target_predicate {
                let target_pred = self.policy_expr_to_predicate_qualified(
                    target_expr,
                    viewer,
                    &inherits.target_table,
                )?;
                or_predicates.push(target_pred);
            }

            if or_predicates.is_empty() {
                // No policy constraints = allow all
                after_additional
            } else if or_predicates.len() == 1 {
                builder.filter(after_additional, or_predicates.remove(0))
            } else {
                let combined = Predicate::Or(or_predicates);
                builder.filter(after_additional, combined)
            }
        };

        // Apply LIMIT/OFFSET if specified
        let limited = builder.limit_offset(after_policy, select.limit, select.offset.unwrap_or(0));

        // Add projection to unqualify column names
        // (SELECT * FROM documents should return "title", not "documents.title")
        let projected = builder.projection_unqualify(limited, left_table, &left_schema);

        Ok(builder.output(projected, GraphId(0)))
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
        let mut builder = QueryGraphBuilder::new(left_table, source_schema.clone());

        // Pre-add schemas for all subsequent hops
        for hop in chain.hops.iter().skip(1) {
            let target_schema = self
                .get_table(&hop.target_table)
                .ok_or_else(|| DatabaseError::TableNotFound(hop.target_table.clone()))?;
            builder.add_schema(&hop.target_table, target_schema);
        }

        // Build first join
        let mut current_node = builder.join(
            &first_hop.target_table,
            first_target_schema.clone(),
            &first_hop.ref_column,
        );

        // Apply user's WHERE clause after first join
        // The predicate must use qualified column names since we're in a JOIN context
        if !select.where_clause.is_empty() {
            let predicate = self.build_predicate(&select.where_clause, source_schema)?;
            let qualified_pred = predicate.qualify(left_table);
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
        let limited = builder.limit_offset(current_node, select.limit, select.offset.unwrap_or(0));

        // Add projection to unqualify column names
        // (SELECT * FROM documents should return "title", not "documents.title")
        let projected = builder.projection_unqualify(limited, left_table, source_schema);

        Ok(builder.output(projected, GraphId(0)))
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
        let limited = builder.limit_offset(recursive, select.limit, select.offset.unwrap_or(0));

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
                .negate()),
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
    ) -> Result<(String, PredicateValue), DatabaseError> {
        match (left, right) {
            (PolicyValue::Column(col), PolicyValue::Viewer) => Ok((
                format!("{}.{}", table_prefix, col),
                PredicateValue::Ref(viewer),
            )),
            (PolicyValue::Viewer, PolicyValue::Column(col)) => Ok((
                format!("{}.{}", table_prefix, col),
                PredicateValue::Ref(viewer),
            )),
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
                Ok(pred.negate())
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
    ) -> Result<(String, PredicateValue), DatabaseError> {
        match (left, right) {
            // column = @viewer
            (PolicyValue::Column(col), PolicyValue::Viewer) => {
                Ok((col.clone(), PredicateValue::Ref(viewer)))
            }
            // @viewer = column
            (PolicyValue::Viewer, PolicyValue::Column(col)) => {
                Ok((col.clone(), PredicateValue::Ref(viewer)))
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
        let limited = builder.limit_offset(filtered, select.limit, select.offset.unwrap_or(0));

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
                builder,
                current,
                expr,
                outer_table,
                outer_alias,
                -1,
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
                    builder,
                    input,
                    inner,
                    outer_table,
                    outer_alias,
                    column_index,
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
            if let Some(col) = inner_schema.column(col_name)
                && matches!(&col.ty, ColumnType::Ref(t) if t == target_table)
            {
                return Ok(col_name.clone());
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
            if let Some(col) = inner_schema.column(col_name)
                && matches!(&col.ty, ColumnType::Ref(t) if t == target_table)
            {
                return Ok(col_name.clone());
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
        // The QueryGraphBuilder expects the "left" table to have the Ref column
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
        let (
            graph_left_table,
            graph_left_schema,
            graph_right_table,
            graph_right_schema,
            ref_column,
        ) = match &first_join_direction {
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
        let mut builder = QueryGraphBuilder::new(graph_left_table, graph_left_schema.clone());

        // Start with first join node
        let mut current_node =
            builder.join(graph_right_table, graph_right_schema.clone(), &ref_column);

        // Track all tables involved for predicate building
        // Store (table_name, alias, schema)
        let from_alias = select.from.alias.as_deref();
        let mut all_tables: Vec<(&str, Option<&str>, TableSchema)> = vec![
            (sql_left_table.as_str(), from_alias, sql_left_schema.clone()),
            (
                sql_first_right_table.as_str(),
                None,
                sql_first_right_schema.clone(),
            ),
        ];

        // Track reverse-joined tables to handle their WHERE conditions specially
        let mut reverse_joined_tables: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        // Process additional JOINs (chain joins)
        for join in select.from.joins.iter().skip(1) {
            let target_table = &join.table;
            let target_schema = self
                .get_table(target_table)
                .ok_or_else(|| DatabaseError::TableNotFound(target_table.clone()))?;

            // Add schema so chain_join can find it
            builder.add_schema(target_table.clone(), target_schema.clone());

            // Determine which table in the chain has the ref column
            let join_info =
                self.find_chain_join_info(&join.on, &all_tables, target_table, &target_schema)?;

            current_node = match join_info {
                ChainJoinInfo::Forward {
                    source_table,
                    ref_column,
                } => {
                    builder.chain_join(current_node, source_table, ref_column, target_table.clone())
                }
                ChainJoinInfo::Reverse {
                    existing_table,
                    ref_column,
                } => {
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
            let remaining_conditions: Vec<_> = select
                .where_clause
                .iter()
                .filter(|cond| !self.condition_on_table(cond, &reverse_joined_tables))
                .cloned()
                .collect();

            if remaining_conditions.is_empty() {
                current_node
            } else {
                // Build predicate for remaining conditions only (with alias support)
                let predicate = self
                    .build_multi_join_predicate_with_aliases(&remaining_conditions, &all_tables)?;
                builder.filter(current_node, predicate)
            }
        };

        // Apply LIMIT/OFFSET if specified
        let limited = builder.limit_offset(filtered, select.limit, select.offset.unwrap_or(0));

        // Process ARRAY subqueries in projection (for reverse refs with includes)
        let outer_table = sql_left_table;
        let outer_alias = select.from.alias.as_deref();
        let with_arrays = if let Projection::Expressions(exprs) = &select.projection {
            self.add_join_array_aggregates(&mut builder, limited, exprs, outer_table, outer_alias)?
        } else {
            limited
        };

        // For reverse JOINs, add projection to output only the SQL FROM table's columns.
        // The SQL is `SELECT Issues.* FROM Issues JOIN IssueAssignees`, but we swapped the
        // tables for the graph builder (because IssueAssignees has the Ref). We project
        // back to Issues (the original SQL left table, now graph_right_table).
        let final_node = if matches!(first_join_direction, JoinDirection::RightToLeft(_)) {
            builder.projection_select_table(with_arrays, graph_right_table, &sql_left_schema)
        } else {
            with_arrays
        };

        // Create output node
        Ok(builder.output(final_node, GraphId(0))) // ID will be assigned by registry
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
                if let Some(col) = schema.column(col_name)
                    && matches!(&col.ty, ColumnType::Ref(t) if t == target_table)
                {
                    return Ok(ChainJoinInfo::Forward {
                        source_table: table_name.to_string(),
                        ref_column: col_name.clone(),
                    });
                }
            }

            // Check reverse: target.id = existing.col (forward ref, swapped)
            let left_is_target = on.left.table.as_deref() == Some(target_table);
            let right_is_existing = matches_table(on.right.table.as_deref(), table_name, *alias);

            if left_is_target && right_is_existing && on.left.column == "id" {
                let col_name = &on.right.column;
                if let Some(col) = schema.column(col_name)
                    && matches!(&col.ty, ColumnType::Ref(t) if t == target_table)
                {
                    return Ok(ChainJoinInfo::Forward {
                        source_table: table_name.to_string(),
                        ref_column: col_name.clone(),
                    });
                }
            }
        }

        // Check for reverse ref: target.col = existing.id (target has ref to existing)
        for (table_name, alias, _schema) in existing_tables {
            let left_is_target = on.left.table.as_deref() == Some(target_table);
            let right_is_existing = matches_table(on.right.table.as_deref(), table_name, *alias);

            if left_is_target && right_is_existing && on.right.column == "id" {
                let col_name = &on.left.column;
                if let Some(col) = target_schema.column(col_name)
                    && matches!(&col.ty, ColumnType::Ref(t) if t == *table_name)
                {
                    // This is a reverse join - the target table has the ref
                    return Ok(ChainJoinInfo::Reverse {
                        existing_table: table_name.to_string(),
                        ref_column: col_name.clone(),
                    });
                }
            }

            // Also check: existing.id = target.col (swapped)
            let left_is_existing = matches_table(on.left.table.as_deref(), table_name, *alias);
            let right_is_target = on.right.table.as_deref() == Some(target_table);

            if left_is_existing && right_is_target && on.left.column == "id" {
                let col_name = &on.right.column;
                if let Some(col) = target_schema.column(col_name)
                    && matches!(&col.ty, ColumnType::Ref(t) if t == *table_name)
                {
                    // This is a reverse join - the target table has the ref
                    return Ok(ChainJoinInfo::Reverse {
                        existing_table: table_name.to_string(),
                        ref_column: col_name.clone(),
                    });
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
                    .find(|(name, alias, _)| *name == t || alias.is_some_and(|a| a == t))
                    .map(|(name, _, schema)| (*name, schema))
                    .ok_or_else(|| {
                        DatabaseError::Parse(parser::ParseError {
                            message: format!("Unknown table {} in WHERE clause", t),
                            position: 0,
                        })
                    })?
            } else {
                // Unqualified column - search all schemas
                // "id" is a special column that exists on every table
                if col_name == "id" {
                    all_tables
                        .first()
                        .map(|(name, _, schema)| (*name, schema))
                        .ok_or_else(|| {
                            DatabaseError::Parse(parser::ParseError {
                                message: "No tables in query".to_string(),
                                position: 0,
                            })
                        })?
                } else {
                    all_tables
                        .iter()
                        .find(|(_, _, s)| s.column(col_name).is_some())
                        .map(|(name, _, schema)| (*name, schema))
                        .ok_or_else(|| {
                            DatabaseError::Parse(parser::ParseError {
                                message: format!("Unknown column {} in WHERE clause", col_name),
                                position: 0,
                            })
                        })?
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

            let value = coerce_predicate_value(&literal_value, &column_type);

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

            let value = coerce_predicate_value(&literal_value, &column_type);

            // Use qualified column name since the join descriptor has qualified column names
            let qualified_name = format!("{}.{}", target_table, col_name);
            predicates.push(Predicate::eq(&qualified_name, value));
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
            // because star expressions expand to multiple columns and joins add more columns.
            current = self.add_join_array_aggregate_for_expr(
                builder,
                current,
                expr,
                outer_table,
                outer_alias,
                -1,
            )?;
        }

        Ok(current)
    }

    /// Add ArrayAggregate node to a JOIN graph for a single expression.
    fn add_join_array_aggregate_for_expr(
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
                    builder,
                    input,
                    inner,
                    outer_table,
                    outer_alias,
                    column_index,
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
            if let Some(col) = left_schema.column(col_name)
                && matches!(&col.ty, ColumnType::Ref(target) if target == right_table)
            {
                return Ok(JoinDirection::LeftToRight(col_name.clone()));
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
            if let Some(col) = left_schema.column(col_name)
                && matches!(&col.ty, ColumnType::Ref(target) if target == right_table)
            {
                return Ok(JoinDirection::LeftToRight(col_name.clone()));
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
            if let Some(col) = right_schema.column(col_name)
                && matches!(&col.ty, ColumnType::Ref(target) if target == left_table)
            {
                return Ok(JoinDirection::RightToLeft(col_name.clone()));
            }
        }

        // Also check: ON left_table.id = right_table.col
        if left_is_from_left && right_is_from_right {
            let col_name = &on.right.column;
            if let Some(col) = right_schema.column(col_name)
                && matches!(&col.ty, ColumnType::Ref(target) if target == left_table)
            {
                return Ok(JoinDirection::RightToLeft(col_name.clone()));
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
                coerce_predicate_value(&literal_value, &ColumnType::Ref("".to_string()))
            } else {
                let col_idx = schema.column_index(column).unwrap();
                coerce_predicate_value(&literal_value, &schema.columns[col_idx].ty)
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

    // =========================================================================
    // Migration Execution
    // =========================================================================

    /// Execute a schema migration on a table.
    ///
    /// This transforms all rows from the old schema to the new schema using
    /// a lens generated from the schema diff. The migration is executed eagerly,
    /// transforming all existing data immediately.
    ///
    /// # Arguments
    ///
    /// * `table` - The table to migrate
    /// * `new_schema` - The new schema to migrate to
    /// * `options` - Lens generation options (e.g., confirmed renames)
    ///
    /// # Returns
    ///
    /// A `MigrationResult` containing the new descriptor ID, lens, and statistics.
    ///
    /// # Errors
    ///
    /// Returns `DatabaseError::Migration` if migration fails.
    pub fn execute_migration(
        &self,
        table: &str,
        new_schema: TableSchema,
        options: LensGenerationOptions,
    ) -> Result<MigrationResult, DatabaseError> {
        // Get the current schema
        let old_schema = self
            .get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

        // Generate lens from schema diff
        let diff = diff_schemas(&old_schema, &new_schema);
        let lens_result = generate_lens(&diff, &options);
        let lens = lens_result.lens;
        let warnings = lens_result.warnings;

        // Get the current descriptor and its ID from the catalog
        let (old_descriptor, old_descriptor_id) = {
            // Get the descriptor ID from the catalog first
            let catalog_bytes = self
                .state
                .node
                .read(self.state.catalog_object_id, "main")
                .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?
                .ok_or_else(|| DatabaseError::Storage("catalog not found".to_string()))?;
            let catalog = Catalog::from_bytes(&catalog_bytes)
                .map_err(|e| DatabaseError::Storage(e.to_string()))?;
            let old_id = catalog
                .get_descriptor_id(table)
                .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

            // Read the descriptor from the version branch
            let data = self
                .state
                .node
                .read(old_id.as_object_id(), &old_id.version)
                .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?
                .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

            let descriptor = TableDescriptor::from_bytes(&data)
                .map_err(|e| MigrationError::CatalogError(e.to_string()))?;

            (descriptor, old_id)
        };

        // Create new descriptor with lens from parent version
        let new_descriptor = TableDescriptor {
            schema: new_schema.clone(),
            policies: old_descriptor.policies.clone(),
            lens_from_parent: Some(lens.clone()), // Transform from previous version
            rows_object_id: old_descriptor.rows_object_id,
            schema_object_id: old_descriptor.schema_object_id,
            index_object_ids: old_descriptor.index_object_ids.clone(),
        };
        // New version on same object (e.g., v1 -> v2)
        let new_descriptor_id = old_descriptor_id.next_version();

        // Read all current rows
        let rows = self.state.read_all_rows(table);

        // Transform each row
        let new_descriptor_arc = Arc::new(RowDescriptor::from_table_schema(&new_schema));
        let mut migrated_count = 0;
        let mut invisible_count = 0;

        for (row_id, old_row) in rows {
            // Apply lens forward transformation
            match lens.apply_forward_owned(&old_row) {
                Ok(mut new_row) => {
                    // Ensure the new row has the correct descriptor
                    new_row = OwnedRow::new(new_descriptor_arc.clone(), new_row.buffer);

                    // Write the transformed row back
                    self.state
                        .node
                        .write(
                            row_id,
                            "main",
                            &new_row.buffer,
                            "migration",
                            timestamp_now(),
                        )
                        .map_err(|e| {
                            DatabaseError::Storage(format!(
                                "failed to write migrated row {}: {:?}",
                                row_id, e
                            ))
                        })?;

                    migrated_count += 1;
                }
                Err(LensError::Incompatible { .. }) => {
                    // Row is incompatible with new schema - mark as invisible
                    // For now, we just count it. In the future, we might want to:
                    // - Move it to a separate branch
                    // - Mark it with metadata
                    // - Delete it
                    invisible_count += 1;
                }
                Err(e) => {
                    // Other lens errors are fatal
                    return Err(MigrationError::LensError { row_id, error: e }.into());
                }
            }
        }

        // Update the schema in our caches
        {
            let tables = self.state.tables.read().unwrap();
            if let Some(schema_id) = tables.get(table) {
                let mut schemas = self.state.schemas.write().unwrap();
                schemas.insert(*schema_id, new_schema.clone());
            }
        }

        // Store the new descriptor on the new version branch
        // (Same object, new branch for the new version)
        {
            let desc_object_id = new_descriptor_id.as_object_id();
            let new_version_branch = new_descriptor_id.version();
            let old_version_branch = &old_descriptor_id.version;

            // First, get the tip commit from the old version branch
            let object =
                self.state.node.get_object(desc_object_id).ok_or_else(|| {
                    DatabaseError::Storage("descriptor object not found".to_string())
                })?;

            let tip_commit = {
                let obj = object.read().unwrap();
                let old_branch = obj.branch(old_version_branch).ok_or_else(|| {
                    DatabaseError::Storage("old version branch not found".to_string())
                })?;
                *old_branch.frontier().first().ok_or_else(|| {
                    DatabaseError::Storage("old version branch has no commits".to_string())
                })?
            };

            // Create new version branch from old version's tip
            {
                let mut obj = object.write().unwrap();
                obj.create_branch(new_version_branch, old_version_branch, &tip_commit)
                    .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?;
            }

            // Write the new descriptor content to the new version branch
            self.state
                .node
                .write(
                    desc_object_id,
                    new_version_branch,
                    &new_descriptor.to_bytes(),
                    "migration",
                    timestamp_now(),
                )
                .map_err(|e| {
                    DatabaseError::Storage(format!("failed to write descriptor: {:?}", e))
                })?;

            // descriptor_objects map stays the same (same ObjectId)
        }

        // Update the catalog
        let catalog_data = self
            .state
            .node
            .read(self.state.catalog_object_id, "main")
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?
            .ok_or_else(|| DatabaseError::Storage("catalog not found".to_string()))?;

        let mut catalog = Catalog::from_bytes(&catalog_data)
            .map_err(|e| DatabaseError::Storage(e.to_string()))?;

        catalog.update_table(table.to_string(), new_descriptor_id.clone());

        self.state
            .node
            .write(
                self.state.catalog_object_id,
                "main",
                &catalog.to_bytes(),
                "migration",
                timestamp_now(),
            )
            .map_err(|e| DatabaseError::Storage(format!("failed to write catalog: {:?}", e)))?;

        // TODO: Notify query graphs about the schema change
        // This would invalidate queries and require re-building with new schema

        Ok(MigrationResult {
            new_descriptor_id: new_descriptor_id.clone(),
            lens,
            migrated_count,
            invisible_count,
            warnings,
        })
    }

    /// Preview a migration without executing it.
    ///
    /// Returns the lens that would be generated and any warnings,
    /// without actually transforming any data.
    pub fn preview_migration(
        &self,
        table: &str,
        new_schema: &TableSchema,
        options: &LensGenerationOptions,
    ) -> Result<(Lens, Vec<LensWarning>), DatabaseError> {
        let old_schema = self
            .get_table(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))?;

        let diff = diff_schemas(&old_schema, new_schema);
        let result = generate_lens(&diff, options);

        Ok((result.lens, result.warnings))
    }

    /// Get the current descriptor for a table.
    pub fn get_descriptor(&self, table: &str) -> Option<TableDescriptor> {
        // Get the current descriptor ID from catalog (includes version)
        let desc_id = self.get_descriptor_id(table).ok()?;
        // Read from the version branch
        let data = self
            .state
            .node
            .read(desc_id.as_object_id(), &desc_id.version)
            .ok()??;
        TableDescriptor::from_bytes(&data).ok()
    }

    /// Get the current descriptor ID for a table from the catalog.
    pub fn get_descriptor_id(&self, table: &str) -> Result<DescriptorId, DatabaseError> {
        let catalog_bytes = self
            .state
            .node
            .read(self.state.catalog_object_id, "main")
            .map_err(|e| DatabaseError::Storage(format!("{:?}", e)))?
            .ok_or_else(|| DatabaseError::Storage("catalog not found".to_string()))?;

        let catalog = Catalog::from_bytes(&catalog_bytes)
            .map_err(|e| DatabaseError::Storage(e.to_string()))?;

        catalog
            .get_descriptor_id(table)
            .ok_or_else(|| DatabaseError::TableNotFound(table.to_string()))
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
