//! SchemaManager - Coordinates schema evolution with query execution.
//!
//! This provides the top-level API for schema-aware queries, combining:
//! - SchemaContext for tracking current/live schema versions
//! - Lens management for migrations
//! - Schema-aware branch naming
//! - Integrated QueryManager for query/insert/update/delete operations
//! - Catalogue persistence for schema/lens discovery via sync

use std::{collections::HashMap, sync::Arc};

use blake3::Hasher;

use crate::object::{BranchName, ObjectId};
use crate::query_manager::manager::{DeleteHandle, InsertResult, QueryError, QueryManager};
use crate::query_manager::query::{Query, QueryBuilder};
use crate::query_manager::session::Session;
use crate::query_manager::types::{
    ComposedBranchName, RowDescriptor, Schema, SchemaHash, TableName, TablePolicies, Value,
};
use crate::storage::Storage;
use crate::sync_manager::SyncManager;
use uuid::Uuid;

use super::auto_lens::generate_lens;
use super::context::{QuerySchemaContext, SchemaContext, SchemaError};
use super::encoding::{
    decode_lens_transform, decode_permissions, decode_permissions_bundle, decode_permissions_head,
    decode_schema, encode_lens_transform, encode_permissions, encode_permissions_bundle,
    encode_permissions_head, encode_schema,
};
use super::lens::Lens;
use super::types::AppId;

#[derive(Clone, Debug, PartialEq)]
struct PermissionsBundleState {
    schema_hash: SchemaHash,
    version: u64,
    parent_bundle_object_id: Option<ObjectId>,
    permissions: HashMap<TableName, TablePolicies>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PermissionsHeadState {
    schema_hash: SchemaHash,
    version: u64,
    parent_bundle_object_id: Option<ObjectId>,
    bundle_object_id: ObjectId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PermissionsHeadSummary {
    pub schema_hash: SchemaHash,
    pub version: u64,
    pub parent_bundle_object_id: Option<ObjectId>,
    pub bundle_object_id: ObjectId,
}

/// SchemaManager coordinates schema evolution with query execution.
///
/// It manages:
/// - Current schema and environment
/// - Live schema versions reachable via lenses
/// - Lens registration and auto-generation
/// - Schema-aware branch naming
/// - Query execution with automatic lens transforms
/// - Catalogue persistence for schema/lens discovery via sync
///
/// # Example
///
/// ```ignore
/// let app_id = AppId::from_name("my-app");
/// let mut manager = SchemaManager::new(
///     SyncManager::new(),
///     schema,
///     app_id,
///     "dev",
///     "main",
/// )?;
///
/// // Add a previous schema version as "live"
/// manager.add_live_schema(old_schema)?;
///
/// // Persist schema and lens to catalogue for other clients
/// manager.persist_schema();
/// manager.persist_lens(&lens);
///
/// // Insert data
/// let handle = manager.insert("users", &[id, name])?;
///
/// // Query across all schema versions via subscription
/// let sub_id = manager.query_manager_mut().subscribe(manager.query("users").build())?;
/// manager.process();
/// let results = manager.query_manager_mut().get_subscription_results(sub_id);
/// manager.query_manager_mut().unsubscribe_with_sync(sub_id);
/// ```
pub struct SchemaManager {
    declared_current_schema: Option<Schema>,
    context: SchemaContext,
    query_manager: QueryManager,
    app_id: AppId,
    current_permissions_head: Option<PermissionsHeadState>,
    known_permissions_bundles: HashMap<ObjectId, PermissionsBundleState>,
    pending_permissions_head: Option<PermissionsHeadState>,
    /// Schemas known to this manager (for server mode).
    /// Server adds schemas here when received via catalogue sync.
    /// These are stored without requiring a lens path to current.
    known_schemas: Arc<HashMap<SchemaHash, Schema>>,
    known_schemas_dirty: bool,
}

impl SchemaManager {
    /// Create a new SchemaManager with integrated QueryManager.
    ///
    /// # Arguments
    ///
    /// * `sync_manager` - SyncManager for object persistence
    /// * `schema` - Current schema for this client
    /// * `app_id` - Application identifier for catalogue queries
    /// * `env` - Environment (e.g., "dev", "prod")
    /// * `user_branch` - User-facing branch name (e.g., "main")
    pub fn new(
        sync_manager: SyncManager,
        schema: Schema,
        app_id: AppId,
        env: &str,
        user_branch: &str,
    ) -> Result<Self, SchemaError> {
        let declared_current_schema = schema.clone();
        let schema = normalize_schema(schema);
        let structural_schema = strip_schema_policies(&schema);

        let context = SchemaContext::new(schema.clone(), env, user_branch);
        let current_hash = SchemaHash::compute(&schema);

        // Create QueryManager with empty context, then set current schema
        let mut query_manager = QueryManager::new(sync_manager);
        query_manager.set_current_schema(schema.clone(), env, user_branch);

        // Initialize known_schemas with current schema
        let mut known_schemas = HashMap::new();
        known_schemas.insert(current_hash, structural_schema);

        Ok(Self {
            declared_current_schema: Some(declared_current_schema),
            context,
            query_manager,
            app_id,
            current_permissions_head: None,
            known_permissions_bundles: HashMap::new(),
            pending_permissions_head: None,
            known_schemas: Arc::new(known_schemas),
            known_schemas_dirty: true,
        })
    }

    /// Create with default environment ("dev").
    pub fn with_defaults(
        sync_manager: SyncManager,
        schema: Schema,
        app_id: AppId,
        user_branch: &str,
    ) -> Result<Self, SchemaError> {
        Self::new(sync_manager, schema, app_id, "dev", user_branch)
    }

    /// Create a server-mode SchemaManager with no fixed current schema.
    ///
    /// Servers don't have a "current" schema - they serve multiple clients
    /// with different schema versions. Schemas are added via `add_known_schema()`
    /// when received from clients via catalogue sync.
    ///
    /// Queries are executed with explicit `QuerySchemaContext` rather than
    /// using implicit current schema context.
    pub fn new_server(sync_manager: SyncManager, app_id: AppId, _env: &str) -> Self {
        let query_manager = QueryManager::new(sync_manager);
        Self {
            declared_current_schema: None,
            context: SchemaContext::empty(),
            query_manager,
            app_id,
            current_permissions_head: None,
            known_permissions_bundles: HashMap::new(),
            pending_permissions_head: None,
            known_schemas: Arc::new(HashMap::new()),
            known_schemas_dirty: false,
        }
    }

    /// Check if this manager has a current schema set.
    ///
    /// Returns false for server-mode managers created with `new_server()`.
    pub fn has_current_schema(&self) -> bool {
        self.context.is_initialized()
    }

    /// Add a schema to known_schemas without requiring a lens path to current.
    ///
    /// Used by servers when receiving client schemas via catalogue sync.
    /// The schema becomes available for use in explicit-context queries.
    ///
    /// Also creates indices for all env/user_branch combinations if known.
    pub fn add_known_schema(&mut self, schema: Schema) {
        let schema = strip_schema_policies(&normalize_schema(schema));
        let hash = SchemaHash::compute(&schema);

        // Skip if already known
        if self.known_schemas.contains_key(&hash) {
            return;
        }

        Arc::make_mut(&mut self.known_schemas).insert(hash, schema.clone());
        self.known_schemas_dirty = true;

        // If we have a current schema context, also try the lens-path activation
        if self.context.is_initialized() {
            self.context.add_pending_schema(schema.clone());
            self.activate_pending_and_sync_to_query_manager();
        }

        self.try_apply_pending_permissions_head();
    }

    /// Get a known schema by hash.
    pub fn get_known_schema(&self, hash: &SchemaHash) -> Option<&Schema> {
        self.known_schemas.get(hash)
    }

    /// Check if a schema is known (either current, live, or in known_schemas).
    pub fn is_schema_known(&self, hash: &SchemaHash) -> bool {
        self.context.is_live(hash) || self.known_schemas.contains_key(hash)
    }

    /// Get the application ID.
    pub fn app_id(&self) -> AppId {
        self.app_id
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

    fn align_insert_values_to_runtime_schema(&self, table: &str, values: &[Value]) -> Vec<Value> {
        let Some(declared_schema) = self.declared_current_schema.as_ref() else {
            return values.to_vec();
        };

        let table_name = TableName::new(table);
        let Some(declared_table) = declared_schema.get(&table_name) else {
            return values.to_vec();
        };
        let Some(runtime_table) = self.context.current_schema.get(&table_name) else {
            return values.to_vec();
        };

        reorder_values_by_column_name(&declared_table.columns, &runtime_table.columns, values)
            .unwrap_or_else(|| values.to_vec())
    }

    /// Add a live schema version with auto-generated lens.
    ///
    /// The lens is automatically generated from the schema diff.
    /// Returns error if the generated lens is a draft (needs manual review).
    ///
    /// Automatically updates QueryManager indices and marks subscriptions for recompile.
    pub fn add_live_schema(&mut self, old_schema: Schema) -> Result<&Lens, SchemaError> {
        let old_schema = strip_schema_policies(&normalize_schema(old_schema));
        let lens = generate_lens(&old_schema, &self.context.current_schema);

        if lens.is_draft() {
            return Err(SchemaError::DraftLensInPath {
                source: lens.source_hash,
                target: lens.target_hash,
            });
        }

        let source_hash = lens.source_hash;

        // Update context
        self.context
            .add_live_schema(old_schema.clone(), lens.clone());

        // Update QueryManager (indices, branch map, subscriptions)
        self.query_manager.add_live_schema(old_schema);
        self.query_manager.register_lens(lens);

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
    /// Automatically updates QueryManager indices and marks subscriptions for recompile.
    pub fn add_live_schema_with_lens(
        &mut self,
        old_schema: Schema,
        lens: Lens,
    ) -> Result<(), SchemaError> {
        let old_schema = strip_schema_policies(&normalize_schema(old_schema));
        if lens.is_draft() {
            return Err(SchemaError::DraftLensInPath {
                source: lens.source_hash,
                target: lens.target_hash,
            });
        }

        // Update context
        self.context
            .add_live_schema(old_schema.clone(), lens.clone());

        // Update QueryManager
        self.query_manager.add_live_schema(old_schema);
        self.query_manager.register_lens(lens);

        Ok(())
    }

    /// Register a lens between two schemas.
    ///
    /// Also registers the lens in QueryManager and tries to activate pending schemas.
    pub fn register_lens(&mut self, lens: Lens) -> Result<(), SchemaError> {
        if lens.is_draft() {
            return Err(SchemaError::DraftLensInPath {
                source: lens.source_hash,
                target: lens.target_hash,
            });
        }

        // Update context
        self.context.register_lens(lens.clone());

        // Update QueryManager
        self.query_manager.register_lens(lens);

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
    ///
    /// Returns pairs of (lens, direction) indicating which transform to use.
    pub fn lens_path(
        &self,
        from: &SchemaHash,
    ) -> Result<Vec<(&Lens, super::lens::Direction)>, SchemaError> {
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

    /// Get all known schema hashes (current + any learned via catalogue).
    pub fn known_schema_hashes(&self) -> Vec<SchemaHash> {
        self.known_schemas.keys().copied().collect()
    }

    /// Get all pending schema hashes awaiting lens-path activation.
    pub fn pending_schema_hashes(&self) -> Vec<SchemaHash> {
        self.context.pending_schemas.keys().copied().collect()
    }

    /// Get all registered lens edges as (source, target) hash pairs.
    pub fn lens_edges(&self) -> Vec<(SchemaHash, SchemaHash)> {
        self.context.lenses.keys().copied().collect()
    }

    /// Compute a canonical digest of the catalogue state known to this manager.
    pub fn catalogue_state_hash(&self) -> String {
        let mut hasher = Hasher::new();
        hasher.update(b"jazz-catalogue-state-v1");

        let mut schemas: Vec<_> = self.known_schemas.iter().collect();
        schemas.sort_by(|(left_hash, _), (right_hash, _)| {
            left_hash.as_bytes().cmp(right_hash.as_bytes())
        });
        hasher.update(&(schemas.len() as u64).to_le_bytes());
        for (hash, schema) in schemas {
            hasher.update(b"schema");
            hasher.update(hash.as_bytes());
            let encoded = encode_schema(schema);
            hash_len_prefixed(&mut hasher, &encoded);
        }

        let mut lenses: Vec<_> = self.context.lenses.values().collect();
        lenses.sort_by(|left, right| {
            left.source_hash
                .as_bytes()
                .cmp(right.source_hash.as_bytes())
                .then_with(|| {
                    left.target_hash
                        .as_bytes()
                        .cmp(right.target_hash.as_bytes())
                })
        });
        hasher.update(&(lenses.len() as u64).to_le_bytes());
        for lens in lenses {
            hasher.update(b"lens");
            hasher.update(lens.source_hash.as_bytes());
            hasher.update(lens.target_hash.as_bytes());
            let encoded = encode_lens_transform(&lens.forward);
            hash_len_prefixed(&mut hasher, &encoded);
        }

        if let Some(head) = self.current_permissions_head
            && let Some(bundle) = self.known_permissions_bundles.get(&head.bundle_object_id)
        {
            hasher.update(b"permissions");
            hasher.update(head.schema_hash.as_bytes());
            hasher.update(&head.version.to_le_bytes());
            if let Some(parent_bundle_object_id) = head.parent_bundle_object_id {
                hasher.update(parent_bundle_object_id.uuid().as_bytes());
            }
            let encoded = encode_permissions(&bundle.permissions);
            hash_len_prefixed(&mut hasher, &encoded);
        }

        hasher.finalize().to_hex().to_string()
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

    pub fn current_permissions_head(&self) -> Option<PermissionsHeadSummary> {
        self.current_permissions_head
            .map(|head| PermissionsHeadSummary {
                schema_hash: head.schema_hash,
                version: head.version,
                parent_bundle_object_id: head.parent_bundle_object_id,
                bundle_object_id: head.bundle_object_id,
            })
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
        Some(&table_schema.columns)
    }

    // =========================================================================
    // Catalogue Persistence
    // =========================================================================

    /// Persist the current schema to the catalogue as an Object.
    ///
    /// The schema is stored on the "main" branch with metadata identifying it
    /// as a catalogue schema for this app. Other clients with the same app_id
    /// will receive this via catalogue sync.
    ///
    /// Returns the ObjectId of the stored schema object.
    pub fn persist_schema<H: Storage>(&mut self, storage: &mut H) -> ObjectId {
        let schema_hash = self.context.current_hash;
        let object_id = schema_hash.to_object_id();
        let content = encode_schema(&strip_schema_policies(&self.context.current_schema));

        let metadata = self.schema_metadata(&schema_hash);
        self.query_manager
            .sync_manager_mut()
            .create_object_with_content(storage, object_id, metadata, content);

        object_id
    }

    /// Persist any schema to the catalogue as an Object.
    ///
    /// Used when seeding or syncing historical schema versions.
    pub fn persist_schema_object<H: Storage>(
        &mut self,
        storage: &mut H,
        schema: &Schema,
    ) -> ObjectId {
        let schema = strip_schema_policies(schema);
        let schema_hash = SchemaHash::compute(&schema);
        let object_id = schema_hash.to_object_id();
        let content = encode_schema(&schema);

        let metadata = self.schema_metadata(&schema_hash);
        self.query_manager
            .sync_manager_mut()
            .create_object_with_content(storage, object_id, metadata, content);

        object_id
    }

    /// Persist a lens to the catalogue as an Object.
    ///
    /// The lens is stored on the "main" branch with metadata identifying it
    /// as a catalogue lens for this app. Other clients with the same app_id
    /// will receive this via catalogue sync.
    ///
    /// Returns the ObjectId of the stored lens object.
    pub fn persist_lens<H: Storage>(&mut self, storage: &mut H, lens: &Lens) -> ObjectId {
        let object_id = lens.object_id();
        let content = encode_lens_transform(&lens.forward);

        let metadata = self.lens_metadata(lens);
        self.query_manager
            .sync_manager_mut()
            .create_object_with_content(storage, object_id, metadata, content);

        object_id
    }

    pub fn persist_current_permissions<H: Storage>(&mut self, storage: &mut H) -> Option<ObjectId> {
        let head = self.current_permissions_head?;
        let bundle = self.known_permissions_bundles.get(&head.bundle_object_id)?;

        let bundle_metadata = self.permissions_bundle_metadata();
        let head_object_id = self.permissions_head_object_id();
        let head_metadata = self.permissions_head_metadata();
        let bundle_content = encode_permissions_bundle(
            bundle.schema_hash,
            bundle.version,
            bundle.parent_bundle_object_id,
            &bundle.permissions,
        );
        self.query_manager
            .sync_manager_mut()
            .create_object_with_content(
                storage,
                head.bundle_object_id,
                bundle_metadata,
                bundle_content,
            );

        let head_content = encode_permissions_head(
            head.schema_hash,
            head.version,
            head.parent_bundle_object_id,
            head.bundle_object_id,
        );
        self.query_manager
            .sync_manager_mut()
            .create_object_with_content(storage, head_object_id, head_metadata, head_content);

        Some(head_object_id)
    }

    pub fn publish_permissions_bundle<H: Storage>(
        &mut self,
        storage: &mut H,
        schema_hash: SchemaHash,
        permissions: HashMap<TableName, TablePolicies>,
        expected_parent_bundle_object_id: Option<ObjectId>,
    ) -> Result<Option<ObjectId>, SchemaError> {
        let current_parent_bundle_object_id = self
            .current_permissions_head
            .map(|head| head.bundle_object_id);
        if current_parent_bundle_object_id != expected_parent_bundle_object_id {
            return Err(SchemaError::StalePermissionsParent {
                expected: expected_parent_bundle_object_id,
                current: current_parent_bundle_object_id,
            });
        }

        let version = self
            .current_permissions_head
            .map(|head| head.version + 1)
            .unwrap_or(1);
        let bundle_state = PermissionsBundleState {
            schema_hash,
            version,
            parent_bundle_object_id: current_parent_bundle_object_id,
            permissions,
        };
        let bundle_object_id = self.permissions_bundle_object_id(&bundle_state);
        self.known_permissions_bundles
            .insert(bundle_object_id, bundle_state);
        let head = PermissionsHeadState {
            schema_hash,
            version,
            parent_bundle_object_id: current_parent_bundle_object_id,
            bundle_object_id,
        };
        self.current_permissions_head = Some(head);
        if self.apply_permissions_head(head) {
            self.pending_permissions_head = None;
        } else {
            self.pending_permissions_head = Some(head);
        }
        Ok(self.persist_current_permissions(storage))
    }

    /// Register a reviewed lens in memory, activate any newly reachable schemas,
    /// and persist the corresponding catalogue object for sync.
    pub fn publish_lens<H: Storage>(
        &mut self,
        storage: &mut H,
        lens: &Lens,
    ) -> Result<ObjectId, SchemaError> {
        self.register_lens(lens.clone())?;
        self.activate_pending_and_sync_to_query_manager();
        Ok(self.persist_lens(storage, lens))
    }

    /// Materialize known schema/lens catalogue objects into object storage for sync replay.
    ///
    /// Rehydration restores schema/lens knowledge into memory, but downstream sync replay
    /// needs the corresponding catalogue objects present in ObjectManager.
    pub fn materialize_catalogue_objects<H: Storage>(&mut self, storage: &mut H) {
        let current_hash = self.context.current_hash;
        let historical_schemas: Vec<Schema> = self
            .known_schemas
            .iter()
            .filter_map(|(hash, schema)| {
                if *hash == current_hash {
                    None
                } else {
                    Some(schema.clone())
                }
            })
            .collect();

        for schema in historical_schemas {
            self.persist_schema_object(storage, &schema);
        }

        let lenses: Vec<Lens> = self.context.lenses.values().cloned().collect();
        for lens in lenses {
            self.persist_lens(storage, &lens);
        }

        self.persist_current_permissions(storage);
    }

    /// Build metadata for a schema catalogue object.
    fn schema_metadata(&self, schema_hash: &SchemaHash) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Type.to_string(),
            crate::metadata::ObjectType::CatalogueSchema.to_string(),
        );
        metadata.insert(
            crate::metadata::MetadataKey::AppId.to_string(),
            self.app_id.uuid().to_string(),
        );
        metadata.insert(
            crate::metadata::MetadataKey::SchemaHash.to_string(),
            schema_hash.to_string(),
        );
        metadata
    }

    pub(crate) fn permissions_head_object_id_for(app_id: AppId) -> ObjectId {
        ObjectId::from_uuid(Uuid::new_v5(
            &Uuid::NAMESPACE_DNS,
            format!("jazz-catalogue-permissions-head:{}", app_id.uuid()).as_bytes(),
        ))
    }

    fn permissions_head_object_id(&self) -> ObjectId {
        Self::permissions_head_object_id_for(self.app_id)
    }

    fn permissions_bundle_object_id(&self, bundle: &PermissionsBundleState) -> ObjectId {
        let mut identity =
            format!("jazz-catalogue-permissions-bundle:{}:", self.app_id.uuid()).into_bytes();
        identity.extend_from_slice(&encode_permissions_bundle(
            bundle.schema_hash,
            bundle.version,
            bundle.parent_bundle_object_id,
            &bundle.permissions,
        ));
        ObjectId::from_uuid(Uuid::new_v5(&Uuid::NAMESPACE_DNS, &identity))
    }

    fn permissions_bundle_metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Type.to_string(),
            crate::metadata::ObjectType::CataloguePermissionsBundle.to_string(),
        );
        metadata.insert(
            crate::metadata::MetadataKey::AppId.to_string(),
            self.app_id.uuid().to_string(),
        );
        metadata
    }

    fn permissions_head_metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Type.to_string(),
            crate::metadata::ObjectType::CataloguePermissionsHead.to_string(),
        );
        metadata.insert(
            crate::metadata::MetadataKey::AppId.to_string(),
            self.app_id.uuid().to_string(),
        );
        metadata
    }

    /// Build metadata for a lens catalogue object.
    fn lens_metadata(&self, lens: &Lens) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Type.to_string(),
            crate::metadata::ObjectType::CatalogueLens.to_string(),
        );
        metadata.insert(
            crate::metadata::MetadataKey::AppId.to_string(),
            self.app_id.uuid().to_string(),
        );
        metadata.insert(
            crate::metadata::MetadataKey::SourceHash.to_string(),
            lens.source_hash.to_string(),
        );
        metadata.insert(
            crate::metadata::MetadataKey::TargetHash.to_string(),
            lens.target_hash.to_string(),
        );
        metadata
    }

    /// Process a catalogue update received via sync.
    ///
    /// Called when QueryManager receives an object with catalogue metadata
    /// matching this app_id.
    ///
    /// For schemas: stored as pending until a lens path exists.
    /// For lenses: registered immediately, then pending schemas are checked.
    pub fn process_catalogue_update(
        &mut self,
        object_id: ObjectId,
        metadata: &HashMap<String, String>,
        content: &[u8],
    ) -> Result<(), SchemaError> {
        let Some(type_str) = metadata.get(crate::metadata::MetadataKey::Type.as_str()) else {
            return Ok(()); // Not a catalogue object
        };

        match type_str.as_str() {
            t if t == crate::metadata::ObjectType::CatalogueSchema.as_str() => {
                self.process_catalogue_schema(metadata, content)
            }
            t if t == crate::metadata::ObjectType::CataloguePermissionsBundle.as_str() => {
                self.process_catalogue_permissions_bundle(object_id, metadata, content)
            }
            t if t == crate::metadata::ObjectType::CataloguePermissionsHead.as_str() => {
                self.process_catalogue_permissions_head(metadata, content)
            }
            t if t == crate::metadata::ObjectType::CataloguePermissions.as_str() => {
                self.process_catalogue_permissions_legacy(object_id, metadata, content)
            }
            t if t == crate::metadata::ObjectType::CatalogueLens.as_str() => {
                self.process_catalogue_lens(metadata, content)
            }
            _ => Ok(()), // Unknown type, ignore
        }
    }

    fn process_catalogue_schema(
        &mut self,
        metadata: &HashMap<String, String>,
        content: &[u8],
    ) -> Result<(), SchemaError> {
        // Verify app_id matches
        let app_id_str = metadata
            .get(crate::metadata::MetadataKey::AppId.as_str())
            .map(|s| s.as_str())
            .unwrap_or("");
        if app_id_str != self.app_id.uuid().to_string() {
            return Ok(()); // Different app, ignore
        }

        // Decode schema
        let schema = decode_schema(content)
            .map_err(|_| SchemaError::SchemaNotFound(SchemaHash::from_bytes([0; 32])))?;

        let hash = SchemaHash::compute(&schema);

        // Always add to known_schemas (server or client)
        // This allows server-mode query execution even without lens paths
        if !self.known_schemas.contains_key(&hash) {
            Arc::make_mut(&mut self.known_schemas).insert(hash, schema.clone());
            self.known_schemas_dirty = true;
        }

        // Skip if already live or is current
        if self.context.is_live(&hash) {
            return Ok(());
        }

        // If we have a current schema, also try lens-path activation
        if self.context.is_initialized() {
            // Add to pending - will be activated when lens path becomes available
            self.context.add_pending_schema(schema);

            // Try to activate in case we already have the lens path
            self.activate_pending_and_sync_to_query_manager();
        }

        self.try_apply_pending_permissions_head();

        Ok(())
    }

    fn process_catalogue_permissions_bundle(
        &mut self,
        object_id: ObjectId,
        metadata: &HashMap<String, String>,
        content: &[u8],
    ) -> Result<(), SchemaError> {
        let app_id_str = metadata
            .get(crate::metadata::MetadataKey::AppId.as_str())
            .map(|s| s.as_str())
            .unwrap_or("");
        if app_id_str != self.app_id.uuid().to_string() {
            return Ok(());
        }

        let (schema_hash, version, parent_bundle_object_id, permissions) =
            decode_permissions_bundle(content)
                .map_err(|_| SchemaError::SchemaNotFound(SchemaHash::from_bytes([0; 32])))?;
        self.known_permissions_bundles.insert(
            object_id,
            PermissionsBundleState {
                schema_hash,
                version,
                parent_bundle_object_id,
                permissions,
            },
        );

        self.try_apply_pending_permissions_head();

        Ok(())
    }

    fn process_catalogue_permissions_head(
        &mut self,
        metadata: &HashMap<String, String>,
        content: &[u8],
    ) -> Result<(), SchemaError> {
        let app_id_str = metadata
            .get(crate::metadata::MetadataKey::AppId.as_str())
            .map(|s| s.as_str())
            .unwrap_or("");
        if app_id_str != self.app_id.uuid().to_string() {
            return Ok(());
        }

        let (schema_hash, version, parent_bundle_object_id, bundle_object_id) =
            decode_permissions_head(content)
                .map_err(|_| SchemaError::SchemaNotFound(SchemaHash::from_bytes([0; 32])))?;
        let head = PermissionsHeadState {
            schema_hash,
            version,
            parent_bundle_object_id,
            bundle_object_id,
        };
        if let Some(current_head) = self.current_permissions_head
            && current_head.version > head.version
        {
            return Ok(());
        }
        self.current_permissions_head = Some(head);
        if self.apply_permissions_head(head) {
            self.pending_permissions_head = None;
        } else {
            self.pending_permissions_head = Some(head);
        }

        Ok(())
    }

    fn process_catalogue_permissions_legacy(
        &mut self,
        object_id: ObjectId,
        metadata: &HashMap<String, String>,
        content: &[u8],
    ) -> Result<(), SchemaError> {
        let app_id_str = metadata
            .get(crate::metadata::MetadataKey::AppId.as_str())
            .map(|s| s.as_str())
            .unwrap_or("");
        if app_id_str != self.app_id.uuid().to_string() {
            return Ok(());
        }

        let schema_hash = metadata
            .get(crate::metadata::MetadataKey::SchemaHash.as_str())
            .ok_or_else(|| SchemaError::SchemaNotFound(SchemaHash::from_bytes([0; 32])))
            .and_then(|value| parse_schema_hash(value))?;
        let permissions = decode_permissions(content)
            .map_err(|_| SchemaError::SchemaNotFound(SchemaHash::from_bytes([0; 32])))?;
        self.known_permissions_bundles.insert(
            object_id,
            PermissionsBundleState {
                schema_hash,
                version: 1,
                parent_bundle_object_id: None,
                permissions,
            },
        );
        let head = PermissionsHeadState {
            schema_hash,
            version: 1,
            parent_bundle_object_id: None,
            bundle_object_id: object_id,
        };
        self.current_permissions_head = Some(head);
        if self.apply_permissions_head(head) {
            self.pending_permissions_head = None;
        } else {
            self.pending_permissions_head = Some(head);
        }

        Ok(())
    }

    fn process_catalogue_lens(
        &mut self,
        metadata: &HashMap<String, String>,
        content: &[u8],
    ) -> Result<(), SchemaError> {
        // Verify app_id matches
        let app_id_str = metadata
            .get(crate::metadata::MetadataKey::AppId.as_str())
            .map(|s| s.as_str())
            .unwrap_or("");
        if app_id_str != self.app_id.uuid().to_string() {
            return Ok(()); // Different app, ignore
        }

        // Parse source/target hashes from metadata
        let source_hex = metadata
            .get(crate::metadata::MetadataKey::SourceHash.as_str())
            .ok_or_else(|| SchemaError::LensNotFound {
                source: SchemaHash::from_bytes([0; 32]),
                target: SchemaHash::from_bytes([0; 32]),
            })?;
        let target_hex = metadata
            .get(crate::metadata::MetadataKey::TargetHash.as_str())
            .ok_or_else(|| SchemaError::LensNotFound {
                source: SchemaHash::from_bytes([0; 32]),
                target: SchemaHash::from_bytes([0; 32]),
            })?;

        let source_hash = parse_schema_hash(source_hex)?;
        let target_hash = parse_schema_hash(target_hex)?;

        // Skip if we already have this lens (handles duplicate syncs)
        // Since ObjectId is deterministic from hashes and encoding is deterministic,
        // the same source/target should always produce identical content.
        if self.context.get_lens(&source_hash, &target_hash).is_some() {
            return Ok(());
        }

        // Decode lens transform
        let transform = decode_lens_transform(content).map_err(|_| SchemaError::LensNotFound {
            source: source_hash,
            target: target_hash,
        })?;

        // Reconstruct lens (backward is computed from forward)
        let lens = Lens::new(source_hash, target_hash, transform);

        // Log warning if draft, but still store it
        // Note: Draft lenses can still be registered but won't be used for activation
        // unless they're the only path available (which will fail validation)
        if lens.is_draft() {
            // TODO: proper logging
            // Draft lens received via catalogue - storing but not activating schemas through it
        }

        // Register the lens in both context and QueryManager
        self.context.register_lens(lens.clone());
        self.query_manager.register_lens(lens);

        // Try to activate pending schemas that may now be reachable
        self.activate_pending_and_sync_to_query_manager();
        self.try_apply_pending_permissions_head();

        Ok(())
    }

    fn schema_for_permissions_hash(&self, schema_hash: SchemaHash) -> Option<Schema> {
        if self.context.is_initialized() && self.context.current_hash == schema_hash {
            return Some(strip_schema_policies(&self.context.current_schema));
        }

        self.context
            .get_schema(&schema_hash)
            .map(strip_schema_policies)
            .or_else(|| self.known_schemas.get(&schema_hash).cloned())
    }

    fn apply_permissions_head(&mut self, head: PermissionsHeadState) -> bool {
        let Some(bundle) = self.known_permissions_bundles.get(&head.bundle_object_id) else {
            return false;
        };
        if bundle.schema_hash != head.schema_hash {
            return false;
        }
        if bundle.version != head.version {
            return false;
        }
        if bundle.parent_bundle_object_id != head.parent_bundle_object_id {
            return false;
        }
        let Some(schema) = self.schema_for_permissions_hash(head.schema_hash) else {
            return false;
        };

        let authorization_schema = merge_permissions_into_schema(&schema, &bundle.permissions);
        self.query_manager
            .set_authorization_schema(authorization_schema);
        true
    }

    fn try_apply_pending_permissions_head(&mut self) {
        let Some(head) = self.pending_permissions_head else {
            return;
        };

        if self.apply_permissions_head(head) {
            self.pending_permissions_head = None;
        }
    }

    /// Try to activate pending schemas that now have lens paths.
    ///
    /// Called after registering new lenses. Returns hashes of newly activated schemas.
    pub fn try_activate_pending_schemas(&mut self) -> Vec<SchemaHash> {
        self.context.try_activate_pending()
    }

    /// Activate pending schemas and sync them to QueryManager.
    ///
    /// This is the incremental replacement for sync_context().
    fn activate_pending_and_sync_to_query_manager(&mut self) {
        let activated = self.context.try_activate_pending();
        if activated.is_empty() {
            return;
        }

        // For each newly activated schema, add it to QueryManager
        for hash in &activated {
            if let Some(schema) = self.context.live_schemas.get(hash).cloned() {
                self.query_manager.add_live_schema(schema);
            }
        }

        // Pending row updates will be retried in the next process() call,
        // which has access to Storage needed for index updates.
    }

    // =========================================================================
    // Query/Write Operations (delegated to QueryManager)
    // =========================================================================

    /// Create a query builder for a table.
    pub fn query(&self, table: &str) -> QueryBuilder {
        QueryBuilder::new(table)
    }

    /// Subscribe to a query with explicit schema context (for server use).
    ///
    /// Servers don't have a fixed "current" schema - they serve multiple clients
    /// with different schema versions. This method allows subscribing to a query
    /// using the client's schema as the "current" for that subscription.
    ///
    /// The schema must be in `known_schemas` (received via catalogue sync).
    /// Returns `UnknownSchema` error if the schema is not known.
    ///
    /// # Arguments
    ///
    /// * `query` - The query to subscribe to
    /// * `ctx` - Schema context from the client (env, schema_hash, user_branch)
    /// * `session` - Optional session for policy evaluation
    pub fn subscribe_with_schema_context(
        &mut self,
        query: Query,
        ctx: &QuerySchemaContext,
        session: Option<Session>,
    ) -> Result<crate::query_manager::QuerySubscriptionId, QueryError> {
        // Look up the target schema in known_schemas
        let target_schema = self
            .known_schemas
            .get(&ctx.schema_hash)
            .ok_or(QueryError::UnknownSchema(ctx.schema_hash))?
            .clone();

        // Build a SchemaContext with target as current
        let mut temp_context =
            SchemaContext::new(target_schema.clone(), &ctx.env, &ctx.user_branch);

        // Copy lenses from our main context for multi-schema queries
        for ((_source, _target), lens) in &self.context.lenses {
            temp_context.register_lens(lens.clone());
        }

        // Add other known schemas as potential live schemas
        for (hash, schema) in self.known_schemas.iter() {
            if *hash != ctx.schema_hash {
                // Add to pending - will activate if lens path exists to target
                temp_context.add_pending_schema(schema.clone());
            }
        }

        // Try to activate any pending schemas that now have lens paths
        temp_context.try_activate_pending();

        // Ensure the client's branch is registered for indexing (server-mode)
        let client_branch =
            ComposedBranchName::new(&ctx.env, ctx.schema_hash, &ctx.user_branch).to_branch_name();
        self.query_manager
            .add_schema_branch(client_branch.as_str(), ctx.schema_hash);

        // Ensure indices exist for all branches in the temp context
        for branch_name in temp_context.all_branch_names() {
            let branch_str = branch_name.as_str();
            if let Some(composed) = ComposedBranchName::parse(&branch_name)
                && let Some(schema) = self.known_schemas.get(&composed.schema_hash)
            {
                for (table_name, table_schema) in schema {
                    self.query_manager.ensure_indices_for_branch(
                        table_name.as_str(),
                        branch_str,
                        table_schema,
                    );
                }
            }
        }

        // Subscribe using the temporary context
        self.query_manager.subscribe_with_explicit_context(
            query,
            &target_schema,
            &temp_context,
            session,
        )
    }

    /// Insert a row into the current schema's branch.
    pub fn insert<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        values: &[Value],
    ) -> Result<InsertResult, QueryError> {
        let _span =
            tracing::debug_span!("SM::insert", table, schema_hash = %self.context.current_hash)
                .entered();
        self.insert_with_session(storage, table, values, None)
    }

    /// Insert with session-based policy checking.
    pub fn insert_with_session<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        values: &[Value],
        session: Option<&Session>,
    ) -> Result<InsertResult, QueryError> {
        let aligned_values = self.align_insert_values_to_runtime_schema(table, values);
        self.query_manager.insert_on_branch_with_session(
            storage,
            table,
            self.context.branch_name().as_str(),
            &aligned_values,
            session,
        )
    }

    /// Delete a row (soft delete) from current schema's branch.
    pub fn delete<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        object_id: ObjectId,
    ) -> Result<DeleteHandle, QueryError> {
        let _span = tracing::debug_span!("SM::delete", table, %object_id, schema_hash = %self.context.current_hash).entered();
        self.query_manager.delete_on_branch(
            storage,
            table,
            self.context.branch_name().as_str(),
            object_id,
        )
    }

    /// Process pending operations (drives SyncManager).
    ///
    /// This also processes any pending catalogue updates (schemas/lenses) that
    /// were received via sync. Catalogue schemas are stored as pending until
    /// a lens path exists, then activated.
    ///
    /// When schemas activate, QueryManager is updated incrementally and
    /// buffered row updates are retried.
    pub fn process<H: Storage>(&mut self, storage: &mut H) {
        let _span = tracing::debug_span!("SM::process").entered();
        self.query_manager.process(storage);

        // Process any catalogue updates queued by QueryManager
        let updates = self.query_manager.take_pending_catalogue_updates();
        for update in updates {
            // Ignore errors from individual catalogue updates - they're non-critical
            let _ =
                self.process_catalogue_update(update.object_id, &update.metadata, &update.content);
        }

        // Sync known schemas to QueryManager for server-mode lazy activation
        // This enables QueryManager to activate branches when rows arrive
        if self.known_schemas_dirty {
            self.query_manager
                .set_known_schemas(Arc::clone(&self.known_schemas));
            self.known_schemas_dirty = false;
        }

        // Final attempt to activate any remaining pending schemas
        self.activate_pending_and_sync_to_query_manager();

        // Retry any pending row updates that might now be processable
        self.query_manager.retry_pending_row_updates(storage);
    }
}

fn reorder_values_by_column_name(
    source_descriptor: &RowDescriptor,
    target_descriptor: &RowDescriptor,
    values: &[Value],
) -> Option<Vec<Value>> {
    if values.len() != source_descriptor.columns.len()
        || source_descriptor.columns.len() != target_descriptor.columns.len()
    {
        return None;
    }

    let mut values_by_column = HashMap::with_capacity(values.len());
    for (column, value) in source_descriptor.columns.iter().zip(values.iter()) {
        values_by_column.insert(column.name, value.clone());
    }

    let mut reordered_values = Vec::with_capacity(values.len());
    for column in &target_descriptor.columns {
        reordered_values.push(values_by_column.remove(&column.name)?);
    }

    Some(reordered_values)
}

fn merge_permissions_into_schema(
    schema: &Schema,
    permissions: &HashMap<TableName, TablePolicies>,
) -> Schema {
    schema
        .iter()
        .map(|(table_name, table_schema)| {
            let mut merged = table_schema.clone();
            if let Some(table_policies) = permissions.get(table_name) {
                merged.policies = table_policies.clone();
            } else {
                merged.policies = TablePolicies::default();
            }
            (*table_name, merged)
        })
        .collect()
}

fn strip_schema_policies(schema: &Schema) -> Schema {
    schema
        .iter()
        .map(|(table_name, table_schema)| {
            let mut structural = table_schema.clone();
            structural.policies = TablePolicies::default();
            (*table_name, structural)
        })
        .collect()
}

fn normalize_schema(mut schema: Schema) -> Schema {
    for table_schema in schema.values_mut() {
        normalize_table_schema(table_schema);
    }
    schema
}

fn hash_len_prefixed(hasher: &mut Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn normalize_table_schema(table_schema: &mut crate::query_manager::types::TableSchema) {
    table_schema
        .columns
        .columns
        .sort_unstable_by(|left, right| left.name.as_str().cmp(right.name.as_str()));
}

/// Parse a hex-encoded SchemaHash string.
fn parse_schema_hash(hex_str: &str) -> Result<SchemaHash, SchemaError> {
    let bytes = hex::decode(hex_str)
        .map_err(|_| SchemaError::SchemaNotFound(SchemaHash::from_bytes([0; 32])))?;
    if bytes.len() != 32 {
        return Err(SchemaError::SchemaNotFound(SchemaHash::from_bytes([0; 32])));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(SchemaHash::from_bytes(arr))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::policy::PolicyExpr;
    use crate::query_manager::types::{
        ColumnType, SchemaBuilder, SchemaHash, TableName, TablePolicies, TableSchema,
    };

    fn test_app_id() -> AppId {
        AppId::from_name("test-app")
    }

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
        let manager =
            SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();

        assert_eq!(manager.env(), "dev");
        assert_eq!(manager.user_branch(), "main");
        assert_eq!(manager.app_id(), test_app_id());
    }

    #[test]
    fn schema_manager_new_normalizes_table_columns_by_name() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("name", ColumnType::Text)
                    .column("id", ColumnType::Uuid)
                    .nullable_column("email", ColumnType::Text),
            )
            .build();

        let manager =
            SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();

        let descriptor = manager.current_schema().get(&"users".into()).unwrap();
        let column_names: Vec<_> = descriptor
            .columns
            .columns
            .iter()
            .map(|column| column.name_str())
            .collect();

        assert_eq!(column_names, vec!["email", "id", "name"]);
    }

    #[test]
    fn schema_manager_new_hashes_equivalent_column_orderings_identically() {
        let schema_a = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("name", ColumnType::Text)
                    .column("id", ColumnType::Uuid)
                    .nullable_column("email", ColumnType::Text),
            )
            .build();
        let schema_b = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .nullable_column("email", ColumnType::Text)
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();

        let manager_a =
            SchemaManager::new(SyncManager::new(), schema_a, test_app_id(), "dev", "main").unwrap();
        let manager_b =
            SchemaManager::new(SyncManager::new(), schema_b, test_app_id(), "dev", "main").unwrap();

        assert_eq!(manager_a.current_hash(), manager_b.current_hash());
    }

    #[test]
    fn schema_manager_branch_name() {
        let schema = make_schema_v1();
        let manager =
            SchemaManager::new(SyncManager::new(), schema, test_app_id(), "prod", "feature")
                .unwrap();

        let branch = manager.branch_name();
        let s = branch.as_str();

        assert!(s.starts_with("prod-"));
        assert!(s.ends_with("-feature"));
    }

    #[test]
    fn schema_manager_add_live_schema() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
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

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
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

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
        manager.add_live_schema_with_lens(v1, lens).unwrap();

        assert_eq!(manager.all_branches().len(), 2);
    }

    #[test]
    fn schema_manager_validate() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
        manager.add_live_schema(v1).unwrap();

        // Should pass - no draft lenses
        assert!(manager.validate().is_ok());
    }

    #[test]
    fn schema_manager_lens_path() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v1_hash = SchemaHash::compute(&v1);

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
        manager.add_live_schema(v1).unwrap();

        let path = manager.lens_path(&v1_hash).unwrap();
        assert_eq!(path.len(), 1);
    }

    #[test]
    fn schema_manager_generate_lens_without_register() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();

        let manager =
            SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main")
                .unwrap();
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

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
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

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
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

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
        manager.add_live_schema(v1).unwrap();

        // V1 has 2 columns (id, name)
        let v1_desc = manager.get_table_descriptor("users", &v1_hash).unwrap();
        assert_eq!(v1_desc.columns.len(), 2);

        // V2 has 3 columns (id, name, email)
        let v2_desc = manager.get_table_descriptor("users", &v2_hash).unwrap();
        assert_eq!(v2_desc.columns.len(), 3);
    }

    #[test]
    fn permissions_head_waits_for_bundle_then_applies() {
        let schema = make_schema_v2();
        let schema_hash = SchemaHash::compute(&schema);
        let mut manager =
            SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();
        let permissions = HashMap::from([(
            TableName::new("users"),
            TablePolicies::new().with_select(PolicyExpr::True),
        )]);
        let bundle = PermissionsBundleState {
            schema_hash,
            version: 3,
            parent_bundle_object_id: Some(ObjectId::new()),
            permissions: permissions.clone(),
        };
        let bundle_object_id = manager.permissions_bundle_object_id(&bundle);
        let head = PermissionsHeadState {
            schema_hash,
            version: bundle.version,
            parent_bundle_object_id: bundle.parent_bundle_object_id,
            bundle_object_id,
        };

        manager
            .process_catalogue_update(
                manager.permissions_head_object_id(),
                &manager.permissions_head_metadata(),
                &encode_permissions_head(
                    schema_hash,
                    bundle.version,
                    bundle.parent_bundle_object_id,
                    bundle_object_id,
                ),
            )
            .expect("head should process");
        assert_eq!(manager.current_permissions_head, Some(head));
        assert_eq!(manager.pending_permissions_head, Some(head));

        manager
            .process_catalogue_update(
                bundle_object_id,
                &manager.permissions_bundle_metadata(),
                &encode_permissions_bundle(
                    schema_hash,
                    bundle.version,
                    bundle.parent_bundle_object_id,
                    &permissions,
                ),
            )
            .expect("bundle should process");

        assert_eq!(manager.current_permissions_head, Some(head));
        assert_eq!(manager.pending_permissions_head, None);
        assert_eq!(
            manager
                .known_permissions_bundles
                .get(&bundle_object_id)
                .map(|state| state.permissions.clone()),
            Some(permissions)
        );
    }

    #[test]
    fn publish_permissions_bundle_rejects_stale_parent() {
        let schema = make_schema_v2();
        let schema_hash = SchemaHash::compute(&schema);
        let mut manager =
            SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();
        let mut storage = crate::storage::MemoryStorage::new();
        let permissions = HashMap::from([(
            TableName::new("users"),
            TablePolicies::new().with_select(PolicyExpr::True),
        )]);

        manager
            .publish_permissions_bundle(&mut storage, schema_hash, permissions.clone(), None)
            .expect("initial permissions publish should succeed");

        let stale =
            manager.publish_permissions_bundle(&mut storage, schema_hash, permissions, None);
        assert!(matches!(
            stale,
            Err(SchemaError::StalePermissionsParent {
                expected: None,
                current: Some(_),
            })
        ));
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

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
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
        use crate::storage::MemoryStorage;

        let schema = make_schema_v2();
        let mut storage = MemoryStorage::new();
        let mut manager =
            SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();

        // Insert a row
        let id = ObjectId::new();
        let id_val = Value::Uuid(id);
        let name = Value::Text("Alice".into());
        let email = Value::Text("alice@example.com".into());

        let descriptor = manager
            .current_schema()
            .get(&"users".into())
            .unwrap()
            .columns
            .clone();
        let values = vec![id_val.clone(), name.clone(), email.clone()];

        let _handle = manager.insert(&mut storage, "users", &values).unwrap();
        manager.process(&mut storage);

        // Query via subscribe/process/unsubscribe pattern
        let query = manager.query("users").build();
        let qm = manager.query_manager_mut();
        let sub_id = qm.subscribe(query).unwrap();
        qm.process(&mut storage);
        let results = qm.get_subscription_results(sub_id);
        qm.unsubscribe_with_sync(sub_id);

        assert_eq!(results.len(), 1);
        let id_idx = descriptor.column_index("id").unwrap();
        assert_eq!(results[0].1[id_idx], id_val);
    }
}
