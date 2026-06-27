//! SchemaManager - Coordinates schema evolution with query execution.
//!
//! This provides the top-level API for schema-aware queries, combining:
//! - SchemaContext for tracking current/live schema versions
//! - Lens management for migrations
//! - Schema-aware branch naming
//! - Catalogue persistence for schema/lens discovery via sync

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use blake3::Hasher;

use crate::catalogue::CatalogueEntry;
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::{
    ComposedBranchName, RowDescriptor, RowPolicyMode, Schema, SchemaHash, TableName, TablePolicies,
    Value,
};
use crate::schema_manager::catalogue_storage::SchemaManagerCatalogueStorage;
use crate::schema_manager::rehydrate::latest_catalogue_content;
use crate::sync::clock::MonotonicClock;
use crate::sync::vocabulary::ConnectionSchemaDiagnostics;
use uuid::Uuid;

use super::auto_lens::generate_lens;
use super::context::{SchemaContext, SchemaError};
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

#[derive(Clone, Debug, PartialEq)]
pub struct CurrentPermissionsSummary {
    pub head: PermissionsHeadSummary,
    pub permissions: HashMap<TableName, TablePolicies>,
}

/// SchemaManager coordinates schema evolution with catalogue state.
///
/// It manages:
/// - Current schema and environment
/// - Live schema versions reachable via lenses
/// - Lens registration and auto-generation
/// - Schema-aware branch naming
/// - Catalogue persistence for schema/lens discovery via sync
///
/// # Example
///
/// ```ignore
/// let app_id = AppId::from_name("my-app");
/// let mut manager = SchemaManager::new(schema, app_id, "dev", "main")?;
///
/// // Add a previous schema version as "live"
/// manager.add_live_schema(old_schema)?;
///
/// // Persist schema and lens to catalogue for other clients
/// manager.persist_schema();
/// manager.persist_lens(&lens);
///
/// // Direct-core runtimes consume the schema/catalogue state exposed here.
/// ```
pub struct SchemaManager {
    context: SchemaContext,
    app_id: AppId,
    catalogue_clock: MonotonicClock,
    pending_catalogue_updates: Vec<CatalogueEntry>,
    catalogue_publish_timestamps: HashMap<ObjectId, u64>,
    current_permissions_head: Option<PermissionsHeadState>,
    known_permissions_bundles: HashMap<ObjectId, PermissionsBundleState>,
    pending_permissions_head: Option<PermissionsHeadState>,
    /// Schemas known to this manager (for server mode).
    /// Server adds schemas here when received via catalogue sync.
    /// These are stored without requiring a lens path to current.
    known_schemas: Arc<HashMap<SchemaHash, Schema>>,
    known_schemas_dirty: bool,
    persisted_current_schema_in_storage: HashSet<(usize, SchemaHash)>,
}

impl SchemaManager {
    /// Create a new SchemaManager.
    ///
    /// # Arguments
    ///
    /// * `schema` - Current schema for this client
    /// * `app_id` - Application identifier for catalogue queries
    /// * `env` - Environment (e.g., "dev", "prod")
    /// * `user_branch` - User-facing branch name (e.g., "main")
    pub fn new(
        schema: Schema,
        app_id: AppId,
        env: &str,
        user_branch: &str,
    ) -> Result<Self, SchemaError> {
        let row_policy_mode = if schema_has_any_explicit_policies(&schema) {
            RowPolicyMode::Enforcing
        } else {
            RowPolicyMode::PermissiveLocal
        };
        Self::new_with_policy_mode(schema, app_id, env, user_branch, row_policy_mode)
    }

    pub fn new_with_policy_mode(
        schema: Schema,
        app_id: AppId,
        env: &str,
        user_branch: &str,
        row_policy_mode: RowPolicyMode,
    ) -> Result<Self, SchemaError> {
        let _ = row_policy_mode;
        let structural_schema = strip_schema_policies(&schema);

        let context = SchemaContext::new(schema.clone(), env, user_branch);
        let current_hash = SchemaHash::compute(&schema);

        // Initialize known_schemas with current schema
        let mut known_schemas = HashMap::new();
        known_schemas.insert(current_hash, structural_schema);

        Ok(Self {
            context,
            app_id,
            catalogue_clock: MonotonicClock::new(),
            pending_catalogue_updates: Vec::new(),
            catalogue_publish_timestamps: HashMap::new(),
            current_permissions_head: None,
            known_permissions_bundles: HashMap::new(),
            pending_permissions_head: None,
            known_schemas: Arc::new(known_schemas),
            known_schemas_dirty: true,
            persisted_current_schema_in_storage: HashSet::new(),
        })
    }

    /// Create with default environment ("dev").
    pub fn with_defaults(
        schema: Schema,
        app_id: AppId,
        user_branch: &str,
    ) -> Result<Self, SchemaError> {
        Self::new(schema, app_id, "dev", user_branch)
    }

    /// Create a server-mode SchemaManager with no fixed current schema.
    ///
    /// Servers don't have a "current" schema - they serve multiple clients
    /// with different schema versions. Schemas are added via `add_known_schema()`
    /// when received from clients via catalogue sync.
    ///
    /// Queries are executed with explicit `QuerySchemaContext` rather than
    /// using implicit current schema context.
    pub fn new_server(app_id: AppId, _env: &str) -> Self {
        Self {
            context: SchemaContext::empty(),
            app_id,
            catalogue_clock: MonotonicClock::new(),
            pending_catalogue_updates: Vec::new(),
            catalogue_publish_timestamps: HashMap::new(),
            current_permissions_head: None,
            known_permissions_bundles: HashMap::new(),
            pending_permissions_head: None,
            known_schemas: Arc::new(HashMap::new()),
            known_schemas_dirty: false,
            persisted_current_schema_in_storage: HashSet::new(),
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
        let schema = strip_schema_policies(&schema);
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
            self.activate_pending_schemas();
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

    /// Add a live schema version with auto-generated lens.
    ///
    /// The lens is automatically generated from the schema diff.
    /// Returns error if the generated lens is a draft (needs manual review).
    ///
    pub fn add_live_schema(&mut self, old_schema: Schema) -> Result<&Lens, SchemaError> {
        let old_schema = strip_schema_policies(&old_schema);
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
    pub fn add_live_schema_with_lens(
        &mut self,
        old_schema: Schema,
        lens: Lens,
    ) -> Result<(), SchemaError> {
        let old_schema = strip_schema_policies(&old_schema);
        if lens.is_draft() {
            return Err(SchemaError::DraftLensInPath {
                source: lens.source_hash,
                target: lens.target_hash,
            });
        }

        // Update context
        self.context
            .add_live_schema(old_schema.clone(), lens.clone());

        Ok(())
    }

    /// Register a lens between two schemas.
    ///
    /// Also tries to activate pending schemas.
    pub fn register_lens(&mut self, lens: Lens) -> Result<(), SchemaError> {
        if lens.is_draft() {
            return Err(SchemaError::DraftLensInPath {
                source: lens.source_hash,
                target: lens.target_hash,
            });
        }

        // Update context
        self.context.register_lens(lens.clone());

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

    pub fn current_permissions_head(&self) -> Option<PermissionsHeadSummary> {
        self.current_permissions_head
            .map(|head| PermissionsHeadSummary {
                schema_hash: head.schema_hash,
                version: head.version,
                parent_bundle_object_id: head.parent_bundle_object_id,
                bundle_object_id: head.bundle_object_id,
            })
    }

    pub fn current_permissions(&self) -> Option<CurrentPermissionsSummary> {
        let head = self.current_permissions_head()?;
        let bundle = self.known_permissions_bundles.get(&head.bundle_object_id)?;
        Some(CurrentPermissionsSummary {
            head,
            permissions: bundle.permissions.clone(),
        })
    }

    pub fn connection_schema_diagnostics(
        &self,
        client_schema_hash: SchemaHash,
    ) -> ConnectionSchemaDiagnostics {
        let active_permissions_hash = self
            .current_permissions_head
            .map(|head| head.schema_hash)
            .or_else(|| self.has_current_schema().then_some(self.current_hash()));
        let reachable_hashes = self.non_draft_reachable_hashes(client_schema_hash);
        let disconnected_permissions_schema_hash = active_permissions_hash
            .filter(|permissions_hash| !reachable_hashes.contains(permissions_hash));

        let mut unreachable_schema_hashes: Vec<_> = self
            .known_schema_hashes()
            .into_iter()
            .filter(|hash| *hash != client_schema_hash)
            .filter(|hash| !reachable_hashes.contains(hash))
            .filter(|hash| Some(*hash) != disconnected_permissions_schema_hash)
            .collect();
        unreachable_schema_hashes.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));

        ConnectionSchemaDiagnostics {
            client_schema_hash,
            disconnected_permissions_schema_hash,
            unreachable_schema_hashes,
        }
    }

    pub fn are_schema_hashes_connected(&self, from_hash: SchemaHash, to_hash: SchemaHash) -> bool {
        self.non_draft_reachable_hashes(from_hash)
            .contains(&to_hash)
    }

    fn non_draft_reachable_hashes(&self, start_hash: SchemaHash) -> HashSet<SchemaHash> {
        if !self.is_schema_known(&start_hash) {
            return HashSet::new();
        }

        let mut reachable = HashSet::from([start_hash]);
        let mut queue = VecDeque::from([start_hash]);

        while let Some(current) = queue.pop_front() {
            for (&(source_hash, target_hash), lens) in &self.context.lenses {
                if lens.is_draft() {
                    continue;
                }

                let next_hash = if source_hash == current {
                    Some(target_hash)
                } else if target_hash == current {
                    Some(source_hash)
                } else {
                    None
                };

                if let Some(next_hash) = next_hash
                    && self.is_schema_known(&next_hash)
                    && reachable.insert(next_hash)
                {
                    queue.push_back(next_hash);
                }
            }
        }

        reachable
    }

    // =========================================================================
    // Multi-Schema Query Support
    // =========================================================================

    /// Get branch names as strings for direct-core query planning.
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

    /// Return the timestamp when a schema was published.
    ///
    /// Tracks the most recent publish timestamp observed by this manager.
    pub fn schema_published_at(&self, schema_hash: &SchemaHash) -> Option<u64> {
        let object_id = schema_hash.to_object_id();
        self.catalogue_publish_timestamps.get(&object_id).copied()
    }

    // =========================================================================
    // Catalogue Persistence
    // =========================================================================

    fn persist_catalogue_object_if_changed<H: SchemaManagerCatalogueStorage>(
        &mut self,
        storage: &mut H,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
        content: Vec<u8>,
    ) -> bool {
        if latest_catalogue_content_matches(storage, object_id, &content) {
            return false;
        }

        let timestamp = self.reserve_catalogue_timestamp();
        let mut metadata = metadata;
        metadata.insert(
            crate::metadata::MetadataKey::PublishedAt.to_string(),
            timestamp.to_string(),
        );
        self.catalogue_publish_timestamps
            .insert(object_id, timestamp);
        self.upsert_catalogue_entry(
            storage,
            CatalogueEntry {
                object_id,
                metadata,
                content,
            },
        );
        true
    }

    fn reserve_catalogue_timestamp(&mut self) -> u64 {
        self.catalogue_clock.reserve_timestamp()
    }

    fn upsert_catalogue_entry<H: SchemaManagerCatalogueStorage>(
        &mut self,
        storage: &mut H,
        entry: CatalogueEntry,
    ) {
        let existing = storage.load_catalogue_entry(entry.object_id).ok().flatten();
        if existing.as_ref() == Some(&entry) {
            return;
        }

        if let Err(error) = storage.upsert_catalogue_entry(&entry) {
            tracing::warn!(
                object_id = %entry.object_id,
                %error,
                "failed to persist schema catalogue entry"
            );
            return;
        }

        self.pending_catalogue_updates.push(entry);
    }

    pub fn ensure_current_schema_persisted<H: SchemaManagerCatalogueStorage>(
        &mut self,
        storage: &mut H,
    ) -> bool {
        if !self.context.is_initialized() {
            return false;
        }
        let schema_hash = self.context.current_hash;
        let storage_key = (storage.storage_cache_namespace(), schema_hash);
        if self
            .persisted_current_schema_in_storage
            .contains(&storage_key)
        {
            return false;
        }
        let object_id = schema_hash.to_object_id();
        let metadata = self.schema_metadata(&schema_hash);
        let content = encode_schema(&strip_schema_policies(&self.context.current_schema));

        let changed =
            self.persist_catalogue_object_if_changed(storage, object_id, metadata, content);
        self.persisted_current_schema_in_storage.insert(storage_key);
        changed
    }

    /// Persist the current schema to the catalogue as an Object.
    ///
    /// The schema is stored on the "main" branch with metadata identifying it
    /// as a catalogue schema for this app. Other clients with the same app_id
    /// will receive this via catalogue sync.
    ///
    /// Returns the ObjectId of the stored schema object.
    pub fn persist_schema<H: SchemaManagerCatalogueStorage>(
        &mut self,
        storage: &mut H,
    ) -> ObjectId {
        let schema_hash = self.context.current_hash;
        let object_id = schema_hash.to_object_id();
        let content = encode_schema(&strip_schema_policies(&self.context.current_schema));

        let timestamp = self.reserve_catalogue_timestamp();
        let metadata = self.schema_metadata_with_published_at(&schema_hash, timestamp);
        self.catalogue_publish_timestamps
            .insert(object_id, timestamp);
        self.upsert_catalogue_entry(
            storage,
            CatalogueEntry {
                object_id,
                metadata,
                content,
            },
        );

        object_id
    }

    /// Persist any schema to the catalogue as an Object.
    ///
    /// Used when seeding or syncing historical schema versions.
    pub fn persist_schema_object<H: SchemaManagerCatalogueStorage>(
        &mut self,
        storage: &mut H,
        schema: &Schema,
    ) -> ObjectId {
        let schema = strip_schema_policies(schema);
        let schema_hash = SchemaHash::compute(&schema);
        let object_id = schema_hash.to_object_id();
        let content = encode_schema(&schema);

        let timestamp = self.reserve_catalogue_timestamp();
        let metadata = self.schema_metadata_with_published_at(&schema_hash, timestamp);
        self.catalogue_publish_timestamps
            .insert(object_id, timestamp);
        self.upsert_catalogue_entry(
            storage,
            CatalogueEntry {
                object_id,
                metadata,
                content,
            },
        );

        object_id
    }

    /// Persist a lens to the catalogue as an Object.
    ///
    /// The lens is stored on the "main" branch with metadata identifying it
    /// as a catalogue lens for this app. Other clients with the same app_id
    /// will receive this via catalogue sync.
    ///
    /// Returns the ObjectId of the stored lens object.
    pub fn persist_lens<H: SchemaManagerCatalogueStorage>(
        &mut self,
        storage: &mut H,
        lens: &Lens,
    ) -> ObjectId {
        let object_id = lens.object_id();
        let content = encode_lens_transform(&lens.forward);

        let metadata = self.lens_metadata(lens);
        let timestamp = self.reserve_catalogue_timestamp();
        self.catalogue_publish_timestamps
            .insert(object_id, timestamp);
        self.upsert_catalogue_entry(
            storage,
            CatalogueEntry {
                object_id,
                metadata,
                content,
            },
        );

        object_id
    }

    pub fn persist_current_permissions<H: SchemaManagerCatalogueStorage>(
        &mut self,
        storage: &mut H,
    ) -> Option<ObjectId> {
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
        let bundle_timestamp = self.reserve_catalogue_timestamp();
        self.catalogue_publish_timestamps
            .insert(head.bundle_object_id, bundle_timestamp);
        self.upsert_catalogue_entry(
            storage,
            CatalogueEntry {
                object_id: head.bundle_object_id,
                metadata: bundle_metadata,
                content: bundle_content,
            },
        );

        let head_content = encode_permissions_head(
            head.schema_hash,
            head.version,
            head.parent_bundle_object_id,
            head.bundle_object_id,
        );
        self.persist_catalogue_object_if_changed(
            storage,
            head_object_id,
            head_metadata,
            head_content,
        );

        Some(head_object_id)
    }

    pub fn publish_permissions_bundle<H: SchemaManagerCatalogueStorage>(
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

        if let Some(head) = self.current_permissions_head
            && head.schema_hash == schema_hash
            && let Some(existing) = self.known_permissions_bundles.get(&head.bundle_object_id)
            && existing.permissions == permissions
        {
            return Ok(Some(self.permissions_head_object_id()));
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
    pub fn publish_lens<H: SchemaManagerCatalogueStorage>(
        &mut self,
        storage: &mut H,
        lens: &Lens,
    ) -> Result<ObjectId, SchemaError> {
        self.register_lens(lens.clone())?;
        self.activate_pending_schemas();
        Ok(self.persist_lens(storage, lens))
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

    fn schema_metadata_with_published_at(
        &self,
        schema_hash: &SchemaHash,
        published_at: u64,
    ) -> HashMap<String, String> {
        let mut metadata = self.schema_metadata(schema_hash);
        metadata.insert(
            crate::metadata::MetadataKey::PublishedAt.to_string(),
            published_at.to_string(),
        );
        metadata
    }

    fn note_catalogue_publish_timestamp(
        &mut self,
        object_id: ObjectId,
        metadata: &HashMap<String, String>,
    ) {
        let Some(timestamp) = metadata
            .get(crate::metadata::MetadataKey::PublishedAt.as_str())
            .and_then(|value| value.parse::<u64>().ok())
        else {
            return;
        };
        self.catalogue_publish_timestamps
            .entry(object_id)
            .and_modify(|existing| *existing = (*existing).max(timestamp))
            .or_insert(timestamp);
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
    /// Called when SyncManager receives an object with catalogue metadata matching this app_id.
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
                self.process_catalogue_schema(object_id, metadata, content)
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
        object_id: ObjectId,
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

        // An empty schema (zero tables) carries no structural information and
        // can only appear from legacy bugs that persisted the uninitialized
        // server context. Ignore it so stale entries don't surface as
        // "unreachable" hashes in connection diagnostics.
        if schema.is_empty() {
            return Ok(());
        }

        let hash = SchemaHash::compute(&schema);
        self.note_catalogue_publish_timestamp(object_id, metadata);

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
            self.activate_pending_schemas();
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
        // Defer flipping row_policy_mode to Enforcing until apply succeeds —
        // apply_permissions_head calls set_authorization_schema which sets it.
        // Flipping earlier denies writes against tables whose explicit policy
        // lives in the not-yet-arrived bundle.
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

        self.context.register_lens(lens);

        // Try to activate pending schemas that may now be reachable
        self.activate_pending_schemas();
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

        let _authorization_schema = merge_permissions_into_schema(&schema, &bundle.permissions);
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

    fn activate_pending_schemas(&mut self) {
        let _ = self.context.try_activate_pending();
    }

    /// Process pending catalogue operations published by this manager.
    pub fn process<H: SchemaManagerCatalogueStorage>(&mut self, storage: &mut H) {
        let _ = storage;
        let _span = tracing::debug_span!("SM::process").entered();

        let updates = std::mem::take(&mut self.pending_catalogue_updates);
        for update in updates {
            // Ignore errors from individual catalogue updates - they're non-critical
            let _ =
                self.process_catalogue_update(update.object_id, &update.metadata, &update.content);
        }

        if self.known_schemas_dirty {
            self.known_schemas_dirty = false;
        }

        // Final attempt to activate any remaining pending schemas
        self.activate_pending_schemas();
    }
}

fn latest_catalogue_content_matches<H: SchemaManagerCatalogueStorage + ?Sized>(
    storage: &H,
    object_id: ObjectId,
    expected: &[u8],
) -> bool {
    let latest_content = if let Ok(Some(content)) = latest_catalogue_content(storage, object_id) {
        content
    } else {
        return false;
    };
    latest_content == expected
}

#[allow(dead_code)]
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

fn schema_has_any_explicit_policies(schema: &Schema) -> bool {
    schema
        .values()
        .any(|table_schema| table_schema.policies != TablePolicies::default())
}

fn hash_len_prefixed(hasher: &mut Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
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
    use crate::schema_manager::catalogue_storage::tests::CatalogueMemoryStorage;

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
        let manager = SchemaManager::new(schema, test_app_id(), "dev", "main").unwrap();

        assert_eq!(manager.env(), "dev");
        assert_eq!(manager.user_branch(), "main");
        assert_eq!(manager.app_id(), test_app_id());
    }

    #[test]
    fn schema_manager_new_preserves_declared_table_column_order() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("name", ColumnType::Text)
                    .column("id", ColumnType::Uuid)
                    .nullable_column("email", ColumnType::Text),
            )
            .build();

        let manager = SchemaManager::new(schema, test_app_id(), "dev", "main").unwrap();

        let descriptor = manager.current_schema().get(&"users".into()).unwrap();
        let column_names: Vec<_> = descriptor
            .columns
            .columns
            .iter()
            .map(|column| column.name_str())
            .collect();

        assert_eq!(column_names, vec!["name", "id", "email"]);
    }

    #[test]
    fn schema_manager_new_hashes_reordered_schemas_differently() {
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

        let manager_a = SchemaManager::new(schema_a, test_app_id(), "dev", "main").unwrap();
        let manager_b = SchemaManager::new(schema_b, test_app_id(), "dev", "main").unwrap();

        assert_ne!(manager_a.current_hash(), manager_b.current_hash());
    }

    #[test]
    fn schema_manager_branch_name() {
        let schema = make_schema_v1();
        let manager = SchemaManager::new(schema, test_app_id(), "prod", "feature").unwrap();

        let branch = manager.branch_name();
        let s = branch.as_str();

        assert!(s.starts_with("prod-"));
        assert!(s.ends_with("-feature"));
    }

    #[test]
    fn schema_manager_add_live_schema() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();

        let mut manager = SchemaManager::new(v2, test_app_id(), "dev", "main").unwrap();
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

        let mut manager = SchemaManager::new(v2, test_app_id(), "dev", "main").unwrap();
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

        let mut manager = SchemaManager::new(v2, test_app_id(), "dev", "main").unwrap();
        manager.add_live_schema_with_lens(v1, lens).unwrap();

        assert_eq!(manager.all_branches().len(), 2);
    }

    #[test]
    fn schema_manager_validate() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();

        let mut manager = SchemaManager::new(v2, test_app_id(), "dev", "main").unwrap();
        manager.add_live_schema(v1).unwrap();

        // Should pass - no draft lenses
        assert!(manager.validate().is_ok());
    }

    #[test]
    fn schema_manager_lens_path() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v1_hash = SchemaHash::compute(&v1);

        let mut manager = SchemaManager::new(v2, test_app_id(), "dev", "main").unwrap();
        manager.add_live_schema(v1).unwrap();

        let path = manager.lens_path(&v1_hash).unwrap();
        assert_eq!(path.len(), 1);
    }

    #[test]
    fn schema_manager_generate_lens_without_register() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();

        let manager = SchemaManager::new(v2.clone(), test_app_id(), "dev", "main").unwrap();
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

        let mut manager = SchemaManager::new(v2, test_app_id(), "dev", "main").unwrap();
        manager.add_live_schema(v1).unwrap();

        let map = manager.branch_schema_map();
        assert_eq!(map.len(), 2);

        // Should contain both schema hashes
        let hashes: std::collections::HashSet<_> = map.values().collect();
        assert!(hashes.contains(&v1_hash));
        assert!(hashes.contains(&v2_hash));
    }

    #[test]
    fn schema_manager_schema_published_at_uses_latest_catalogue_commit_timestamp() {
        let schema = make_schema_v1();
        let schema_hash = SchemaHash::compute(&schema);
        let mut storage = CatalogueMemoryStorage::new();
        let mut manager = SchemaManager::new(schema.clone(), test_app_id(), "dev", "main").unwrap();

        assert_eq!(manager.schema_published_at(&schema_hash), None);

        manager.persist_schema_object(&mut storage, &schema);
        let first_timestamp = manager
            .schema_published_at(&schema_hash)
            .expect("schema should expose publish timestamp after first persist");

        manager.persist_schema_object(&mut storage, &schema);
        let second_timestamp = manager
            .schema_published_at(&schema_hash)
            .expect("schema should expose publish timestamp after republish");

        assert!(
            second_timestamp > first_timestamp,
            "republishing the same schema should advance the visible publish timestamp"
        );
    }

    #[test]
    fn schema_manager_rehydrates_schema_published_at_from_catalogue_metadata() {
        let schema = make_schema_v1();
        let schema_hash = SchemaHash::compute(&schema);
        let mut storage = CatalogueMemoryStorage::new();
        let app_id = test_app_id();
        let mut publisher = SchemaManager::new(schema.clone(), app_id, "dev", "main").unwrap();

        publisher.persist_schema_object(&mut storage, &schema);
        let published_at = publisher
            .schema_published_at(&schema_hash)
            .expect("publisher should track publish timestamp");

        let mut rehydrated = SchemaManager::new_server(app_id, "prod");
        crate::schema_manager::rehydrate::rehydrate_schema_manager_from_catalogue(
            &mut rehydrated,
            &storage,
            app_id,
        )
        .expect("rehydrate catalogue");

        assert_eq!(
            rehydrated.schema_published_at(&schema_hash),
            Some(published_at)
        );
    }

    #[test]
    fn schema_manager_all_branch_strings() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();

        let mut manager = SchemaManager::new(v2, test_app_id(), "dev", "main").unwrap();
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

        let mut manager = SchemaManager::new(v2, test_app_id(), "dev", "main").unwrap();
        manager.add_live_schema(v1).unwrap();

        // V1 has 2 columns (id, name)
        let v1_desc = manager.get_table_descriptor("users", &v1_hash).unwrap();
        assert_eq!(v1_desc.columns.len(), 2);

        // V2 has 3 columns (id, name, email)
        let v2_desc = manager.get_table_descriptor("users", &v2_hash).unwrap();
        assert_eq!(v2_desc.columns.len(), 3);
    }

    #[test]
    fn server_dynamic_mode_does_not_persist_empty_placeholder_schema() {
        // Regression from the deleted alpha runtime constructor, which called
        // ensure_current_schema_persisted on every runtime construction. On a
        // dynamic-schema server built with
        // SchemaManager::new_server(...), the context has an uninitialized
        // sentinel hash ([0; 32]) and an empty Schema. Persisting that writes
        // a bogus catalogue_schema row whose content hashes to BLAKE3("") =
        // af1349b9f5f9..., which later appears as an "unreachable schema hash"
        // in every connection diagnostics call.
        let mut storage = CatalogueMemoryStorage::new();
        let mut manager = SchemaManager::new_server(test_app_id(), "prod");
        let wrote = manager.ensure_current_schema_persisted(&mut storage);
        assert!(
            !wrote,
            "dynamic server with no current schema must not persist a placeholder entry"
        );
        let entries = storage.scan_catalogue_entries().unwrap();
        assert!(
            entries.is_empty(),
            "no catalogue entries should be written for an uninitialized schema context, got: {:?}",
            entries.iter().map(|e| &e.metadata).collect::<Vec<_>>()
        );
    }

    #[test]
    fn process_catalogue_schema_ignores_empty_schema_rows_from_legacy_bug() {
        // Defensive: sqlite files written by a pre-fix server may contain a
        // bogus catalogue_schema row whose content encodes an empty Schema
        // (the uninitialized sentinel). On rehydrate that empty schema would
        // hash to BLAKE3("") = af1349b9f5f9... and surface as an unreachable
        // hash in every client's diagnostics. Ignore empty schemas.
        let schema = make_schema_v1();
        let real_hash = SchemaHash::compute(&schema);
        let mut manager = SchemaManager::new(schema, test_app_id(), "dev", "main").unwrap();

        let empty_content = crate::schema_manager::encoding::encode_schema(&Schema::new());
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Type.to_string(),
            crate::metadata::ObjectType::CatalogueSchema.to_string(),
        );
        metadata.insert(
            crate::metadata::MetadataKey::AppId.to_string(),
            test_app_id().uuid().to_string(),
        );
        metadata.insert(
            crate::metadata::MetadataKey::SchemaHash.to_string(),
            SchemaHash::from_bytes([0; 32]).to_string(),
        );
        let sentinel_object_id = SchemaHash::from_bytes([0; 32]).to_object_id();

        manager
            .process_catalogue_update(sentinel_object_id, &metadata, &empty_content)
            .unwrap();

        let empty_hash = SchemaHash::compute(&Schema::new());
        let known: std::collections::HashSet<_> =
            manager.known_schema_hashes().into_iter().collect();
        assert!(
            !known.contains(&empty_hash),
            "empty-schema hash must not be registered in known_schemas"
        );
        assert!(known.contains(&real_hash));
    }

    #[test]
    fn connection_schema_diagnostics_treat_unknown_client_schema_as_disconnected() {
        let schema = make_schema_v2();
        let current_hash = SchemaHash::compute(&schema);
        let manager = SchemaManager::new(schema, test_app_id(), "dev", "main").unwrap();

        let diagnostics = manager.connection_schema_diagnostics(SchemaHash::from_bytes([7; 32]));

        assert_eq!(
            diagnostics.client_schema_hash,
            SchemaHash::from_bytes([7; 32])
        );
        assert_eq!(
            diagnostics.disconnected_permissions_schema_hash,
            Some(current_hash)
        );
        assert!(diagnostics.unreachable_schema_hashes.is_empty());
    }

    #[test]
    fn connection_schema_diagnostics_reports_other_unreachable_server_schemas() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v3 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .nullable_column("email", ColumnType::Text)
                    .nullable_column("nickname", ColumnType::Text),
            )
            .build();
        let v1_hash = SchemaHash::compute(&v1);
        let v3_hash = SchemaHash::compute(&v3);

        let mut manager = SchemaManager::new(v2, test_app_id(), "dev", "main").unwrap();
        manager.add_live_schema(v1).unwrap();
        manager.add_known_schema(v3);

        let diagnostics = manager.connection_schema_diagnostics(v1_hash);

        assert_eq!(diagnostics.disconnected_permissions_schema_hash, None);
        assert_eq!(diagnostics.unreachable_schema_hashes, vec![v3_hash]);
    }

    #[test]
    fn connection_schema_diagnostics_ignore_draft_lenses() {
        let v1 = make_schema_v1();
        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .column("org_id", ColumnType::Uuid),
            )
            .build();
        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);
        let draft_lens = generate_lens(&v1, &v2);

        assert!(draft_lens.is_draft());

        let mut manager = SchemaManager::new(v2, test_app_id(), "dev", "main").unwrap();
        manager.add_known_schema(v1);
        manager.context.register_lens(draft_lens);

        let diagnostics = manager.connection_schema_diagnostics(v1_hash);

        assert_eq!(
            diagnostics.disconnected_permissions_schema_hash,
            Some(v2_hash)
        );
        assert!(diagnostics.unreachable_schema_hashes.is_empty());
    }

    #[test]
    fn schema_hash_connectivity_requires_non_draft_uploaded_lenses() {
        let v1 = make_schema_v1();
        let v1_hash = SchemaHash::compute(&v1);
        let draft_target = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .column("org_id", ColumnType::Uuid),
            )
            .build();
        let draft_target_hash = SchemaHash::compute(&draft_target);
        let draft_lens = generate_lens(&v1, &draft_target);

        assert!(draft_lens.is_draft());

        let mut disconnected =
            SchemaManager::new(draft_target, test_app_id(), "dev", "main").unwrap();
        disconnected.add_known_schema(v1.clone());
        disconnected.context.register_lens(draft_lens);
        assert!(!disconnected.are_schema_hashes_connected(v1_hash, draft_target_hash));

        let live_target = make_schema_v2();
        let live_target_hash = SchemaHash::compute(&live_target);
        let mut connected = SchemaManager::new(live_target, test_app_id(), "dev", "main").unwrap();
        connected.add_live_schema(v1).unwrap();
        assert!(connected.are_schema_hashes_connected(v1_hash, live_target_hash));
    }

    #[test]
    fn permissions_head_waits_for_bundle_then_applies() {
        let schema = make_schema_v2();
        let schema_hash = SchemaHash::compute(&schema);
        let mut manager = SchemaManager::new(schema, test_app_id(), "dev", "main").unwrap();
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
    fn repersisting_rehydrated_permissions_keeps_unchanged_entries_stable() {
        let app_id = test_app_id();
        let schema = make_schema_v2();
        let schema_hash = SchemaHash::compute(&schema);
        let permissions = HashMap::from([(
            TableName::new("users"),
            TablePolicies::new().with_select(PolicyExpr::True),
        )]);

        let mut storage = CatalogueMemoryStorage::new();
        let mut previous_run = SchemaManager::new(schema.clone(), app_id, "dev", "main").unwrap();
        previous_run.persist_schema(&mut storage);
        previous_run
            .publish_permissions_bundle(&mut storage, schema_hash, permissions, None)
            .expect("previous run should publish permissions");
        previous_run.process(&mut storage);

        let head_object_id = SchemaManager::permissions_head_object_id_for(app_id);
        let head_entry_before = storage
            .load_catalogue_entry(head_object_id)
            .expect("head entry should load")
            .expect("head entry should exist");
        let bundle_entry_before = storage
            .load_catalogue_entry(
                previous_run
                    .current_permissions_head
                    .expect("permissions head should exist")
                    .bundle_object_id,
            )
            .expect("bundle entry should load")
            .expect("bundle entry should exist");

        let mut restarted = SchemaManager::new(schema, app_id, "dev", "main").unwrap();
        crate::schema_manager::rehydrate_schema_manager_from_catalogue(
            &mut restarted,
            &storage,
            app_id,
        )
        .expect("rehydrate should succeed");

        let republished_head_object_id = restarted
            .persist_current_permissions(&mut storage)
            .expect("rehydrated permissions should republish");
        assert_eq!(republished_head_object_id, head_object_id);

        let head_entry_after = storage
            .load_catalogue_entry(head_object_id)
            .expect("head entry should load after materialize")
            .expect("head entry should still exist");
        let bundle_entry_after = storage
            .load_catalogue_entry(
                restarted
                    .current_permissions_head
                    .expect("rehydrated permissions head should exist")
                    .bundle_object_id,
            )
            .expect("bundle entry should load after materialize")
            .expect("bundle entry should still exist");
        assert_eq!(
            head_entry_after, head_entry_before,
            "materializing unchanged permissions head should not rewrite the stored head entry"
        );
        assert_eq!(
            bundle_entry_after, bundle_entry_before,
            "materializing unchanged permissions head should not rewrite the stored bundle entry"
        );
    }

    #[test]
    fn publish_permissions_bundle_rejects_stale_parent() {
        let schema = make_schema_v2();
        let schema_hash = SchemaHash::compute(&schema);
        let mut manager = SchemaManager::new(schema, test_app_id(), "dev", "main").unwrap();
        let mut storage = CatalogueMemoryStorage::new();
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
    fn republishing_identical_permissions_is_a_no_op() {
        let schema = make_schema_v2();
        let schema_hash = SchemaHash::compute(&schema);
        let mut manager = SchemaManager::new(schema, test_app_id(), "dev", "main").unwrap();
        let mut storage = CatalogueMemoryStorage::new();
        let permissions = HashMap::from([(
            TableName::new("users"),
            TablePolicies::new().with_select(PolicyExpr::True),
        )]);

        manager
            .publish_permissions_bundle(&mut storage, schema_hash, permissions.clone(), None)
            .expect("initial permissions publish should succeed");
        manager.process(&mut storage);

        let head_before = manager
            .current_permissions_head()
            .expect("head should exist after first publish");
        let bundle_entry_before = storage
            .load_catalogue_entry(head_before.bundle_object_id)
            .expect("bundle entry should load")
            .expect("bundle entry should exist");
        let head_object_id = SchemaManager::permissions_head_object_id_for(test_app_id());
        let head_entry_before = storage
            .load_catalogue_entry(head_object_id)
            .expect("head entry should load")
            .expect("head entry should exist");

        let republished = manager
            .publish_permissions_bundle(
                &mut storage,
                schema_hash,
                permissions,
                Some(head_before.bundle_object_id),
            )
            .expect("republishing identical permissions should succeed");

        assert_eq!(
            republished,
            Some(head_object_id),
            "republish should return the existing head object id"
        );

        let head_after = manager
            .current_permissions_head()
            .expect("head should still exist after no-op publish");
        assert_eq!(
            head_after.version, head_before.version,
            "no-op publish must not bump the version"
        );
        assert_eq!(
            head_after.bundle_object_id, head_before.bundle_object_id,
            "no-op publish must not produce a new bundle object id"
        );
        assert_eq!(
            head_after.parent_bundle_object_id, head_before.parent_bundle_object_id,
            "no-op publish must not change the parent link"
        );

        let bundle_entry_after = storage
            .load_catalogue_entry(head_after.bundle_object_id)
            .expect("bundle entry should load after no-op publish")
            .expect("bundle entry should still exist");
        let head_entry_after = storage
            .load_catalogue_entry(head_object_id)
            .expect("head entry should load after no-op publish")
            .expect("head entry should still exist");
        assert_eq!(
            bundle_entry_after, bundle_entry_before,
            "no-op publish must not rewrite the stored bundle entry"
        );
        assert_eq!(
            head_entry_after, head_entry_before,
            "no-op publish must not rewrite the stored head entry"
        );
    }

    #[test]
    fn republishing_changed_permissions_bumps_version() {
        let schema = make_schema_v2();
        let schema_hash = SchemaHash::compute(&schema);
        let mut manager = SchemaManager::new(schema, test_app_id(), "dev", "main").unwrap();
        let mut storage = CatalogueMemoryStorage::new();
        let permissive = HashMap::from([(
            TableName::new("users"),
            TablePolicies::new().with_select(PolicyExpr::True),
        )]);
        let restrictive = HashMap::from([(
            TableName::new("users"),
            TablePolicies::new().with_select(PolicyExpr::False),
        )]);

        manager
            .publish_permissions_bundle(&mut storage, schema_hash, permissive, None)
            .expect("initial permissions publish should succeed");
        let head_before = manager
            .current_permissions_head()
            .expect("head should exist after first publish");

        manager
            .publish_permissions_bundle(
                &mut storage,
                schema_hash,
                restrictive,
                Some(head_before.bundle_object_id),
            )
            .expect("changed publish should succeed");
        let head_after = manager
            .current_permissions_head()
            .expect("head should exist after changed publish");

        assert_eq!(
            head_after.version,
            head_before.version + 1,
            "changed permissions must bump the version"
        );
        assert_ne!(
            head_after.bundle_object_id, head_before.bundle_object_id,
            "changed permissions must produce a new bundle object id"
        );
        assert_eq!(
            head_after.parent_bundle_object_id,
            Some(head_before.bundle_object_id),
            "changed permissions must chain off the previous bundle"
        );
    }

    #[test]
    fn dedup_preserves_parent_chain_for_subsequent_change() {
        let schema = make_schema_v2();
        let schema_hash = SchemaHash::compute(&schema);
        let mut manager = SchemaManager::new(schema, test_app_id(), "dev", "main").unwrap();
        let mut storage = CatalogueMemoryStorage::new();
        let permissive = HashMap::from([(
            TableName::new("users"),
            TablePolicies::new().with_select(PolicyExpr::True),
        )]);
        let restrictive = HashMap::from([(
            TableName::new("users"),
            TablePolicies::new().with_select(PolicyExpr::False),
        )]);

        manager
            .publish_permissions_bundle(&mut storage, schema_hash, permissive.clone(), None)
            .expect("initial publish should succeed");
        let head_a = manager
            .current_permissions_head()
            .expect("head should exist after publish A");

        manager
            .publish_permissions_bundle(
                &mut storage,
                schema_hash,
                permissive,
                Some(head_a.bundle_object_id),
            )
            .expect("identical republish should succeed");
        let head_a_again = manager
            .current_permissions_head()
            .expect("head should exist after no-op republish");
        assert_eq!(
            head_a_again, head_a,
            "no-op republish must leave the head untouched"
        );

        manager
            .publish_permissions_bundle(
                &mut storage,
                schema_hash,
                restrictive,
                Some(head_a.bundle_object_id),
            )
            .expect("subsequent changed publish should succeed");
        let head_b = manager
            .current_permissions_head()
            .expect("head should exist after publish B");

        assert_eq!(
            head_b.version,
            head_a.version + 1,
            "version must advance by one across no-op + change, not two"
        );
        assert_eq!(
            head_b.parent_bundle_object_id,
            Some(head_a.bundle_object_id),
            "B must chain off A, not off the skipped no-op"
        );
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

        let mut manager = SchemaManager::new(v2, test_app_id(), "dev", "main").unwrap();
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
}
