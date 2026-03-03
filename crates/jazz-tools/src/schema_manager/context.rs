//! Schema Context - Tracks current schema and live schema versions.
//!
//! SchemaContext holds the current schema, environment, and tracks
//! which other schema versions are "live" (reachable via lenses).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::object::BranchName;
use crate::query_manager::types::{ComposedBranchName, Schema, SchemaHash};

use super::lens::Lens;

/// Schema context for a query operation.
///
/// On client: constructed once from app's schema.
/// On server: constructed per-query from client-provided params.
///
/// This is the minimal information needed to execute a query against
/// a specific schema version. Servers use this to handle multi-tenant
/// queries where each client may have a different schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuerySchemaContext {
    /// Environment (e.g., "dev", "prod").
    pub env: String,
    /// Hash of the target schema for this query.
    pub schema_hash: SchemaHash,
    /// User-facing branch name (e.g., "main", "feature-x").
    pub user_branch: String,
}

impl QuerySchemaContext {
    /// Create a new query schema context.
    pub fn new(
        env: impl Into<String>,
        schema_hash: SchemaHash,
        user_branch: impl Into<String>,
    ) -> Self {
        Self {
            env: env.into(),
            schema_hash,
            user_branch: user_branch.into(),
        }
    }

    /// Get the composed branch name for this context.
    pub fn branch_name(&self) -> ComposedBranchName {
        ComposedBranchName::new(&self.env, self.schema_hash, &self.user_branch)
    }
}

/// Error type for schema context operations.
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaError {
    /// Draft lens found in path to live schema.
    DraftLensInPath {
        source: SchemaHash,
        target: SchemaHash,
    },
    /// No lens path exists between schemas.
    NoLensPath {
        source: SchemaHash,
        target: SchemaHash,
    },
    /// Schema not found in context.
    SchemaNotFound(SchemaHash),
    /// Catalogue schema metadata missing `schema_json`.
    MissingSchemaJson,
    /// Lens not found.
    LensNotFound {
        source: SchemaHash,
        target: SchemaHash,
    },
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaError::DraftLensInPath { source, target } => {
                write!(
                    f,
                    "Draft lens in path from {} to {}",
                    source.short(),
                    target.short()
                )
            }
            SchemaError::NoLensPath { source, target } => {
                write!(
                    f,
                    "No lens path from {} to {}",
                    source.short(),
                    target.short()
                )
            }
            SchemaError::SchemaNotFound(hash) => {
                write!(f, "Schema not found: {}", hash.short())
            }
            SchemaError::MissingSchemaJson => {
                write!(f, "Catalogue schema metadata missing schema_json")
            }
            SchemaError::LensNotFound { source, target } => {
                write!(
                    f,
                    "Lens not found: {} -> {}",
                    source.short(),
                    target.short()
                )
            }
        }
    }
}

impl std::error::Error for SchemaError {}

/// Context describing the current schema and related live schemas.
#[derive(Debug, Clone)]
pub struct SchemaContext {
    /// The current schema being used for queries.
    pub current_schema: Schema,
    /// Hash of the current schema.
    pub current_hash: SchemaHash,
    /// Environment (e.g., "dev", "prod").
    pub env: String,
    /// User-facing branch name (e.g., "main", "feature-x").
    pub user_branch: String,

    /// Other live schemas reachable via lenses (hash -> schema).
    pub live_schemas: HashMap<SchemaHash, Schema>,
    /// Registered lenses between schema versions ((source, target) -> lens).
    pub lenses: HashMap<(SchemaHash, SchemaHash), Lens>,

    /// Schemas received via catalogue sync but not yet live (awaiting lens path).
    /// These become live when a lens path to current_schema becomes available.
    pub pending_schemas: HashMap<SchemaHash, Schema>,

    /// Whether set_current() has been called (guards against double initialization).
    is_initialized: bool,
}

impl SchemaContext {
    /// Create an empty context (no current schema yet).
    ///
    /// Call `set_current()` to initialize the current schema before use.
    pub fn empty() -> Self {
        Self {
            current_schema: Schema::new(),
            current_hash: SchemaHash::from_bytes([0; 32]), // Sentinel value
            env: String::new(),
            user_branch: String::new(),
            live_schemas: HashMap::new(),
            lenses: HashMap::new(),
            pending_schemas: HashMap::new(),
            is_initialized: false,
        }
    }

    /// Create a new context with only the current schema (no live schemas).
    pub fn new(schema: Schema, env: &str, user_branch: &str) -> Self {
        let hash = SchemaHash::compute(&schema);
        Self {
            current_schema: schema,
            current_hash: hash,
            env: env.to_string(),
            user_branch: user_branch.to_string(),
            live_schemas: HashMap::new(),
            lenses: HashMap::new(),
            pending_schemas: HashMap::new(),
            is_initialized: true,
        }
    }

    /// Create with default environment ("dev").
    pub fn with_defaults(schema: Schema, user_branch: &str) -> Self {
        Self::new(schema, "dev", user_branch)
    }

    /// Set the current schema (can only be called once).
    ///
    /// # Panics
    /// Panics if called more than once.
    pub fn set_current(&mut self, schema: Schema, env: &str, user_branch: &str) {
        assert!(
            !self.is_initialized,
            "set_current() called on already-initialized SchemaContext"
        );
        self.current_schema = schema;
        self.current_hash = SchemaHash::compute(&self.current_schema);
        self.env = env.to_string();
        self.user_branch = user_branch.to_string();
        self.is_initialized = true;
    }

    /// Check if this context has been initialized with a current schema.
    pub fn is_initialized(&self) -> bool {
        self.is_initialized
    }

    /// Get the composed branch name for the current schema.
    pub fn branch_name(&self) -> BranchName {
        ComposedBranchName::new(&self.env, self.current_hash, &self.user_branch).to_branch_name()
    }

    /// Get branch names for all live schemas (current + live).
    pub fn all_branch_names(&self) -> Vec<BranchName> {
        let mut names = vec![self.branch_name()];
        for hash in self.live_schemas.keys() {
            names.push(
                ComposedBranchName::new(&self.env, *hash, &self.user_branch).to_branch_name(),
            );
        }
        names
    }

    /// Add a live schema with its lens to the current schema.
    pub fn add_live_schema(&mut self, schema: Schema, lens: Lens) {
        let hash = SchemaHash::compute(&schema);
        self.live_schemas.insert(hash, schema);
        self.lenses
            .insert((lens.source_hash, lens.target_hash), lens);
    }

    /// Register a lens between two schemas.
    pub fn register_lens(&mut self, lens: Lens) {
        self.lenses
            .insert((lens.source_hash, lens.target_hash), lens);
    }

    /// Get lens between two schemas if it exists.
    pub fn get_lens(&self, source: &SchemaHash, target: &SchemaHash) -> Option<&Lens> {
        self.lenses.get(&(*source, *target))
    }

    /// Find the lens path from a source schema to the current schema.
    /// Returns the sequence of (lens, direction) pairs to apply (in order).
    ///
    /// This traverses both forward and backward lens directions:
    /// - Forward: lens(A→B) allows A to reach B (use lens.forward transform)
    /// - Backward: lens(A→B) also allows B to reach A (use lens.backward transform)
    ///
    /// The returned path contains lenses paired with the direction to apply.
    pub fn lens_path(
        &self,
        from: &SchemaHash,
    ) -> Result<Vec<(&Lens, super::lens::Direction)>, SchemaError> {
        use super::lens::Direction;

        if from == &self.current_hash {
            return Ok(Vec::new()); // Already at current
        }

        // BFS to find path, considering both forward and backward lens directions
        let mut visited = std::collections::HashSet::new();
        let mut queue = std::collections::VecDeque::new();
        // Parent map: target -> (previous_node, lens, direction)
        let mut parent: HashMap<SchemaHash, (SchemaHash, &Lens, Direction)> = HashMap::new();

        queue.push_back(*from);
        visited.insert(*from);

        while let Some(current) = queue.pop_front() {
            // Check all lenses - both forward and backward directions
            for ((source, target), lens) in &self.lenses {
                // Forward direction: source -> target
                if source == &current && !visited.contains(target) {
                    visited.insert(*target);
                    parent.insert(*target, (current, lens, Direction::Forward));
                    queue.push_back(*target);

                    if target == &self.current_hash {
                        return self.reconstruct_path(&parent);
                    }
                }
                // Backward direction: target -> source (using backward transform)
                if target == &current && !visited.contains(source) {
                    visited.insert(*source);
                    parent.insert(*source, (current, lens, Direction::Backward));
                    queue.push_back(*source);

                    if source == &self.current_hash {
                        return self.reconstruct_path(&parent);
                    }
                }
            }
        }

        Err(SchemaError::NoLensPath {
            source: *from,
            target: self.current_hash,
        })
    }

    /// Reconstruct the lens path from BFS parent map.
    fn reconstruct_path<'a>(
        &'a self,
        parent: &HashMap<SchemaHash, (SchemaHash, &'a Lens, super::lens::Direction)>,
    ) -> Result<Vec<(&'a Lens, super::lens::Direction)>, SchemaError> {
        let mut path = Vec::new();
        let mut curr = self.current_hash;
        while let Some((prev, lens, direction)) = parent.get(&curr) {
            path.push((*lens, *direction));
            curr = *prev;
        }
        path.reverse();
        Ok(path)
    }

    /// Validate the context: ensure no draft lenses in paths to live schemas.
    pub fn validate(&self) -> Result<(), SchemaError> {
        for live_hash in self.live_schemas.keys() {
            let path = self.lens_path(live_hash)?;
            for (lens, _direction) in path {
                if lens.is_draft() {
                    return Err(SchemaError::DraftLensInPath {
                        source: lens.source_hash,
                        target: lens.target_hash,
                    });
                }
            }
        }
        Ok(())
    }

    /// Get schema by hash (current or live).
    pub fn get_schema(&self, hash: &SchemaHash) -> Option<&Schema> {
        if hash == &self.current_hash {
            Some(&self.current_schema)
        } else {
            self.live_schemas.get(hash)
        }
    }

    /// Check if a schema is live (current or in live_schemas).
    pub fn is_live(&self, hash: &SchemaHash) -> bool {
        hash == &self.current_hash || self.live_schemas.contains_key(hash)
    }

    /// Get all live schema hashes (current + live).
    pub fn all_live_hashes(&self) -> Vec<SchemaHash> {
        let mut hashes = vec![self.current_hash];
        hashes.extend(self.live_schemas.keys().copied());
        hashes
    }

    /// Add a schema to the pending set (awaiting lens path).
    ///
    /// The schema will become live once a lens path to current_schema is available.
    pub fn add_pending_schema(&mut self, schema: Schema) {
        let hash = SchemaHash::compute(&schema);
        // Don't add if already live or current
        if !self.is_live(&hash) {
            self.pending_schemas.insert(hash, schema);
        }
    }

    /// Check if a schema is pending activation.
    pub fn is_pending(&self, hash: &SchemaHash) -> bool {
        self.pending_schemas.contains_key(hash)
    }

    /// Try to activate pending schemas that now have lens paths to current.
    ///
    /// Returns the hashes of newly activated schemas.
    /// Should be called after registering new lenses.
    pub fn try_activate_pending(&mut self) -> Vec<SchemaHash> {
        let mut activated = Vec::new();
        let pending_hashes: Vec<_> = self.pending_schemas.keys().copied().collect();

        for hash in pending_hashes {
            // Check if we can now reach current from this pending schema
            if self.lens_path(&hash).is_ok() {
                // Move from pending to live
                if let Some(schema) = self.pending_schemas.remove(&hash) {
                    self.live_schemas.insert(hash, schema);
                    activated.push(hash);
                }
            }
        }

        activated
    }

    /// Get pending schema by hash.
    pub fn get_pending_schema(&self, hash: &SchemaHash) -> Option<&Schema> {
        self.pending_schemas.get(hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::types::{ColumnType, SchemaBuilder, TableSchema};
    use crate::schema_manager::auto_lens::generate_lens;

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
    fn context_branch_name() {
        let schema = make_schema_v1();
        let ctx = SchemaContext::new(schema, "dev", "main");

        let branch = ctx.branch_name();
        let s = branch.as_str();

        assert!(s.starts_with("dev-"));
        assert!(s.ends_with("-main"));
    }

    #[test]
    fn context_with_defaults() {
        let schema = make_schema_v1();
        let ctx = SchemaContext::with_defaults(schema, "feature-x");

        assert_eq!(ctx.env, "dev");
        assert_eq!(ctx.user_branch, "feature-x");
    }

    #[test]
    fn context_all_branch_names() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let lens = generate_lens(&v1, &v2);

        let mut ctx = SchemaContext::new(v2, "prod", "main");
        ctx.add_live_schema(v1, lens);

        let names = ctx.all_branch_names();
        assert_eq!(names.len(), 2);
    }

    #[test]
    fn context_lens_path_direct() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v1_hash = SchemaHash::compute(&v1);
        let lens = generate_lens(&v1, &v2);

        let mut ctx = SchemaContext::new(v2, "dev", "main");
        ctx.add_live_schema(v1, lens);

        let path = ctx.lens_path(&v1_hash).unwrap();
        assert_eq!(path.len(), 1);
        assert_eq!(path[0].0.source_hash, v1_hash);
    }

    #[test]
    fn context_lens_path_current() {
        let v2 = make_schema_v2();
        let v2_hash = SchemaHash::compute(&v2);

        let ctx = SchemaContext::new(v2, "dev", "main");

        let path = ctx.lens_path(&v2_hash).unwrap();
        assert!(path.is_empty()); // Already at current
    }

    #[test]
    fn context_lens_path_not_found() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v1_hash = SchemaHash::compute(&v1);

        // No lens registered
        let ctx = SchemaContext::new(v2, "dev", "main");

        let result = ctx.lens_path(&v1_hash);
        assert!(matches!(result, Err(SchemaError::NoLensPath { .. })));
    }

    #[test]
    fn context_validate_no_drafts() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let lens = generate_lens(&v1, &v2);

        let mut ctx = SchemaContext::new(v2, "dev", "main");
        ctx.add_live_schema(v1, lens);

        // This lens is not draft (add nullable column with Null default)
        assert!(ctx.validate().is_ok());
    }

    #[test]
    fn context_validate_draft_lens_fails() {
        let v1 = make_schema_v1();
        // Add non-nullable UUID column - will create draft lens
        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .column("org_id", ColumnType::Uuid), // non-nullable UUID = draft
            )
            .build();
        let lens = generate_lens(&v1, &v2);
        assert!(lens.is_draft());

        let mut ctx = SchemaContext::new(v2, "dev", "main");
        ctx.add_live_schema(v1, lens);

        let result = ctx.validate();
        assert!(matches!(result, Err(SchemaError::DraftLensInPath { .. })));
    }

    #[test]
    fn context_is_live() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);
        let lens = generate_lens(&v1, &v2);

        let mut ctx = SchemaContext::new(v2, "dev", "main");
        ctx.add_live_schema(v1, lens);

        assert!(ctx.is_live(&v2_hash));
        assert!(ctx.is_live(&v1_hash));

        let other_hash = SchemaHash::from_bytes([99; 32]);
        assert!(!ctx.is_live(&other_hash));
    }

    #[test]
    fn context_all_live_hashes() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);
        let lens = generate_lens(&v1, &v2);

        let mut ctx = SchemaContext::new(v2, "dev", "main");
        ctx.add_live_schema(v1, lens);

        let hashes = ctx.all_live_hashes();
        assert_eq!(hashes.len(), 2);
        assert!(hashes.contains(&v1_hash));
        assert!(hashes.contains(&v2_hash));
    }

    // ========================================================================
    // Multi-Hop Lens Path Tests
    // ========================================================================

    fn make_schema_v3() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .nullable_column("email", ColumnType::Text)
                    .nullable_column("role", ColumnType::Text),
            )
            .build()
    }

    #[test]
    fn context_lens_path_multi_hop() {
        // v1 -> v2 -> v3 (current)
        let v1 = make_schema_v1(); // users(id, name)
        let v2 = make_schema_v2(); // users(id, name, email)
        let v3 = make_schema_v3(); // users(id, name, email, role)

        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        let lens_v1_v2 = generate_lens(&v1, &v2);
        let lens_v2_v3 = generate_lens(&v2, &v3);

        let mut ctx = SchemaContext::new(v3, "dev", "main");
        // Register intermediate schema and its lens to current
        ctx.add_live_schema(v2.clone(), lens_v2_v3);
        // Register oldest schema and its lens to intermediate
        ctx.add_live_schema(v1, lens_v1_v2);

        // Path from v1 to current (v3) should have 2 hops
        let path = ctx.lens_path(&v1_hash).unwrap();
        assert_eq!(path.len(), 2);

        // First hop: v1 -> v2
        assert_eq!(path[0].0.source_hash, v1_hash);
        assert_eq!(path[0].0.target_hash, v2_hash);

        // Second hop: v2 -> v3
        assert_eq!(path[1].0.source_hash, v2_hash);
        assert_eq!(path[1].0.target_hash, ctx.current_hash);
    }

    #[test]
    fn context_lens_path_multi_hop_from_middle() {
        // Path from v2 (middle) to v3 (current) should be 1 hop
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v3 = make_schema_v3();

        let v2_hash = SchemaHash::compute(&v2);

        let lens_v1_v2 = generate_lens(&v1, &v2);
        let lens_v2_v3 = generate_lens(&v2, &v3);

        let mut ctx = SchemaContext::new(v3, "dev", "main");
        ctx.add_live_schema(v2.clone(), lens_v2_v3);
        ctx.add_live_schema(v1, lens_v1_v2);

        let path = ctx.lens_path(&v2_hash).unwrap();
        assert_eq!(path.len(), 1);
        assert_eq!(path[0].0.source_hash, v2_hash);
        assert_eq!(path[0].0.target_hash, ctx.current_hash);
    }

    #[test]
    fn context_validate_multi_hop_no_drafts() {
        // v1 -> v2 -> v3 with all non-draft lenses should validate
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v3 = make_schema_v3();

        let lens_v1_v2 = generate_lens(&v1, &v2);
        let lens_v2_v3 = generate_lens(&v2, &v3);

        // Both lenses should be non-draft (adding nullable columns)
        assert!(!lens_v1_v2.is_draft());
        assert!(!lens_v2_v3.is_draft());

        let mut ctx = SchemaContext::new(v3, "dev", "main");
        ctx.add_live_schema(v2.clone(), lens_v2_v3);
        ctx.add_live_schema(v1, lens_v1_v2);

        assert!(ctx.validate().is_ok());
    }

    #[test]
    fn context_validate_multi_hop_draft_in_middle() {
        // v1 -> v2 -> v3 where v2->v3 has a draft lens
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        // v3 adds a non-nullable column requiring a draft lens
        let v3 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .nullable_column("email", ColumnType::Text)
                    .column("org_id", ColumnType::Uuid), // non-nullable UUID = draft
            )
            .build();

        let v2_hash = SchemaHash::compute(&v2);
        let v3_hash = SchemaHash::compute(&v3);

        let lens_v1_v2 = generate_lens(&v1, &v2);
        let lens_v2_v3 = generate_lens(&v2, &v3);

        // v2->v3 lens should be draft (adding non-nullable UUID)
        assert!(lens_v2_v3.is_draft());

        let mut ctx = SchemaContext::new(v3, "dev", "main");
        ctx.add_live_schema(v2.clone(), lens_v2_v3);
        ctx.add_live_schema(v1, lens_v1_v2);

        // Validation should fail - draft lens in path from v1 to v3
        let result = ctx.validate();
        assert!(matches!(result, Err(SchemaError::DraftLensInPath { .. })));

        if let Err(SchemaError::DraftLensInPath { source, target }) = result {
            // The draft lens is v2->v3
            assert_eq!(source, v2_hash);
            assert_eq!(target, v3_hash);
        }
    }

    #[test]
    fn context_lens_path_shortest_path() {
        // If v1->v3 (direct) and v1->v2->v3 both exist, BFS should find v1->v3 first
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v3 = make_schema_v3();

        let v1_hash = SchemaHash::compute(&v1);

        let lens_v1_v2 = generate_lens(&v1, &v2);
        let lens_v2_v3 = generate_lens(&v2, &v3);
        let lens_v1_v3 = generate_lens(&v1, &v3); // Direct path

        let mut ctx = SchemaContext::new(v3, "dev", "main");
        ctx.add_live_schema(v2.clone(), lens_v2_v3);
        ctx.add_live_schema(v1.clone(), lens_v1_v2);
        // Register direct lens v1->v3
        ctx.register_lens(lens_v1_v3);

        let path = ctx.lens_path(&v1_hash).unwrap();

        // BFS should find the direct path (1 hop) before the 2-hop path
        assert_eq!(path.len(), 1);
        assert_eq!(path[0].0.source_hash, v1_hash);
        assert_eq!(path[0].0.target_hash, ctx.current_hash);
    }

    #[test]
    fn context_lens_path_missing_intermediate() {
        // Register lens v1->v2 but NOT v2 as live schema, then try v1->v3
        // This tests that lens path fails if intermediate schema isn't registered
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v3 = make_schema_v3();

        let v1_hash = SchemaHash::compute(&v1);

        let lens_v1_v2 = generate_lens(&v1, &v2);
        let lens_v2_v3 = generate_lens(&v2, &v3);

        let mut ctx = SchemaContext::new(v3, "dev", "main");
        // Only register lenses, not the intermediate schema v2
        ctx.register_lens(lens_v1_v2);
        ctx.register_lens(lens_v2_v3);

        // The lens path algorithm should still find the path via registered lenses
        // (it doesn't require schemas to be in live_schemas, just lenses to be registered)
        let path = ctx.lens_path(&v1_hash);
        assert!(path.is_ok());
        assert_eq!(path.unwrap().len(), 2);
    }
}
