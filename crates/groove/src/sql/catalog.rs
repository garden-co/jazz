//! Query manager catalog for persistence.
//!
//! The catalog stores metadata about tables, allowing a QueryManager to be
//! restored from an Environment after being thrown away.
//!
//! ## Schema Descriptors
//!
//! Table descriptors are identified by ObjectIds (like other objects in Jazz).
//! Each descriptor contains:
//! - The table schema (columns, types)
//! - Access control policies
//! - Parent descriptor IDs (for schema history/migration DAG)
//! - References to associated objects (rows, indexes)

use std::collections::HashMap;

use crate::object::ObjectId;
use crate::sql::lens::Lens;
use crate::sql::policy::TablePolicies;
use crate::sql::schema::TableSchema;

/// A descriptor ID identifies a specific schema version for a table.
///
/// With branch-based versioning, each table has a single descriptor object,
/// and each schema version is a branch on that object.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DescriptorId {
    /// The descriptor object for this table.
    pub object_id: ObjectId,
    /// The schema version (branch name), e.g., "v1", "v2".
    pub version: String,
}

impl DescriptorId {
    /// Create a new DescriptorId for the first version of a table.
    pub fn new_v1(object_id: ObjectId) -> Self {
        DescriptorId {
            object_id,
            version: "v1".to_string(),
        }
    }

    /// Create a DescriptorId with a specific version.
    pub fn new(object_id: ObjectId, version: impl Into<String>) -> Self {
        DescriptorId {
            object_id,
            version: version.into(),
        }
    }

    /// Get the underlying ObjectId.
    pub fn as_object_id(&self) -> ObjectId {
        self.object_id
    }

    /// Get the version (branch name).
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Create the next version (e.g., v1 -> v2).
    pub fn next_version(&self) -> Self {
        let next = if let Some(num) = self.version.strip_prefix('v') {
            if let Ok(n) = num.parse::<u32>() {
                format!("v{}", n + 1)
            } else {
                format!("{}_next", self.version)
            }
        } else {
            format!("{}_next", self.version)
        };
        DescriptorId {
            object_id: self.object_id,
            version: next,
        }
    }
}

impl std::fmt::Display for DescriptorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.object_id, self.version)
    }
}

/// QueryManager catalog - stored in a well-known object.
///
/// Maps table names to their current descriptor IDs.
#[derive(Debug, Clone, Default)]
pub struct Catalog {
    /// Table name → current descriptor ID
    pub tables: HashMap<String, DescriptorId>,
}

impl Catalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a table with its descriptor ID.
    pub fn register_table(&mut self, name: String, descriptor_id: DescriptorId) {
        self.tables.insert(name, descriptor_id);
    }

    /// Update a table's descriptor ID (for migrations).
    pub fn update_table(&mut self, name: String, descriptor_id: DescriptorId) {
        self.tables.insert(name, descriptor_id);
    }

    /// Get the current descriptor ID for a table.
    pub fn get_descriptor_id(&self, name: &str) -> Option<DescriptorId> {
        self.tables.get(name).cloned()
    }

    /// Serialize catalog to bytes.
    ///
    /// Format:
    /// - u32: number of tables
    /// - For each table:
    ///   - u32: name length
    ///   - bytes: name (UTF-8)
    ///   - 16 bytes: ObjectId
    ///   - u32: version length
    ///   - bytes: version (UTF-8)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Number of tables
        buf.extend_from_slice(&(self.tables.len() as u32).to_le_bytes());

        for (name, id) in &self.tables {
            // Name length + name
            let name_bytes = name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(name_bytes);

            // ObjectId (16 bytes)
            buf.extend_from_slice(&u128::from(id.object_id).to_le_bytes());

            // Version length + version
            let version_bytes = id.version.as_bytes();
            buf.extend_from_slice(&(version_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(version_bytes);
        }

        buf
    }

    /// Deserialize catalog from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, CatalogError> {
        if data.len() < 4 {
            return Err(CatalogError::UnexpectedEof);
        }

        let mut pos = 0;

        // Number of tables
        let num_tables = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let mut tables = HashMap::with_capacity(num_tables);

        for _ in 0..num_tables {
            // Name length
            if data.len() < pos + 4 {
                return Err(CatalogError::UnexpectedEof);
            }
            let name_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            // Name
            if data.len() < pos + name_len {
                return Err(CatalogError::UnexpectedEof);
            }
            let name = String::from_utf8(data[pos..pos + name_len].to_vec())
                .map_err(|_| CatalogError::InvalidUtf8)?;
            pos += name_len;

            // ObjectId (16 bytes)
            if data.len() < pos + 16 {
                return Err(CatalogError::UnexpectedEof);
            }
            let id_bytes: [u8; 16] = data[pos..pos + 16].try_into().unwrap();
            let object_id = ObjectId::new(u128::from_le_bytes(id_bytes));
            pos += 16;

            // Version length
            if data.len() < pos + 4 {
                return Err(CatalogError::UnexpectedEof);
            }
            let version_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            // Version
            if data.len() < pos + version_len {
                return Err(CatalogError::UnexpectedEof);
            }
            let version = String::from_utf8(data[pos..pos + version_len].to_vec())
                .map_err(|_| CatalogError::InvalidUtf8)?;
            pos += version_len;

            let id = DescriptorId::new(object_id, version);
            tables.insert(name, id);
        }

        Ok(Catalog { tables })
    }
}

/// Per-table descriptor - stored in descriptor object.
///
/// Contains all metadata needed to restore a table. Each schema version is
/// stored as a branch on the descriptor object.
///
/// ## Schema History
///
/// Schema versions form a DAG through the commit graph. Each branch represents
/// a schema version, and the commit parents track lineage:
/// - v1 branch: initial schema (no parent commit)
/// - v2 branch: migration from v1 (parent commit is v1's tip)
/// - etc.
///
/// ## Lenses
///
/// Each version (except v1) has a `lens_from_parent` that describes how to
/// transform data from the parent version to this version:
/// - `forward`: transforms data from parent schema to this schema
/// - `backward`: transforms data from this schema to parent schema
///
/// This enables bidirectional compatibility: old clients can read new data
/// (via backward transform) and new clients can read old data (via forward).
#[derive(Debug, Clone)]
pub struct TableDescriptor {
    /// The table schema.
    pub schema: TableSchema,
    /// Access control policies for this table.
    pub policies: TablePolicies,
    /// Lens to transform data from the parent schema version.
    /// None for the initial schema (v1).
    pub lens_from_parent: Option<Lens>,
    /// Object ID for the TableRows (set of row IDs).
    pub rows_object_id: ObjectId,
    /// Schema object ID (where schema is stored).
    pub schema_object_id: ObjectId,
    /// Index object IDs: column name → object ID.
    pub index_object_ids: HashMap<String, ObjectId>,
}

impl TableDescriptor {
    /// Serialize descriptor to bytes.
    ///
    /// Format:
    /// - schema bytes (with length prefix)
    /// - policies bytes (with length prefix)
    /// - u8: has_lens_from_parent (0 or 1)
    /// - if has_lens_from_parent:
    ///   - u32: lens length
    ///   - bytes: lens
    /// - 16 bytes: rows_object_id
    /// - 16 bytes: schema_object_id
    /// - u32: number of indexes
    /// - For each index:
    ///   - u32: column name length
    ///   - bytes: column name (UTF-8)
    ///   - 16 bytes: ObjectId
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Schema (length-prefixed)
        let schema_bytes = self.schema.to_bytes();
        buf.extend_from_slice(&(schema_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&schema_bytes);

        // Policies (length-prefixed)
        let policies_bytes = self.policies.to_bytes();
        buf.extend_from_slice(&(policies_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&policies_bytes);

        // Lens from parent (optional)
        if let Some(lens) = &self.lens_from_parent {
            buf.push(1); // has lens
            let lens_bytes = lens.to_bytes();
            buf.extend_from_slice(&(lens_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&lens_bytes);
        } else {
            buf.push(0); // no lens
        }

        // rows_object_id
        buf.extend_from_slice(&u128::from(self.rows_object_id).to_le_bytes());

        // schema_object_id
        buf.extend_from_slice(&u128::from(self.schema_object_id).to_le_bytes());

        // Index objects
        buf.extend_from_slice(&(self.index_object_ids.len() as u32).to_le_bytes());
        for (col_name, id) in &self.index_object_ids {
            let name_bytes = col_name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(name_bytes);
            buf.extend_from_slice(&u128::from(*id).to_le_bytes());
        }

        buf
    }

    /// Deserialize descriptor from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, CatalogError> {
        let mut pos = 0;

        // Schema length
        if data.len() < pos + 4 {
            return Err(CatalogError::UnexpectedEof);
        }
        let schema_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        // Schema bytes
        if data.len() < pos + schema_len {
            return Err(CatalogError::UnexpectedEof);
        }
        let schema = TableSchema::from_bytes(&data[pos..pos + schema_len])
            .map_err(|e| CatalogError::SchemaError(e.to_string()))?;
        pos += schema_len;

        // Policies length
        if data.len() < pos + 4 {
            return Err(CatalogError::UnexpectedEof);
        }
        let policies_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        // Policies bytes
        if data.len() < pos + policies_len {
            return Err(CatalogError::UnexpectedEof);
        }
        let policies = TablePolicies::from_bytes(&data[pos..pos + policies_len], &schema.name)
            .map_err(|e| CatalogError::PolicyError(e.to_string()))?;
        pos += policies_len;

        // Lens from parent (optional)
        if data.len() < pos + 1 {
            return Err(CatalogError::UnexpectedEof);
        }
        let has_lens = data[pos] != 0;
        pos += 1;

        let lens_from_parent = if has_lens {
            // Lens length
            if data.len() < pos + 4 {
                return Err(CatalogError::UnexpectedEof);
            }
            let lens_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            // Lens bytes
            if data.len() < pos + lens_len {
                return Err(CatalogError::UnexpectedEof);
            }
            let (lens, _) = Lens::from_bytes(&data[pos..pos + lens_len])
                .map_err(|e| CatalogError::LensError(e.to_string()))?;
            pos += lens_len;
            Some(lens)
        } else {
            None
        };

        // rows_object_id
        if data.len() < pos + 16 {
            return Err(CatalogError::UnexpectedEof);
        }
        let rows_id_bytes: [u8; 16] = data[pos..pos + 16].try_into().unwrap();
        let rows_object_id = ObjectId::new(u128::from_le_bytes(rows_id_bytes));
        pos += 16;

        // schema_object_id
        if data.len() < pos + 16 {
            return Err(CatalogError::UnexpectedEof);
        }
        let schema_id_bytes: [u8; 16] = data[pos..pos + 16].try_into().unwrap();
        let schema_object_id = ObjectId::new(u128::from_le_bytes(schema_id_bytes));
        pos += 16;

        // Number of indexes
        if data.len() < pos + 4 {
            return Err(CatalogError::UnexpectedEof);
        }
        let num_indexes = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let mut index_object_ids = HashMap::with_capacity(num_indexes);
        for _ in 0..num_indexes {
            // Column name length
            if data.len() < pos + 4 {
                return Err(CatalogError::UnexpectedEof);
            }
            let name_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            // Column name
            if data.len() < pos + name_len {
                return Err(CatalogError::UnexpectedEof);
            }
            let col_name = String::from_utf8(data[pos..pos + name_len].to_vec())
                .map_err(|_| CatalogError::InvalidUtf8)?;
            pos += name_len;

            // ObjectId
            if data.len() < pos + 16 {
                return Err(CatalogError::UnexpectedEof);
            }
            let id_bytes: [u8; 16] = data[pos..pos + 16].try_into().unwrap();
            let id = ObjectId::new(u128::from_le_bytes(id_bytes));
            pos += 16;

            index_object_ids.insert(col_name, id);
        }

        Ok(TableDescriptor {
            schema,
            policies,
            lens_from_parent,
            rows_object_id,
            schema_object_id,
            index_object_ids,
        })
    }
}

/// Errors during catalog operations.
#[derive(Debug, Clone)]
pub enum CatalogError {
    UnexpectedEof,
    InvalidUtf8,
    SchemaError(String),
    PolicyError(String),
    LensError(String),
}

impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogError::UnexpectedEof => write!(f, "unexpected end of catalog data"),
            CatalogError::InvalidUtf8 => write!(f, "invalid UTF-8 in catalog"),
            CatalogError::SchemaError(e) => write!(f, "schema error: {}", e),
            CatalogError::PolicyError(e) => write!(f, "policy error: {}", e),
            CatalogError::LensError(e) => write!(f, "lens error: {}", e),
        }
    }
}

impl std::error::Error for CatalogError {}

// =============================================================================
// Schema Relationship Resolution
// =============================================================================

/// Error when schemas from different branches cannot be resolved to a target.
#[derive(Debug, Clone)]
pub enum SchemaConflictError {
    /// No schemas provided to resolve.
    Empty,
    /// A required descriptor was not found in the provided set.
    DescriptorNotFound(DescriptorId),
    /// Schemas have diverged - no schema is a descendant of all others.
    /// Contains the diverged descriptor IDs.
    Diverged(Vec<DescriptorId>),
}

impl std::fmt::Display for SchemaConflictError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaConflictError::Empty => write!(f, "no schemas to resolve"),
            SchemaConflictError::DescriptorNotFound(id) => {
                write!(f, "descriptor not found: {}", id)
            }
            SchemaConflictError::Diverged(ids) => {
                write!(f, "schemas have diverged, no common descendant: {:?}", ids)
            }
        }
    }
}

impl std::error::Error for SchemaConflictError {}

/// Find the "target" schema from a set of descriptors.
///
/// The target schema is the one that is a descendant of all others. This is
/// used when querying across branches with different schema versions - the
/// result should use the "newest" schema.
///
/// With branch-based versioning, all descriptors for a table share the same
/// ObjectId. The "newest" is the one with the highest version number.
///
/// # Arguments
///
/// * `descriptor_ids` - The descriptor IDs to consider (typically from branch names)
/// * `descriptors` - All available descriptors indexed by ID (unused in new model)
///
/// # Returns
///
/// The DescriptorId with the highest version, or an error if:
/// - No descriptors provided
/// - Descriptors are from different tables (different ObjectIds)
pub fn find_target_schema(
    descriptor_ids: &[DescriptorId],
    _descriptors: &HashMap<DescriptorId, TableDescriptor>,
) -> Result<DescriptorId, SchemaConflictError> {
    if descriptor_ids.is_empty() {
        return Err(SchemaConflictError::Empty);
    }

    if descriptor_ids.len() == 1 {
        return Ok(descriptor_ids[0].clone());
    }

    // Check all descriptors are for the same table (same ObjectId)
    let first_obj_id = descriptor_ids[0].object_id;
    for id in descriptor_ids.iter().skip(1) {
        if id.object_id != first_obj_id {
            // Different tables - can't resolve
            return Err(SchemaConflictError::Diverged(descriptor_ids.to_vec()));
        }
    }

    // Find the highest version (latest schema)
    // Version format: "v1", "v2", etc. Higher number = newer
    let target = descriptor_ids
        .iter()
        .max_by(|a, b| compare_versions(&a.version, &b.version))
        .cloned()
        .unwrap();

    Ok(target)
}

/// Compare version strings for ordering.
/// Supports "v1", "v2", etc. format.
fn compare_versions(a: &str, b: &str) -> std::cmp::Ordering {
    let a_num = a.strip_prefix('v').and_then(|n| n.parse::<u32>().ok());
    let b_num = b.strip_prefix('v').and_then(|n| n.parse::<u32>().ok());

    match (a_num, b_num) {
        (Some(an), Some(bn)) => an.cmp(&bn),
        (Some(_), None) => std::cmp::Ordering::Greater, // "v1" > "custom"
        (None, Some(_)) => std::cmp::Ordering::Less,
        (None, None) => a.cmp(b), // Fall back to lexicographic
    }
}

/// Get the lens to transform data from `source` schema to `target` schema.
///
/// Composes lenses along the version chain from source to target.
///
/// # Arguments
///
/// * `source` - The source descriptor ID (older schema)
/// * `target` - The target descriptor ID (newer schema, must be descendant of source)
/// * `descriptors` - All available descriptors indexed by ID
///
/// # Returns
///
/// The composed lens for the transformation, or None if no path exists.
pub fn get_lens_path(
    source: &DescriptorId,
    target: &DescriptorId,
    descriptors: &HashMap<DescriptorId, TableDescriptor>,
) -> Option<Lens> {
    if source == target {
        return Some(Lens::identity());
    }

    // Must be same table
    if source.object_id != target.object_id {
        return None;
    }

    // Get version numbers
    let source_num = source.version.strip_prefix('v')?.parse::<u32>().ok()?;
    let target_num = target.version.strip_prefix('v')?.parse::<u32>().ok()?;

    if source_num >= target_num {
        return None; // Source must be older than target
    }

    // Compose lenses from source+1 to target
    // Each version has lens_from_parent that transforms from the previous version
    let mut composed = Lens::identity();
    for v in (source_num + 1)..=target_num {
        let version_id = DescriptorId::new(source.object_id, format!("v{}", v));
        if let Some(desc) = descriptors.get(&version_id) {
            if let Some(lens) = &desc.lens_from_parent {
                composed = composed.compose(lens);
            }
        } else {
            return None; // Missing intermediate version
        }
    }

    Some(composed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::schema::{ColumnDef, ColumnType};

    #[test]
    fn catalog_roundtrip() {
        let mut catalog = Catalog::new();
        // Create some test descriptor IDs
        let desc_id1 = DescriptorId::new_v1(ObjectId::new(1));
        let desc_id2 = DescriptorId::new(ObjectId::new(2), "v2");

        catalog.tables.insert("users".to_string(), desc_id1.clone());
        catalog.tables.insert("posts".to_string(), desc_id2.clone());

        let bytes = catalog.to_bytes();
        let restored = Catalog::from_bytes(&bytes).unwrap();

        assert_eq!(restored.tables.len(), 2);
        assert_eq!(restored.tables.get("users"), Some(&desc_id1));
        assert_eq!(restored.tables.get("posts"), Some(&desc_id2));
    }

    #[test]
    fn table_descriptor_roundtrip() {
        let schema = TableSchema {
            name: "users".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    ty: ColumnType::I32,
                    nullable: false,
                },
                ColumnDef {
                    name: "name".to_string(),
                    ty: ColumnType::String,
                    nullable: false,
                },
            ],
        };

        let mut index_ids = HashMap::new();
        index_ids.insert("org_id".to_string(), ObjectId::new(300));

        let descriptor = TableDescriptor {
            schema: schema.clone(),
            policies: TablePolicies::default(),
            lens_from_parent: None,
            rows_object_id: ObjectId::new(100),
            schema_object_id: ObjectId::new(200),
            index_object_ids: index_ids,
        };

        let bytes = descriptor.to_bytes();
        let restored = TableDescriptor::from_bytes(&bytes).unwrap();

        assert_eq!(restored.schema.name, "users");
        assert_eq!(restored.schema.columns.len(), 2);
        assert!(restored.lens_from_parent.is_none());
        assert_eq!(restored.rows_object_id, ObjectId::new(100));
        assert_eq!(restored.schema_object_id, ObjectId::new(200));
        assert_eq!(
            restored.index_object_ids.get("org_id"),
            Some(&ObjectId::new(300))
        );
    }

    #[test]
    fn table_descriptor_with_lens_roundtrip() {
        use crate::sql::lens::ColumnTransform;

        let schema = TableSchema {
            name: "users".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                ty: ColumnType::I32,
                nullable: false,
            }],
        };

        // Create a lens from parent
        let lens = Lens::from_forward(vec![ColumnTransform::rename("old_name", "new_name")]);

        let descriptor = TableDescriptor {
            schema: schema.clone(),
            policies: TablePolicies::default(),
            lens_from_parent: Some(lens.clone()),
            rows_object_id: ObjectId::new(100),
            schema_object_id: ObjectId::new(200),
            index_object_ids: HashMap::new(),
        };

        let bytes = descriptor.to_bytes();
        let restored = TableDescriptor::from_bytes(&bytes).unwrap();

        assert!(restored.lens_from_parent.is_some());
        let restored_lens = restored.lens_from_parent.unwrap();
        // Check lens has the rename transform
        assert_eq!(restored_lens.forward.len(), 1);
    }

    #[test]
    fn descriptor_id_basics() {
        // DescriptorId contains ObjectId + version
        let obj_id = ObjectId::new(12345);
        let desc_id = DescriptorId::new_v1(obj_id);

        assert_eq!(desc_id.as_object_id(), obj_id);
        assert_eq!(desc_id.version(), "v1");

        // Display format: object_id@version
        let display = desc_id.to_string();
        assert!(display.contains("@v1"));
    }

    #[test]
    fn descriptor_id_next_version() {
        let obj_id = ObjectId::new(12345);
        let v1 = DescriptorId::new_v1(obj_id);
        let v2 = v1.next_version();
        let v3 = v2.next_version();

        assert_eq!(v1.version(), "v1");
        assert_eq!(v2.version(), "v2");
        assert_eq!(v3.version(), "v3");

        // All share same object_id
        assert_eq!(v1.object_id, v2.object_id);
        assert_eq!(v2.object_id, v3.object_id);
    }

    // =============================================================================
    // Schema Resolution Tests
    // =============================================================================

    fn make_test_schema(name: &str) -> TableSchema {
        TableSchema {
            name: name.to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                ty: ColumnType::I32,
                nullable: false,
            }],
        }
    }

    fn make_test_descriptor(lens_from_parent: Option<Lens>) -> TableDescriptor {
        TableDescriptor {
            schema: make_test_schema("test"),
            policies: TablePolicies::default(),
            lens_from_parent,
            rows_object_id: ObjectId::new(100),
            schema_object_id: ObjectId::new(200),
            index_object_ids: HashMap::new(),
        }
    }

    #[test]
    fn find_target_schema_single_descriptor() {
        let id = DescriptorId::new_v1(ObjectId::new(1));
        let mut descriptors = HashMap::new();
        descriptors.insert(id.clone(), make_test_descriptor(None));

        let result = find_target_schema(std::slice::from_ref(&id), &descriptors);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), id);
    }

    #[test]
    fn find_target_schema_empty_returns_error() {
        let descriptors = HashMap::new();
        let result = find_target_schema(&[], &descriptors);
        assert!(matches!(result, Err(SchemaConflictError::Empty)));
    }

    #[test]
    fn find_target_schema_linear_chain() {
        // Same table: v1, v2, v3
        // v3 is the target (highest version)
        let obj_id = ObjectId::new(1);
        let v1 = DescriptorId::new(obj_id, "v1");
        let v2 = DescriptorId::new(obj_id, "v2");
        let v3 = DescriptorId::new(obj_id, "v3");

        let mut descriptors = HashMap::new();
        descriptors.insert(v1.clone(), make_test_descriptor(None));
        descriptors.insert(v2.clone(), make_test_descriptor(Some(Lens::identity())));
        descriptors.insert(v3.clone(), make_test_descriptor(Some(Lens::identity())));

        // v3 should be the target (highest version number)
        let result = find_target_schema(&[v1.clone(), v2.clone(), v3.clone()], &descriptors);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), v3);

        // Order shouldn't matter
        let result2 = find_target_schema(&[v3.clone(), v1.clone(), v2.clone()], &descriptors);
        assert!(result2.is_ok());
        assert_eq!(result2.unwrap(), v3);
    }

    #[test]
    fn find_target_schema_different_tables_returns_error() {
        // Different tables (different ObjectIds) - can't compare
        let v1 = DescriptorId::new_v1(ObjectId::new(1));
        let v2 = DescriptorId::new_v1(ObjectId::new(2));

        let mut descriptors = HashMap::new();
        descriptors.insert(v1.clone(), make_test_descriptor(None));
        descriptors.insert(v2.clone(), make_test_descriptor(None));

        // Different tables can't be resolved
        let result = find_target_schema(&[v1, v2], &descriptors);
        assert!(matches!(result, Err(SchemaConflictError::Diverged(_))));
    }

    #[test]
    fn get_lens_path_identity() {
        let id = DescriptorId::new_v1(ObjectId::new(1));
        let mut descriptors = HashMap::new();
        descriptors.insert(id.clone(), make_test_descriptor(None));

        // Same source and target should return identity lens
        let lens = get_lens_path(&id, &id, &descriptors);
        assert!(lens.is_some());
        assert!(lens.unwrap().forward.is_empty());
    }

    #[test]
    fn get_lens_path_linear_chain() {
        use crate::sql::lens::ColumnTransform;

        let obj_id = ObjectId::new(1);
        let v1 = DescriptorId::new(obj_id, "v1");
        let v2 = DescriptorId::new(obj_id, "v2");
        let v3 = DescriptorId::new(obj_id, "v3");

        // v1 -> v2 (rename a -> b)
        // v2 -> v3 (rename b -> c)
        let lens1 = Lens::from_forward(vec![ColumnTransform::rename("a", "b")]);
        let lens2 = Lens::from_forward(vec![ColumnTransform::rename("b", "c")]);

        let mut descriptors = HashMap::new();
        descriptors.insert(v1.clone(), make_test_descriptor(None));
        descriptors.insert(v2.clone(), make_test_descriptor(Some(lens1)));
        descriptors.insert(v3.clone(), make_test_descriptor(Some(lens2)));

        // Path from v1 to v3 should exist
        let lens = get_lens_path(&v1, &v3, &descriptors);
        assert!(lens.is_some());

        // The composed lens should have 2 transforms
        let composed = lens.unwrap();
        assert_eq!(composed.forward.len(), 2);
    }

    #[test]
    fn get_lens_path_no_path_different_tables() {
        let v1 = DescriptorId::new_v1(ObjectId::new(1));
        let v2 = DescriptorId::new_v1(ObjectId::new(2));

        let mut descriptors = HashMap::new();
        descriptors.insert(v1.clone(), make_test_descriptor(None));
        descriptors.insert(v2.clone(), make_test_descriptor(None));

        // No path between different tables
        let lens = get_lens_path(&v1, &v2, &descriptors);
        assert!(lens.is_none());
    }

    #[test]
    fn compare_versions_ordering() {
        // v1 < v2 < v3
        assert_eq!(compare_versions("v1", "v2"), std::cmp::Ordering::Less);
        assert_eq!(compare_versions("v2", "v1"), std::cmp::Ordering::Greater);
        assert_eq!(compare_versions("v1", "v1"), std::cmp::Ordering::Equal);
        assert_eq!(compare_versions("v10", "v2"), std::cmp::Ordering::Greater);
    }
}
