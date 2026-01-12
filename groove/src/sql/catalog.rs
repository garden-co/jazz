//! Database catalog for persistence.
//!
//! The catalog stores metadata about tables, allowing a Database to be
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

/// A descriptor ID wraps an ObjectId for type safety.
/// Schema descriptors are identified by ObjectIds just like other Jazz objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DescriptorId(ObjectId);

impl DescriptorId {
    /// Create a new DescriptorId with a fresh ObjectId.
    pub fn new() -> Self {
        DescriptorId(ObjectId::new_random())
    }

    /// Create a DescriptorId from an ObjectId.
    pub fn from_object_id(id: ObjectId) -> Self {
        DescriptorId(id)
    }

    /// Get the underlying ObjectId.
    pub fn as_object_id(&self) -> ObjectId {
        self.0
    }

    /// Get a short prefix for branch naming (first 12 chars of the ObjectId string).
    pub fn short_prefix(&self) -> String {
        let s = self.0.to_string();
        s.chars().take(12).collect()
    }
}

impl Default for DescriptorId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for DescriptorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<ObjectId> for DescriptorId {
    fn from(id: ObjectId) -> Self {
        DescriptorId(id)
    }
}

impl From<DescriptorId> for ObjectId {
    fn from(id: DescriptorId) -> Self {
        id.0
    }
}

/// Database catalog - stored in a well-known object.
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
        self.tables.get(name).copied()
    }

    /// Serialize catalog to bytes.
    ///
    /// Format:
    /// - u32: number of tables
    /// - For each table:
    ///   - u32: name length
    ///   - bytes: name (UTF-8)
    ///   - 16 bytes: DescriptorId (ObjectId)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Number of tables
        buf.extend_from_slice(&(self.tables.len() as u32).to_le_bytes());

        for (name, id) in &self.tables {
            // Name length + name
            let name_bytes = name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(name_bytes);

            // DescriptorId (16 bytes - ObjectId)
            buf.extend_from_slice(&u128::from(id.as_object_id()).to_le_bytes());
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

            // DescriptorId (16 bytes - ObjectId)
            if data.len() < pos + 16 {
                return Err(CatalogError::UnexpectedEof);
            }
            let id_bytes: [u8; 16] = data[pos..pos + 16].try_into().unwrap();
            let object_id = ObjectId::new(u128::from_le_bytes(id_bytes));
            let id = DescriptorId::from_object_id(object_id);
            pos += 16;

            tables.insert(name, id);
        }

        Ok(Catalog { tables })
    }
}

/// Per-table descriptor - stored in descriptor object.
///
/// Contains all metadata needed to restore a table. Descriptors are identified
/// by ObjectIds (wrapped in DescriptorId for type safety).
///
/// ## Schema History (DAG)
///
/// Descriptors form a DAG through `parent_descriptors`. This enables:
/// - Tracking schema evolution over time
/// - Supporting schema branches (multiple children of one parent)
/// - Supporting schema merges (multiple parents for one descriptor)
/// - Computing migration paths between any two schema versions
///
/// ## Lenses
///
/// Each parent descriptor has a corresponding lens that describes how to
/// transform data between schema versions:
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
    /// Parent descriptor IDs (empty for root/initial schema).
    /// Multiple parents indicate a schema merge.
    pub parent_descriptors: Vec<DescriptorId>,
    /// Lenses for transforming data between parent schemas and this schema.
    /// Parallel to `parent_descriptors` - one lens per parent.
    /// `lenses[i]` describes the transform for `parent_descriptors[i]`.
    pub lenses: Vec<Lens>,
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
    /// - u32: number of parent descriptors (and lenses)
    /// - For each parent:
    ///   - 16 bytes: DescriptorId (ObjectId)
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

        // Parent descriptors with their lenses
        buf.extend_from_slice(&(self.parent_descriptors.len() as u32).to_le_bytes());
        for (parent, lens) in self.parent_descriptors.iter().zip(self.lenses.iter()) {
            buf.extend_from_slice(&u128::from(parent.as_object_id()).to_le_bytes());
            let lens_bytes = lens.to_bytes();
            buf.extend_from_slice(&(lens_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&lens_bytes);
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

        // Parent descriptors with lenses
        if data.len() < pos + 4 {
            return Err(CatalogError::UnexpectedEof);
        }
        let num_parents = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let mut parent_descriptors = Vec::with_capacity(num_parents);
        let mut lenses = Vec::with_capacity(num_parents);
        for _ in 0..num_parents {
            // DescriptorId (16 bytes - ObjectId)
            if data.len() < pos + 16 {
                return Err(CatalogError::UnexpectedEof);
            }
            let parent_bytes: [u8; 16] = data[pos..pos + 16].try_into().unwrap();
            let parent_object_id = ObjectId::new(u128::from_le_bytes(parent_bytes));
            parent_descriptors.push(DescriptorId::from_object_id(parent_object_id));
            pos += 16;

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
            lenses.push(lens);
            pos += lens_len;
        }

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
            parent_descriptors,
            lenses,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::schema::{ColumnDef, ColumnType};

    #[test]
    fn catalog_roundtrip() {
        let mut catalog = Catalog::new();
        // Create some test descriptor IDs
        let desc_id1 = DescriptorId::from_object_id(ObjectId::new(1));
        let desc_id2 = DescriptorId::from_object_id(ObjectId::new(2));

        catalog.tables.insert("users".to_string(), desc_id1);
        catalog.tables.insert("posts".to_string(), desc_id2);

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
            parent_descriptors: vec![],
            lenses: vec![],
            rows_object_id: ObjectId::new(100),
            schema_object_id: ObjectId::new(200),
            index_object_ids: index_ids,
        };

        let bytes = descriptor.to_bytes();
        let restored = TableDescriptor::from_bytes(&bytes).unwrap();

        assert_eq!(restored.schema.name, "users");
        assert_eq!(restored.schema.columns.len(), 2);
        assert_eq!(restored.parent_descriptors.len(), 0);
        assert_eq!(restored.lenses.len(), 0);
        assert_eq!(restored.rows_object_id, ObjectId::new(100));
        assert_eq!(restored.schema_object_id, ObjectId::new(200));
        assert_eq!(
            restored.index_object_ids.get("org_id"),
            Some(&ObjectId::new(300))
        );
    }

    #[test]
    fn table_descriptor_with_parents_roundtrip() {
        use crate::sql::lens::ColumnTransform;

        let schema = TableSchema {
            name: "users".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                ty: ColumnType::I32,
                nullable: false,
            }],
        };

        let parent1 = DescriptorId::from_object_id(ObjectId::new(1));
        let parent2 = DescriptorId::from_object_id(ObjectId::new(2));

        // Create a lens for each parent
        let lens1 = Lens::from_forward(vec![ColumnTransform::rename("old_name", "new_name")]);
        let lens2 = Lens::identity();

        let descriptor = TableDescriptor {
            schema: schema.clone(),
            policies: TablePolicies::default(),
            parent_descriptors: vec![parent1, parent2],
            lenses: vec![lens1.clone(), lens2.clone()],
            rows_object_id: ObjectId::new(100),
            schema_object_id: ObjectId::new(200),
            index_object_ids: HashMap::new(),
        };

        let bytes = descriptor.to_bytes();
        let restored = TableDescriptor::from_bytes(&bytes).unwrap();

        assert_eq!(restored.parent_descriptors.len(), 2);
        assert_eq!(restored.parent_descriptors[0], parent1);
        assert_eq!(restored.parent_descriptors[1], parent2);
        assert_eq!(restored.lenses.len(), 2);
        // Check first lens has the rename transform
        assert_eq!(restored.lenses[0].forward.len(), 1);
        // Check second lens is identity (empty)
        assert_eq!(restored.lenses[1].forward.len(), 0);
    }

    #[test]
    fn descriptor_id_basics() {
        // DescriptorId wraps ObjectId
        let obj_id = ObjectId::new(12345);
        let desc_id = DescriptorId::from_object_id(obj_id);

        assert_eq!(desc_id.as_object_id(), obj_id);

        // Short prefix for branch naming
        let short = desc_id.short_prefix();
        assert_eq!(short.len(), 12);

        // Full display matches ObjectId display
        let full = desc_id.to_string();
        assert_eq!(full, obj_id.to_string());

        // From/Into conversions
        let desc_id2: DescriptorId = obj_id.into();
        assert_eq!(desc_id, desc_id2);

        let obj_id2: ObjectId = desc_id.into();
        assert_eq!(obj_id, obj_id2);
    }

    #[test]
    fn descriptor_id_new_generates_unique() {
        let id1 = DescriptorId::new();
        let id2 = DescriptorId::new();

        // New IDs should be different
        assert_ne!(id1, id2);
    }
}
