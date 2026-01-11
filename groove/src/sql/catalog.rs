//! Database catalog for persistence.
//!
//! The catalog stores metadata about tables, allowing a Database to be
//! restored from an Environment after being thrown away.

use std::collections::HashMap;

use crate::object::ObjectId;
use crate::sql::policy::TablePolicies;
use crate::sql::schema::TableSchema;

/// Database catalog - stored in a well-known object.
///
/// Maps table names to their descriptor object IDs.
#[derive(Debug, Clone, Default)]
pub struct Catalog {
    /// Table name → descriptor object ID
    pub tables: HashMap<String, ObjectId>,
}

impl Catalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Serialize catalog to bytes.
    ///
    /// Format:
    /// - u32: number of tables
    /// - For each table:
    ///   - u32: name length
    ///   - bytes: name (UTF-8)
    ///   - 16 bytes: ObjectId
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Number of tables
        buf.extend_from_slice(&(self.tables.len() as u32).to_le_bytes());

        for (name, id) in &self.tables {
            // Name length + name
            let name_bytes = name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(name_bytes);

            // ObjectId (u128)
            buf.extend_from_slice(&u128::from(*id).to_le_bytes());
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

            // ObjectId
            if data.len() < pos + 16 {
                return Err(CatalogError::UnexpectedEof);
            }
            let id_bytes: [u8; 16] = data[pos..pos + 16].try_into().unwrap();
            let id = ObjectId::new(u128::from_le_bytes(id_bytes));
            pos += 16;

            tables.insert(name, id);
        }

        Ok(Catalog { tables })
    }
}

/// Per-table descriptor - stored in descriptor object.
///
/// Contains all metadata needed to restore a table.
#[derive(Debug, Clone)]
pub struct TableDescriptor {
    /// The table schema.
    pub schema: TableSchema,
    /// Access control policies for this table.
    pub policies: TablePolicies,
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
}

impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogError::UnexpectedEof => write!(f, "unexpected end of catalog data"),
            CatalogError::InvalidUtf8 => write!(f, "invalid UTF-8 in catalog"),
            CatalogError::SchemaError(e) => write!(f, "schema error: {}", e),
            CatalogError::PolicyError(e) => write!(f, "policy error: {}", e),
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
        catalog
            .tables
            .insert("users".to_string(), ObjectId::new(100));
        catalog
            .tables
            .insert("posts".to_string(), ObjectId::new(200));

        let bytes = catalog.to_bytes();
        let restored = Catalog::from_bytes(&bytes).unwrap();

        assert_eq!(restored.tables.len(), 2);
        assert_eq!(restored.tables.get("users"), Some(&ObjectId::new(100)));
        assert_eq!(restored.tables.get("posts"), Some(&ObjectId::new(200)));
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
            rows_object_id: ObjectId::new(100),
            schema_object_id: ObjectId::new(200),
            index_object_ids: index_ids,
        };

        let bytes = descriptor.to_bytes();
        let restored = TableDescriptor::from_bytes(&bytes).unwrap();

        assert_eq!(restored.schema.name, "users");
        assert_eq!(restored.schema.columns.len(), 2);
        assert_eq!(restored.rows_object_id, ObjectId::new(100));
        assert_eq!(restored.schema_object_id, ObjectId::new(200));
        assert_eq!(
            restored.index_object_ids.get("org_id"),
            Some(&ObjectId::new(300))
        );
    }
}
