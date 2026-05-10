use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use crate::object::BranchName;

use super::*;

// ============================================================================
// Schema Hashing - Content-addressed schema identification
// ============================================================================

/// Content-addressed hash of a schema's structural elements.
/// Uses BLAKE3 over deterministic table ordering while preserving each table's
/// declared column order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SchemaHash(pub [u8; 32]);

impl SchemaHash {
    /// Create a SchemaHash from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Create a SchemaHash from a hex string.
    pub fn from_hex(hex_str: &str) -> Option<Self> {
        let bytes = hex::decode(hex_str).ok()?;
        if bytes.len() != 32 {
            return None;
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Some(Self(arr))
    }

    /// Get the raw bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Get a 12-character hex prefix for display/filenames.
    /// Uses 6 bytes (48 bits) for good collision resistance.
    pub fn short(&self) -> String {
        hex::encode(&self.0[..6])
    }

    pub fn to_hex(&self) -> String {
        static CACHE: OnceLock<Mutex<HashMap<SchemaHash, String>>> = OnceLock::new();
        let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
        if let Some(cached) = cache
            .lock()
            .expect("schema hash hex cache poisoned")
            .get(self)
            .cloned()
        {
            return cached;
        }

        let encoded = hex::encode(&self.0);
        cache
            .lock()
            .expect("schema hash hex cache poisoned")
            .insert(*self, encoded.clone());
        encoded
    }

    /// Convert to an ObjectId for storage in the catalogue.
    ///
    /// Uses UUIDv5 with DNS namespace over the hash bytes.
    /// Deterministic: same hash always produces same ObjectId.
    pub fn to_object_id(&self) -> crate::object::ObjectId {
        crate::object::ObjectId::from_uuid(uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_DNS, &self.0))
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

            // Hash row descriptor in declared column order
            hash_row_descriptor(&mut hasher, &table_schema.columns);

            // Hash the optional index override. Absence means historical
            // "index every declared user column"; an explicit subset changes
            // query-planning/index-maintenance semantics and therefore belongs
            // to the schema identity.
            hasher.update(&[1]);
            if let Some(indexed_columns) = &table_schema.indexed_columns {
                let mut columns: Vec<_> = indexed_columns.iter().map(|c| c.as_str()).collect();
                columns.sort_unstable();
                for column in columns {
                    hasher.update(column.as_bytes());
                    hasher.update(&[0]);
                }
            }
        }

        Self(*hasher.finalize().as_bytes())
    }
}

impl std::fmt::Display for SchemaHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl Serialize for SchemaHash {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for SchemaHash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        let bytes = hex::decode(&s).map_err(serde::de::Error::custom)?;
        if bytes.len() != 32 {
            return Err(serde::de::Error::custom("SchemaHash must be 32 bytes"));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(SchemaHash(arr))
    }
}

/// Hash a RowDescriptor into a hasher, preserving declared column order.
pub(crate) fn hash_row_descriptor(hasher: &mut blake3::Hasher, descriptor: &RowDescriptor) {
    for col in &descriptor.columns {
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

    // Schema-level default
    if let Some(default) = &col.default {
        hasher.update(&[1]);
        hash_value(hasher, default);
    } else {
        hasher.update(&[0]);
    }

    if let Some(strategy) = col.merge_strategy {
        hasher.update(&[1]);
        match strategy {
            ColumnMergeStrategy::Counter => {
                hasher.update(&[1]);
            }
        }
    } else {
        hasher.update(&[0]);
    }
    hasher.update(&[0]); // delimiter
}

fn hash_value(hasher: &mut blake3::Hasher, value: &Value) {
    match value {
        Value::Integer(v) => {
            hasher.update(&[1]);
            hasher.update(&v.to_le_bytes());
        }
        Value::BigInt(v) => {
            hasher.update(&[2]);
            hasher.update(&v.to_le_bytes());
        }
        Value::Double(v) => {
            hasher.update(&[10]);
            hasher.update(&v.to_le_bytes());
        }
        Value::Boolean(v) => {
            hasher.update(&[3, *v as u8]);
        }
        Value::Text(v) => {
            hasher.update(&[4]);
            hasher.update(v.as_bytes());
            hasher.update(&[0]);
        }
        Value::Timestamp(v) => {
            hasher.update(&[5]);
            hasher.update(&v.to_le_bytes());
        }
        Value::Uuid(v) => {
            hasher.update(&[6]);
            hasher.update(v.uuid().as_bytes());
        }
        Value::BatchId(v) => {
            hasher.update(&[12]);
            hasher.update(v);
        }
        Value::Bytea(v) => {
            hasher.update(&[11]);
            hasher.update(&(v.len() as u64).to_le_bytes());
            hasher.update(v);
        }
        Value::Array(values) => {
            hasher.update(&[7]);
            hasher.update(&(values.len() as u64).to_le_bytes());
            for inner in values {
                hash_value(hasher, inner);
            }
        }
        Value::Row { values, .. } => {
            hasher.update(&[8]);
            hasher.update(&(values.len() as u64).to_le_bytes());
            for inner in values {
                hash_value(hasher, inner);
            }
        }
        Value::Null => {
            hasher.update(&[9]);
        }
    }
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
        ColumnType::Double => {
            hasher.update(&[10]);
        }
        ColumnType::Boolean => {
            hasher.update(&[3]);
        }
        ColumnType::Text => {
            hasher.update(&[4]);
        }
        ColumnType::Enum { variants } => {
            hasher.update(&[9]);
            // Enum variant ordering is normalized for hashing.
            let mut normalized = variants.clone();
            normalized.sort();
            normalized.dedup();
            hasher.update(&(normalized.len() as u64).to_le_bytes());
            for variant in normalized {
                hasher.update(variant.as_bytes());
                hasher.update(&[0]);
            }
        }
        ColumnType::Timestamp => {
            hasher.update(&[5]);
        }
        ColumnType::Uuid => {
            hasher.update(&[6]);
        }
        ColumnType::BatchId => {
            hasher.update(&[12]);
        }
        ColumnType::Bytea => {
            hasher.update(&[10]);
        }
        ColumnType::Json { schema } => {
            hasher.update(&[11]);
            match schema {
                Some(schema) => {
                    hasher.update(&[1]);
                    if let Ok(encoded) = serde_json::to_vec(schema) {
                        hasher.update(&(encoded.len() as u64).to_le_bytes());
                        hasher.update(&encoded);
                    } else {
                        hasher.update(&0u64.to_le_bytes());
                    }
                }
                None => {
                    hasher.update(&[0]);
                }
            }
        }
        ColumnType::Array { element: elem } => {
            hasher.update(&[7]);
            hash_column_type(hasher, elem);
        }
        ColumnType::Row { columns: desc } => {
            hasher.update(&[8]);
            hash_row_descriptor(hasher, desc);
        }
    }
}

/// Simple hex encoding/decoding (avoiding external crate).
pub mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }

    pub fn decode(s: &str) -> Result<Vec<u8>, &'static str> {
        if !s.len().is_multiple_of(2) {
            return Err("hex string must have even length");
        }
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| "invalid hex character"))
            .collect()
    }
}

// ============================================================================
// Composed Branch Name - Environment-qualified branch naming
// ============================================================================

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

        // Validate hash is 12 hex chars (6 bytes)
        if hash_str.len() != 12 || !hash_str.chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }

        // We can't fully reconstruct the hash from just 12 chars,
        // so we store a partial hash. For matching purposes, we use a zeroed hash
        // with the short portion filled in.
        let mut hash_bytes = [0u8; 32];
        if let Ok(bytes) = hex_decode(hash_str) {
            hash_bytes[..6].copy_from_slice(&bytes);
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
