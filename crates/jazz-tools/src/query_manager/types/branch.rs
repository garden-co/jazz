use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use crate::object::BranchName;

use super::*;

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

    /// Get a 12-character hex prefix for display/filenames.
    /// Uses 6 bytes (48 bits) for good collision resistance.
    pub fn short(&self) -> String {
        hex::encode(&self.0[..6])
    }

    pub fn parse_hex(hex_str: &str) -> Option<Self> {
        let bytes = hex::decode(hex_str).ok()?;
        if bytes.len() != 32 {
            return None;
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Some(Self(arr))
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

            // Hash row descriptor (columns sorted by name)
            hash_row_descriptor(&mut hasher, &table_schema.columns);
        }

        Self(*hasher.finalize().as_bytes())
    }
}

impl std::fmt::Display for SchemaHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}

impl Serialize for SchemaHash {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex::encode(&self.0))
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

/// Hash a RowDescriptor into a hasher, sorting columns by name for order-independence.
pub(crate) fn hash_row_descriptor(hasher: &mut blake3::Hasher, descriptor: &RowDescriptor) {
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

    // Schema-level default
    if let Some(default) = &col.default {
        hasher.update(&[1]);
        hash_value(hasher, default);
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

/// Device-generated identifier for a batch of writes.
///
/// Text form is a single branch-safe segment: `b` + 32 lowercase hex chars.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BatchId(pub [u8; 16]);

/// Dense per-prefix ordinal for a batch in hot in-memory/storage catalogs.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub struct BatchOrd(pub u32);

impl BatchOrd {
    pub fn as_u32(self) -> u32 {
        self.0
    }

    pub fn as_usize(self) -> usize {
        self.0 as usize
    }
}

impl BatchId {
    pub fn new() -> Self {
        Self::from_uuid(Uuid::now_v7())
    }

    pub fn nil() -> Self {
        Self::from_uuid(Uuid::nil())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(*uuid.as_bytes())
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub fn to_uuid(&self) -> Uuid {
        Uuid::from_bytes(self.0)
    }

    pub fn branch_segment(&self) -> String {
        format!("b{}", hex::encode(&self.0))
    }

    pub fn parse_segment(segment: &str) -> Option<Self> {
        let raw = segment.strip_prefix('b')?;
        if raw.len() != 32 || !raw.chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }

        let bytes = hex_decode(raw).ok()?;
        let mut arr = [0u8; 16];
        arr.copy_from_slice(&bytes);
        Some(Self(arr))
    }
}

impl Default for BatchId {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for BatchId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.branch_segment())
    }
}

impl<'de> Deserialize<'de> for BatchId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::parse_segment(&s).ok_or_else(|| serde::de::Error::custom("invalid BatchId segment"))
    }
}

impl std::fmt::Display for BatchId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.branch_segment())
    }
}

/// Shared branch prefix across all batches for the same env/schema/user branch.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BranchPrefixName {
    pub env: String,
    pub schema_hash: SchemaHash,
    pub user_branch: String,
}

impl BranchPrefixName {
    pub fn new(env: &str, schema_hash: SchemaHash, user_branch: &str) -> Self {
        Self {
            env: env.to_string(),
            schema_hash,
            user_branch: user_branch.to_string(),
        }
    }

    pub fn from_schema(env: &str, schema: &Schema, user_branch: &str) -> Self {
        Self::new(env, SchemaHash::compute(schema), user_branch)
    }

    pub fn branch_prefix(&self) -> String {
        format!("{}-{}-{}", self.env, self.schema_hash, self.user_branch)
    }

    /// Prefix shared by all batches: `{env}-{schemaHash}-{userBranch}-`.
    pub fn to_batch_prefix(&self) -> String {
        format!("{}-", self.branch_prefix())
    }

    pub fn with_batch_id(&self, batch_id: BatchId) -> ComposedBranchName {
        ComposedBranchName::new(&self.env, self.schema_hash, &self.user_branch, batch_id)
    }

    pub fn parse(name: &BranchName) -> Option<Self> {
        let s = name.as_str();
        let parts: Vec<&str> = s.splitn(3, '-').collect();
        if parts.len() != 3 {
            return None;
        }

        let env = parts[0];
        let schema_hash = SchemaHash::parse_hex(parts[1])?;
        let user_branch = parts[2];
        if user_branch.is_empty() {
            return None;
        }

        Some(Self::new(env, schema_hash, user_branch))
    }
}

impl std::fmt::Display for BranchPrefixName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.branch_prefix())
    }
}

/// Internal branch reference used by query compilation and index access.
///
/// `BatchBranchKey` is the compact identity form used in hot in-memory maps.
/// `QueryBranchRef` adds a cached interned full branch name for APIs that still
/// operate on string-shaped branch ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BatchBranchKey {
    prefix_name: BranchName,
    batch_id: BatchId,
}

impl BatchBranchKey {
    pub fn from_prefix_and_batch(prefix: &BranchPrefixName, batch_id: BatchId) -> Self {
        Self {
            prefix_name: BranchName::new(prefix.branch_prefix()),
            batch_id,
        }
    }

    pub fn from_prefix_name_and_batch(prefix_name: BranchName, batch_id: BatchId) -> Self {
        Self {
            prefix_name,
            batch_id,
        }
    }

    pub fn from_branch_name(branch_name: impl Into<BranchName>) -> Self {
        let branch_name = branch_name.into();
        let composed_branch = ComposedBranchName::parse(&branch_name)
            .expect("branch names must use composed batch format");
        Self {
            prefix_name: BranchName::new(composed_branch.prefix().branch_prefix()),
            batch_id: composed_branch.batch_id,
        }
    }

    pub fn branch_name(&self) -> BranchName {
        BranchName::new(format!("{}-{}", self.prefix_name.as_str(), self.batch_id))
    }

    pub fn prefix_name(&self) -> BranchName {
        self.prefix_name
    }

    pub fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    pub fn as_query_branch_ref(&self) -> QueryBranchRef {
        QueryBranchRef::from_batch_branch_key(*self)
    }
}

impl std::fmt::Display for BatchBranchKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.prefix_name.as_str(), self.batch_id)
    }
}

/// Internal branch reference used by query compilation and index access.
///
/// Composed batch branches keep their shared prefix and batch id in structured
/// form, while also caching the full interned branch name for object lookups
/// and row provenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryBranchRef {
    key: BatchBranchKey,
    branch_name: BranchName,
}

impl QueryBranchRef {
    pub fn from_prefix_and_batch(prefix: &BranchPrefixName, batch_id: BatchId) -> Self {
        let key = BatchBranchKey::from_prefix_and_batch(prefix, batch_id);
        let branch_name = prefix.with_batch_id(batch_id).to_branch_name();
        Self { key, branch_name }
    }

    pub fn from_prefix_name_and_batch(prefix_name: BranchName, batch_id: BatchId) -> Self {
        let key = BatchBranchKey::from_prefix_name_and_batch(prefix_name, batch_id);
        let branch_name = key.branch_name();
        Self { key, branch_name }
    }

    pub fn from_branch_name(branch_name: impl Into<BranchName>) -> Self {
        let branch_name = branch_name.into();
        let key = BatchBranchKey::from_branch_name(branch_name);
        Self { branch_name, key }
    }

    pub fn branch_name(&self) -> BranchName {
        self.branch_name
    }

    pub fn as_str(&self) -> &str {
        self.branch_name.as_str()
    }

    pub fn batch_branch_key(&self) -> BatchBranchKey {
        self.key
    }

    pub fn prefix_name(&self) -> BranchName {
        self.key.prefix_name()
    }

    pub fn batch_id(&self) -> BatchId {
        self.key.batch_id()
    }

    pub fn from_batch_branch_key(key: BatchBranchKey) -> Self {
        Self {
            branch_name: key.branch_name(),
            key,
        }
    }
}

impl std::fmt::Display for QueryBranchRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A branch name composed of environment, schema hash, user branch, and batch id.
/// Format: `{env}-{fullSchemaHash}-{userBranch}-{batchId}`
/// Example: `dev-a1b2c3d4e5f6...-main-b018d6f2...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ComposedBranchName {
    pub env: String,
    pub schema_hash: SchemaHash,
    pub user_branch: String,
    pub batch_id: BatchId,
}

impl ComposedBranchName {
    /// Create a new composed branch name.
    pub fn new(env: &str, schema_hash: SchemaHash, user_branch: &str, batch_id: BatchId) -> Self {
        Self {
            env: env.to_string(),
            schema_hash,
            user_branch: user_branch.to_string(),
            batch_id,
        }
    }

    /// Create from a schema, computing the hash automatically.
    pub fn from_schema(env: &str, schema: &Schema, user_branch: &str, batch_id: BatchId) -> Self {
        Self::new(env, SchemaHash::compute(schema), user_branch, batch_id)
    }

    pub fn prefix(&self) -> BranchPrefixName {
        BranchPrefixName::new(&self.env, self.schema_hash, &self.user_branch)
    }

    /// Convert to a BranchName string.
    pub fn to_branch_name(&self) -> BranchName {
        BranchName::new(format!(
            "{}{}",
            self.prefix().to_batch_prefix(),
            self.batch_id.branch_segment()
        ))
    }

    /// Parse a BranchName back into its components.
    /// Returns None if the format doesn't match.
    pub fn parse(name: &BranchName) -> Option<Self> {
        let (prefix, batch_segment) = name.as_str().rsplit_once('-')?;
        let prefix = BranchPrefixName::parse(&BranchName::new(prefix))?;
        let batch_id = BatchId::parse_segment(batch_segment)?;

        Some(Self {
            env: prefix.env,
            schema_hash: prefix.schema_hash,
            user_branch: prefix.user_branch,
            batch_id,
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
        write!(f, "{}", self.to_branch_name())
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
