use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use super::*;

// ============================================================================
// Schema Hashing - Content-addressed schema identification
// ============================================================================

#[repr(u8)]
#[derive(Clone, Copy)]
enum ColumnTypeHashTag {
    Integer = 1,
    BigInt = 2,
    Boolean = 3,
    Text = 4,
    Timestamp = 5,
    Uuid = 6,
    Array = 7,
    Row = 8,
    Enum = 9,
    Double = 10,
    Json = 11,
    TransactionId = 12,
    EnumPayload = 13,
    ScalarEnum = 14,
    CatalogueEnumPayload = 15,
    Bytea = 16,
}

#[repr(u8)]
#[derive(Clone, Copy)]
enum ValueHashTag {
    Integer = 1,
    BigInt = 2,
    Boolean = 3,
    Text = 4,
    Timestamp = 5,
    Uuid = 6,
    Array = 7,
    Row = 8,
    Null = 9,
    Double = 10,
    Bytea = 11,
    TransactionId = 12,
    Enum = 14,
}

#[derive(Clone, Copy)]
enum SchemaHashFormat {
    Current,
    LegacyByteaCollision,
}

impl SchemaHashFormat {
    fn bytea_tag(self) -> u8 {
        match self {
            Self::Current => ColumnTypeHashTag::Bytea as u8,
            Self::LegacyByteaCollision => ColumnTypeHashTag::Double as u8,
        }
    }
}

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
    pub fn to_object_id(&self) -> crate::tools::object::ObjectId {
        crate::tools::object::ObjectId::from_uuid(uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_DNS,
            &self.0,
        ))
    }

    /// Compute the current structural hash for a complete schema.
    ///
    /// New catalogue identities always use the current format. Historical
    /// Bytea identities can be derived explicitly with
    /// [`SchemaHash::compute_legacy_bytea`] for catalogue resolution and a
    /// durable migration edge.
    pub fn compute(schema: &Schema) -> Self {
        Self::compute_with_format(schema, SchemaHashFormat::Current)
    }

    /// Compute the historical schema identity where `Bytea` shared `Double`'s
    /// column-type tag.
    ///
    /// This is a read/migration compatibility operation only. New catalogue
    /// entries must use [`SchemaHash::compute`].
    pub fn compute_legacy_bytea(schema: &Schema) -> Self {
        Self::compute_with_format(schema, SchemaHashFormat::LegacyByteaCollision)
    }

    fn compute_with_format(schema: &Schema, format: SchemaHashFormat) -> Self {
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
            hash_row_descriptor_with_format(&mut hasher, &table_schema.columns, format);

            if let Some(indexed_columns) = &table_schema.indexed_columns {
                // `None` must hash exactly like pre-index-override schemas so
                // existing data keeps the same schema-qualified branch names.
                hasher.update(&[1]);
                let mut columns: Vec<_> = indexed_columns.iter().map(|c| c.as_str()).collect();
                columns.sort_unstable();
                for column in columns {
                    hasher.update(column.as_bytes());
                    hasher.update(&[0]);
                }
            }

            if !table_schema.branch_by.is_empty() {
                hasher.update(b"branch_by\0");
                hasher.update(
                    &serde_json::to_vec(&table_schema.branch_by)
                        .expect("branch bindings always serialize"),
                );
                hasher.update(&[0]);
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
    hash_row_descriptor_with_format(hasher, descriptor, SchemaHashFormat::Current);
}

fn hash_row_descriptor_with_format(
    hasher: &mut blake3::Hasher,
    descriptor: &RowDescriptor,
    format: SchemaHashFormat,
) {
    for col in &descriptor.columns {
        hash_column_descriptor(hasher, col, format);
    }
}

/// Hash a single ColumnDescriptor.
fn hash_column_descriptor(
    hasher: &mut blake3::Hasher,
    col: &ColumnDescriptor,
    format: SchemaHashFormat,
) {
    // Name
    hasher.update(col.name.as_str().as_bytes());
    hasher.update(&[0]);

    // Type
    hash_column_type(hasher, &col.column_type, format);

    // Nullable flag
    hasher.update(&[col.nullable as u8]);

    // References (FK)
    if let Some(ref table) = col.references {
        hasher.update(&[1]);
        hasher.update(table.as_str().as_bytes());
    } else {
        hasher.update(&[0]);
    }

    // Absence of a default must hash like pre-default schemas so historical
    // schema-qualified branch names remain stable.
    if let Some(default) = &col.default {
        hasher.update(&[1]);
        hash_value(hasher, default);
    }

    if let Some(strategy) = col.merge_strategy {
        hasher.update(&[1]);
        match strategy {
            ColumnMergeStrategy::Counter => {
                hasher.update(&[1]);
            }
            ColumnMergeStrategy::GSet => {
                hasher.update(&[2]);
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
            hasher.update(&[ValueHashTag::Integer as u8]);
            hasher.update(&v.to_le_bytes());
        }
        Value::BigInt(v) => {
            hasher.update(&[ValueHashTag::BigInt as u8]);
            hasher.update(&v.to_le_bytes());
        }
        Value::Double(v) => {
            hasher.update(&[ValueHashTag::Double as u8]);
            hasher.update(&v.to_le_bytes());
        }
        Value::Boolean(v) => {
            hasher.update(&[ValueHashTag::Boolean as u8, *v as u8]);
        }
        Value::Text(v) => {
            hasher.update(&[ValueHashTag::Text as u8]);
            hasher.update(v.as_bytes());
            hasher.update(&[0]);
        }
        Value::Timestamp(v) => {
            hasher.update(&[ValueHashTag::Timestamp as u8]);
            hasher.update(&v.to_le_bytes());
        }
        Value::Uuid(v) => {
            hasher.update(&[ValueHashTag::Uuid as u8]);
            hasher.update(v.uuid().as_bytes());
        }
        Value::TransactionId(v) => {
            hasher.update(&[ValueHashTag::TransactionId as u8]);
            hasher.update(v);
        }
        Value::Bytea(v) => {
            hasher.update(&[ValueHashTag::Bytea as u8]);
            hasher.update(&(v.len() as u64).to_le_bytes());
            hasher.update(v);
        }
        Value::Array(values) => {
            hasher.update(&[ValueHashTag::Array as u8]);
            hasher.update(&(values.len() as u64).to_le_bytes());
            for inner in values {
                hash_value(hasher, inner);
            }
        }
        Value::Row { values, .. } => {
            hasher.update(&[ValueHashTag::Row as u8]);
            hasher.update(&(values.len() as u64).to_le_bytes());
            for inner in values {
                hash_value(hasher, inner);
            }
        }
        Value::Enum { case, values } => {
            hasher.update(&[ValueHashTag::Enum as u8]);
            hasher.update(case.as_bytes());
            hasher.update(&[0]);
            hasher.update(&(values.len() as u64).to_le_bytes());
            for inner in values {
                hash_value(hasher, inner);
            }
        }
        Value::Null => {
            hasher.update(&[ValueHashTag::Null as u8]);
        }
    }
}

/// Hash a ColumnType recursively (for Array and Row types).
fn hash_column_type(hasher: &mut blake3::Hasher, col_type: &ColumnType, format: SchemaHashFormat) {
    match col_type {
        ColumnType::Integer => {
            hasher.update(&[ColumnTypeHashTag::Integer as u8]);
        }
        ColumnType::BigInt => {
            hasher.update(&[ColumnTypeHashTag::BigInt as u8]);
        }
        ColumnType::Double => {
            hasher.update(&[ColumnTypeHashTag::Double as u8]);
        }
        ColumnType::Boolean => {
            hasher.update(&[ColumnTypeHashTag::Boolean as u8]);
        }
        ColumnType::Text => {
            hasher.update(&[ColumnTypeHashTag::Text as u8]);
        }
        ColumnType::Enum { variants } => {
            hasher.update(&[ColumnTypeHashTag::Enum as u8]);
            // Scalar enum order assigns durable discriminant tags, so it is
            // structural schema identity rather than presentation metadata.
            hasher.update(&(variants.len() as u64).to_le_bytes());
            for variant in variants {
                hasher.update(variant.as_bytes());
                hasher.update(&[0]);
            }
        }
        ColumnType::ScalarEnum { name, variants } => {
            hasher.update(&[ColumnTypeHashTag::ScalarEnum as u8]);
            hasher.update(name.as_bytes());
            hasher.update(&[0]);
            hasher.update(&(variants.len() as u64).to_le_bytes());
            for variant in variants {
                hasher.update(variant.as_bytes());
                hasher.update(&[0]);
            }
        }
        ColumnType::CatalogueEnumPayload { name, cases } => {
            hasher.update(&[ColumnTypeHashTag::CatalogueEnumPayload as u8]);
            hasher.update(name.as_bytes());
            hasher.update(&[0]);
            hasher.update(&(cases.len() as u64).to_le_bytes());
            for case in cases {
                hasher.update(case.name.as_bytes());
                hasher.update(&[0]);
                hasher.update(&(case.fields.len() as u64).to_le_bytes());
                for field in &case.fields {
                    hasher.update(field.name.as_str().as_bytes());
                    hasher.update(&[0]);
                    hash_column_type(hasher, &field.column_type, format);
                    hasher.update(&[u8::from(field.nullable)]);
                }
            }
        }
        ColumnType::EnumPayload { cases } => {
            hasher.update(&[ColumnTypeHashTag::EnumPayload as u8]);
            hasher.update(&(cases.len() as u64).to_le_bytes());
            for case in cases {
                hasher.update(case.name.as_bytes());
                hasher.update(&[0]);
                hasher.update(&(case.fields.len() as u64).to_le_bytes());
                for field in &case.fields {
                    hasher.update(field.name.as_str().as_bytes());
                    hasher.update(&[0]);
                    hash_column_type(hasher, &field.column_type, format);
                    hasher.update(&[u8::from(field.nullable)]);
                }
            }
        }
        ColumnType::Timestamp => {
            hasher.update(&[ColumnTypeHashTag::Timestamp as u8]);
        }
        ColumnType::Uuid => {
            hasher.update(&[ColumnTypeHashTag::Uuid as u8]);
        }
        ColumnType::TransactionId => {
            hasher.update(&[ColumnTypeHashTag::TransactionId as u8]);
        }
        ColumnType::Bytea => {
            hasher.update(&[format.bytea_tag()]);
        }
        ColumnType::Json { schema } => {
            hasher.update(&[ColumnTypeHashTag::Json as u8]);
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
            hasher.update(&[ColumnTypeHashTag::Array as u8]);
            hash_column_type(hasher, elem, format);
        }
        ColumnType::Row { columns: desc } => {
            hasher.update(&[ColumnTypeHashTag::Row as u8]);
            hash_row_descriptor_with_format(hasher, desc, format);
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
