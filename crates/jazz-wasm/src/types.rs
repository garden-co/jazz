//! Type bridges for WASM boundary.
//!
//! Serializable versions of key Groove types for crossing the WASM/JS boundary.
//! Types with `Tsify` derive automatically generate TypeScript definitions.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tsify::Tsify;

// ============================================================================
// Value Serialization
// ============================================================================

/// Value type for WASM boundary (mirrors groove::query_manager::types::Value).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
#[serde(tag = "type", content = "value")]
pub enum WasmValue {
    Integer(i32),
    BigInt(i64),
    Boolean(bool),
    Text(String),
    Timestamp(u64),
    Uuid(String), // UUID as string for JS compatibility
    Array(Vec<WasmValue>),
    Row(Vec<WasmValue>),
    Null,
}

impl From<groove::query_manager::types::Value> for WasmValue {
    fn from(v: groove::query_manager::types::Value) -> Self {
        use groove::query_manager::types::Value;
        match v {
            Value::Integer(i) => WasmValue::Integer(i),
            Value::BigInt(i) => WasmValue::BigInt(i),
            Value::Boolean(b) => WasmValue::Boolean(b),
            Value::Text(s) => WasmValue::Text(s),
            Value::Timestamp(t) => WasmValue::Timestamp(t),
            Value::Uuid(id) => WasmValue::Uuid(id.uuid().to_string()),
            Value::Array(arr) => WasmValue::Array(arr.into_iter().map(Into::into).collect()),
            Value::Row(row) => WasmValue::Row(row.into_iter().map(Into::into).collect()),
            Value::Null => WasmValue::Null,
        }
    }
}

impl TryFrom<WasmValue> for groove::query_manager::types::Value {
    type Error = String;

    fn try_from(v: WasmValue) -> Result<Self, Self::Error> {
        use groove::object::ObjectId;
        use groove::query_manager::types::Value;

        Ok(match v {
            WasmValue::Integer(i) => Value::Integer(i),
            WasmValue::BigInt(i) => Value::BigInt(i),
            WasmValue::Boolean(b) => Value::Boolean(b),
            WasmValue::Text(s) => Value::Text(s),
            WasmValue::Timestamp(t) => Value::Timestamp(t),
            WasmValue::Uuid(s) => {
                let uuid = uuid::Uuid::parse_str(&s).map_err(|e| format!("Invalid UUID: {}", e))?;
                Value::Uuid(ObjectId::from_uuid(uuid))
            }
            WasmValue::Array(arr) => {
                let converted: Result<Vec<_>, _> = arr.into_iter().map(TryInto::try_into).collect();
                Value::Array(converted?)
            }
            WasmValue::Row(row) => {
                let converted: Result<Vec<_>, _> = row.into_iter().map(TryInto::try_into).collect();
                Value::Row(converted?)
            }
            WasmValue::Null => Value::Null,
        })
    }
}

// ============================================================================
// Row Delta Serialization
// ============================================================================

/// Serializable row for WASM boundary.
#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct WasmRow {
    pub id: String, // ObjectId as UUID string
    pub values: Vec<WasmValue>,
}

/// Row + post-delta index for added rows.
#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct WasmIndexedRow {
    pub row: WasmRow,
    pub index: usize,
}

/// Updated row pair with pre/post indices.
#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct WasmUpdatedIndexedRow {
    pub old_row: WasmRow,
    pub new_row: WasmRow,
    pub old_index: usize,
    pub new_index: usize,
}

/// Removed row + pre-delta index.
#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct WasmRemovedIndexedRow {
    pub row: WasmRow,
    pub index: usize,
}

/// Delta for row-level changes (mirrors groove::query_manager::types::RowDelta).
#[derive(Debug, Clone, Default, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct WasmRowDelta {
    pub added: Vec<WasmIndexedRow>,
    pub removed: Vec<WasmRemovedIndexedRow>,
    pub updated: Vec<WasmUpdatedIndexedRow>,
    pub pending: bool,
}

// ============================================================================
// Schema Serialization
// ============================================================================

/// Serializable column type for WASM boundary.
#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
#[serde(tag = "type")]
pub enum WasmColumnType {
    Integer,
    BigInt,
    Boolean,
    Text,
    Timestamp,
    Uuid,
    Array { element: Box<WasmColumnType> },
    Row { columns: Vec<WasmColumnDescriptor> },
}

/// Serializable column descriptor for WASM boundary.
#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct WasmColumnDescriptor {
    pub name: String,
    pub column_type: WasmColumnType,
    pub nullable: bool,
    #[tsify(optional)]
    pub references: Option<String>,
}

/// Serializable table schema for WASM boundary.
#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct WasmTableSchema {
    pub columns: Vec<WasmColumnDescriptor>,
}

/// Serializable schema for WASM boundary.
#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct WasmSchema {
    #[tsify(type = "Record<string, WasmTableSchema>")]
    pub tables: HashMap<String, WasmTableSchema>,
}

impl From<groove::query_manager::types::ColumnType> for WasmColumnType {
    fn from(ct: groove::query_manager::types::ColumnType) -> Self {
        use groove::query_manager::types::ColumnType;
        match ct {
            ColumnType::Integer => WasmColumnType::Integer,
            ColumnType::BigInt => WasmColumnType::BigInt,
            ColumnType::Boolean => WasmColumnType::Boolean,
            ColumnType::Text => WasmColumnType::Text,
            ColumnType::Timestamp => WasmColumnType::Timestamp,
            ColumnType::Uuid => WasmColumnType::Uuid,
            ColumnType::Array(elem) => WasmColumnType::Array {
                element: Box::new((*elem).into()),
            },
            ColumnType::Row(desc) => WasmColumnType::Row {
                columns: desc
                    .columns
                    .into_iter()
                    .map(|c| WasmColumnDescriptor {
                        name: c.name.as_str().to_string(),
                        column_type: c.column_type.into(),
                        nullable: c.nullable,
                        references: c.references.map(|r| r.as_str().to_string()),
                    })
                    .collect(),
            },
        }
    }
}

impl From<&groove::query_manager::types::Schema> for WasmSchema {
    fn from(schema: &groove::query_manager::types::Schema) -> Self {
        let tables = schema
            .iter()
            .map(|(name, ts)| {
                let columns = ts
                    .descriptor
                    .columns
                    .iter()
                    .map(|c| WasmColumnDescriptor {
                        name: c.name.as_str().to_string(),
                        column_type: c.column_type.clone().into(),
                        nullable: c.nullable,
                        references: c.references.map(|r| r.as_str().to_string()),
                    })
                    .collect();
                (name.as_str().to_string(), WasmTableSchema { columns })
            })
            .collect();
        WasmSchema { tables }
    }
}

/// Convert WasmSchema back to Groove Schema.
impl TryFrom<WasmSchema> for groove::query_manager::types::Schema {
    type Error = String;

    fn try_from(ws: WasmSchema) -> Result<Self, Self::Error> {
        use groove::query_manager::types::{
            ColumnDescriptor, ColumnType, RowDescriptor, TableName, TableSchema,
        };

        fn wasm_type_to_groove(wt: WasmColumnType) -> ColumnType {
            match wt {
                WasmColumnType::Integer => ColumnType::Integer,
                WasmColumnType::BigInt => ColumnType::BigInt,
                WasmColumnType::Boolean => ColumnType::Boolean,
                WasmColumnType::Text => ColumnType::Text,
                WasmColumnType::Timestamp => ColumnType::Timestamp,
                WasmColumnType::Uuid => ColumnType::Uuid,
                WasmColumnType::Array { element } => {
                    ColumnType::Array(Box::new(wasm_type_to_groove(*element)))
                }
                WasmColumnType::Row { columns } => {
                    let cols = columns
                        .into_iter()
                        .map(|c| {
                            let mut cd =
                                ColumnDescriptor::new(&c.name, wasm_type_to_groove(c.column_type));
                            if c.nullable {
                                cd = cd.nullable();
                            }
                            if let Some(ref_table) = c.references {
                                cd = cd.references(&ref_table);
                            }
                            cd
                        })
                        .collect();
                    ColumnType::Row(Box::new(RowDescriptor::new(cols)))
                }
            }
        }

        let mut schema = groove::query_manager::types::Schema::new();
        for (table_name, table_schema) in ws.tables {
            let columns = table_schema
                .columns
                .into_iter()
                .map(|c| {
                    let mut cd = ColumnDescriptor::new(&c.name, wasm_type_to_groove(c.column_type));
                    if c.nullable {
                        cd = cd.nullable();
                    }
                    if let Some(ref_table) = c.references {
                        cd = cd.references(&ref_table);
                    }
                    cd
                })
                .collect();
            schema.insert(
                TableName::new(&table_name),
                TableSchema::new(RowDescriptor::new(columns)),
            );
        }
        Ok(schema)
    }
}
