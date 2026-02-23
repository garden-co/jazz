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

/// Delta for row-level changes (mirrors groove::query_manager::types::RowDelta).
#[derive(Debug, Clone, Default, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct WasmRowDelta {
    pub added: Vec<WasmRow>,
    pub removed: Vec<WasmRow>,
    pub updated: Vec<(WasmRow, WasmRow)>,
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

#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
#[serde(tag = "type")]
pub enum WasmPolicyValue {
    Literal { value: WasmValue },
    SessionRef { path: Vec<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub enum WasmCmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub enum WasmPolicyOperation {
    Select,
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
#[serde(tag = "type")]
pub enum WasmPolicyExpr {
    Cmp {
        column: String,
        op: WasmCmpOp,
        value: WasmPolicyValue,
    },
    IsNull {
        column: String,
    },
    IsNotNull {
        column: String,
    },
    In {
        column: String,
        session_path: Vec<String>,
    },
    Exists {
        table: String,
        condition: Box<WasmPolicyExpr>,
    },
    ExistsRel {
        #[tsify(type = "any")]
        rel: serde_json::Value,
    },
    Inherits {
        operation: WasmPolicyOperation,
        via_column: String,
        #[tsify(optional)]
        max_depth: Option<u32>,
    },
    And {
        exprs: Vec<WasmPolicyExpr>,
    },
    Or {
        exprs: Vec<WasmPolicyExpr>,
    },
    Not {
        expr: Box<WasmPolicyExpr>,
    },
    True,
    False,
}

#[derive(Debug, Clone, Serialize, Deserialize, Tsify, Default)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct WasmOperationPolicy {
    #[tsify(optional)]
    pub using: Option<WasmPolicyExpr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[tsify(optional)]
    pub with_check: Option<WasmPolicyExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Tsify, Default)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct WasmTablePolicies {
    #[tsify(optional)]
    pub select: Option<WasmOperationPolicy>,
    #[tsify(optional)]
    pub insert: Option<WasmOperationPolicy>,
    #[tsify(optional)]
    pub update: Option<WasmOperationPolicy>,
    #[tsify(optional)]
    pub delete: Option<WasmOperationPolicy>,
}

/// Serializable table schema for WASM boundary.
#[derive(Debug, Clone, Serialize, Deserialize, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct WasmTableSchema {
    pub columns: Vec<WasmColumnDescriptor>,
    #[tsify(optional)]
    pub policies: Option<WasmTablePolicies>,
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

impl From<groove::query_manager::policy::PolicyValue> for WasmPolicyValue {
    fn from(value: groove::query_manager::policy::PolicyValue) -> Self {
        match value {
            groove::query_manager::policy::PolicyValue::Literal(v) => WasmPolicyValue::Literal {
                value: WasmValue::from(v),
            },
            groove::query_manager::policy::PolicyValue::SessionRef(path) => {
                WasmPolicyValue::SessionRef { path }
            }
        }
    }
}

impl TryFrom<WasmPolicyValue> for groove::query_manager::policy::PolicyValue {
    type Error = String;

    fn try_from(value: WasmPolicyValue) -> Result<Self, Self::Error> {
        match value {
            WasmPolicyValue::Literal { value } => {
                Ok(groove::query_manager::policy::PolicyValue::Literal(
                    groove::query_manager::types::Value::try_from(value)?,
                ))
            }
            WasmPolicyValue::SessionRef { path } => {
                Ok(groove::query_manager::policy::PolicyValue::SessionRef(path))
            }
        }
    }
}

impl From<groove::query_manager::policy::CmpOp> for WasmCmpOp {
    fn from(op: groove::query_manager::policy::CmpOp) -> Self {
        match op {
            groove::query_manager::policy::CmpOp::Eq => WasmCmpOp::Eq,
            groove::query_manager::policy::CmpOp::Ne => WasmCmpOp::Ne,
            groove::query_manager::policy::CmpOp::Lt => WasmCmpOp::Lt,
            groove::query_manager::policy::CmpOp::Le => WasmCmpOp::Le,
            groove::query_manager::policy::CmpOp::Gt => WasmCmpOp::Gt,
            groove::query_manager::policy::CmpOp::Ge => WasmCmpOp::Ge,
        }
    }
}

impl From<WasmCmpOp> for groove::query_manager::policy::CmpOp {
    fn from(op: WasmCmpOp) -> Self {
        match op {
            WasmCmpOp::Eq => groove::query_manager::policy::CmpOp::Eq,
            WasmCmpOp::Ne => groove::query_manager::policy::CmpOp::Ne,
            WasmCmpOp::Lt => groove::query_manager::policy::CmpOp::Lt,
            WasmCmpOp::Le => groove::query_manager::policy::CmpOp::Le,
            WasmCmpOp::Gt => groove::query_manager::policy::CmpOp::Gt,
            WasmCmpOp::Ge => groove::query_manager::policy::CmpOp::Ge,
        }
    }
}

impl From<groove::query_manager::policy::Operation> for WasmPolicyOperation {
    fn from(op: groove::query_manager::policy::Operation) -> Self {
        match op {
            groove::query_manager::policy::Operation::Select => WasmPolicyOperation::Select,
            groove::query_manager::policy::Operation::Insert => WasmPolicyOperation::Insert,
            groove::query_manager::policy::Operation::Update => WasmPolicyOperation::Update,
            groove::query_manager::policy::Operation::Delete => WasmPolicyOperation::Delete,
        }
    }
}

impl From<WasmPolicyOperation> for groove::query_manager::policy::Operation {
    fn from(op: WasmPolicyOperation) -> Self {
        match op {
            WasmPolicyOperation::Select => groove::query_manager::policy::Operation::Select,
            WasmPolicyOperation::Insert => groove::query_manager::policy::Operation::Insert,
            WasmPolicyOperation::Update => groove::query_manager::policy::Operation::Update,
            WasmPolicyOperation::Delete => groove::query_manager::policy::Operation::Delete,
        }
    }
}

impl From<groove::query_manager::policy::PolicyExpr> for WasmPolicyExpr {
    fn from(expr: groove::query_manager::policy::PolicyExpr) -> Self {
        match expr {
            groove::query_manager::policy::PolicyExpr::Cmp { column, op, value } => {
                WasmPolicyExpr::Cmp {
                    column,
                    op: op.into(),
                    value: value.into(),
                }
            }
            groove::query_manager::policy::PolicyExpr::IsNull { column } => {
                WasmPolicyExpr::IsNull { column }
            }
            groove::query_manager::policy::PolicyExpr::IsNotNull { column } => {
                WasmPolicyExpr::IsNotNull { column }
            }
            groove::query_manager::policy::PolicyExpr::In {
                column,
                session_path,
            } => WasmPolicyExpr::In {
                column,
                session_path,
            },
            groove::query_manager::policy::PolicyExpr::Exists { table, condition } => {
                WasmPolicyExpr::Exists {
                    table,
                    condition: Box::new((*condition).into()),
                }
            }
            groove::query_manager::policy::PolicyExpr::ExistsRel { rel } => {
                WasmPolicyExpr::ExistsRel {
                    rel: serde_json::to_value(rel).unwrap_or(serde_json::Value::Null),
                }
            }
            groove::query_manager::policy::PolicyExpr::Inherits {
                operation,
                via_column,
                max_depth,
            } => WasmPolicyExpr::Inherits {
                operation: operation.into(),
                via_column,
                max_depth: max_depth.map(|v| v as u32),
            },
            groove::query_manager::policy::PolicyExpr::And(exprs) => WasmPolicyExpr::And {
                exprs: exprs.into_iter().map(Into::into).collect(),
            },
            groove::query_manager::policy::PolicyExpr::Or(exprs) => WasmPolicyExpr::Or {
                exprs: exprs.into_iter().map(Into::into).collect(),
            },
            groove::query_manager::policy::PolicyExpr::Not(expr) => WasmPolicyExpr::Not {
                expr: Box::new((*expr).into()),
            },
            groove::query_manager::policy::PolicyExpr::True => WasmPolicyExpr::True,
            groove::query_manager::policy::PolicyExpr::False => WasmPolicyExpr::False,
        }
    }
}

impl TryFrom<WasmPolicyExpr> for groove::query_manager::policy::PolicyExpr {
    type Error = String;

    fn try_from(expr: WasmPolicyExpr) -> Result<Self, Self::Error> {
        Ok(match expr {
            WasmPolicyExpr::Cmp { column, op, value } => {
                groove::query_manager::policy::PolicyExpr::Cmp {
                    column,
                    op: op.into(),
                    value: value.try_into()?,
                }
            }
            WasmPolicyExpr::IsNull { column } => {
                groove::query_manager::policy::PolicyExpr::IsNull { column }
            }
            WasmPolicyExpr::IsNotNull { column } => {
                groove::query_manager::policy::PolicyExpr::IsNotNull { column }
            }
            WasmPolicyExpr::In {
                column,
                session_path,
            } => groove::query_manager::policy::PolicyExpr::In {
                column,
                session_path,
            },
            WasmPolicyExpr::Exists { table, condition } => {
                groove::query_manager::policy::PolicyExpr::Exists {
                    table,
                    condition: Box::new((*condition).try_into()?),
                }
            }
            WasmPolicyExpr::ExistsRel { rel } => {
                groove::query_manager::policy::PolicyExpr::ExistsRel {
                    rel: serde_json::from_value(rel)
                        .map_err(|err| format!("Invalid relation IR in ExistsRel: {err}"))?,
                }
            }
            WasmPolicyExpr::Inherits {
                operation,
                via_column,
                max_depth,
            } => groove::query_manager::policy::PolicyExpr::Inherits {
                operation: operation.into(),
                via_column,
                max_depth: max_depth.map(|v| v as usize),
            },
            WasmPolicyExpr::And { exprs } => groove::query_manager::policy::PolicyExpr::And(
                exprs
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            WasmPolicyExpr::Or { exprs } => groove::query_manager::policy::PolicyExpr::Or(
                exprs
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            WasmPolicyExpr::Not { expr } => {
                groove::query_manager::policy::PolicyExpr::Not(Box::new((*expr).try_into()?))
            }
            WasmPolicyExpr::True => groove::query_manager::policy::PolicyExpr::True,
            WasmPolicyExpr::False => groove::query_manager::policy::PolicyExpr::False,
        })
    }
}

impl From<groove::query_manager::types::OperationPolicy> for WasmOperationPolicy {
    fn from(policy: groove::query_manager::types::OperationPolicy) -> Self {
        Self {
            using: policy.using.map(Into::into),
            with_check: policy.with_check.map(Into::into),
        }
    }
}

impl TryFrom<WasmOperationPolicy> for groove::query_manager::types::OperationPolicy {
    type Error = String;

    fn try_from(policy: WasmOperationPolicy) -> Result<Self, Self::Error> {
        Ok(groove::query_manager::types::OperationPolicy {
            using: policy.using.map(TryInto::try_into).transpose()?,
            with_check: policy.with_check.map(TryInto::try_into).transpose()?,
        })
    }
}

impl From<groove::query_manager::types::TablePolicies> for WasmTablePolicies {
    fn from(policies: groove::query_manager::types::TablePolicies) -> Self {
        Self {
            select: Some(policies.select.into()),
            insert: Some(policies.insert.into()),
            update: Some(policies.update.into()),
            delete: Some(policies.delete.into()),
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
                let policies =
                    if ts.policies == groove::query_manager::types::TablePolicies::default() {
                        None
                    } else {
                        Some(ts.policies.clone().into())
                    };
                (
                    name.as_str().to_string(),
                    WasmTableSchema { columns, policies },
                )
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
            ColumnDescriptor, ColumnType, OperationPolicy, RowDescriptor, TableName, TablePolicies,
            TableSchema,
        };

        fn wasm_table_policies_to_groove(
            policies: Option<WasmTablePolicies>,
        ) -> Result<TablePolicies, String> {
            let Some(policies) = policies else {
                return Ok(TablePolicies::default());
            };

            let select: OperationPolicy = policies.select.unwrap_or_default().try_into()?;
            let insert: OperationPolicy = policies.insert.unwrap_or_default().try_into()?;
            let update: OperationPolicy = policies.update.unwrap_or_default().try_into()?;
            let delete: OperationPolicy = policies.delete.unwrap_or_default().try_into()?;

            Ok(TablePolicies {
                select,
                insert,
                update,
                delete,
            })
        }

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
            let policies = wasm_table_policies_to_groove(table_schema.policies)?;
            schema.insert(
                TableName::new(&table_name),
                TableSchema::with_policies(RowDescriptor::new(columns), policies),
            );
        }
        Ok(schema)
    }
}
