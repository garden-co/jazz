use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::query_manager::{
    policy::{CmpOp, Operation, PolicyExpr, PolicyValue},
    types::{ColumnType, Schema, TablePolicies, Value},
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum CatalogueWasmValue {
    Integer(i32),
    BigInt(i64),
    Double(f64),
    Boolean(bool),
    Text(String),
    Timestamp(u64),
    Uuid(String),
    Bytea(Vec<u8>),
    Array(Vec<CatalogueWasmValue>),
    Row(Vec<CatalogueWasmValue>),
    Null,
}

impl From<Value> for CatalogueWasmValue {
    fn from(value: Value) -> Self {
        match value {
            Value::Integer(v) => Self::Integer(v),
            Value::BigInt(v) => Self::BigInt(v),
            Value::Double(v) => Self::Double(v),
            Value::Boolean(v) => Self::Boolean(v),
            Value::Text(v) => Self::Text(v),
            Value::Timestamp(v) => Self::Timestamp(v),
            Value::Uuid(v) => Self::Uuid(v.uuid().to_string()),
            Value::Bytea(v) => Self::Bytea(v),
            Value::Array(values) => Self::Array(values.into_iter().map(Into::into).collect()),
            Value::Row(values) => Self::Row(values.into_iter().map(Into::into).collect()),
            Value::Null => Self::Null,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CatalogueColumnType {
    Integer,
    BigInt,
    Double,
    Boolean,
    Text,
    Json {
        #[serde(skip_serializing_if = "Option::is_none")]
        schema: Option<serde_json::Value>,
    },
    Enum {
        variants: Vec<String>,
    },
    Timestamp,
    Uuid,
    Bytea,
    Array {
        element: Box<CatalogueColumnType>,
    },
    Row {
        columns: Vec<CatalogueColumnDescriptor>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatalogueColumnDescriptor {
    pub name: String,
    pub column_type: CatalogueColumnType,
    pub nullable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub references: Option<String>,
}

impl From<ColumnType> for CatalogueColumnType {
    fn from(column_type: ColumnType) -> Self {
        match column_type {
            ColumnType::Integer => Self::Integer,
            ColumnType::BigInt => Self::BigInt,
            ColumnType::Boolean => Self::Boolean,
            ColumnType::Double => Self::Double,
            ColumnType::Text => Self::Text,
            ColumnType::Json(schema) => Self::Json { schema },
            ColumnType::Enum(variants) => Self::Enum { variants },
            ColumnType::Timestamp => Self::Timestamp,
            ColumnType::Uuid => Self::Uuid,
            ColumnType::Bytea => Self::Bytea,
            ColumnType::Array(element) => Self::Array {
                element: Box::new((*element).into()),
            },
            ColumnType::Row(descriptor) => Self::Row {
                columns: descriptor
                    .columns
                    .into_iter()
                    .map(|column| CatalogueColumnDescriptor {
                        name: column.name.as_str().to_string(),
                        column_type: column.column_type.into(),
                        nullable: column.nullable,
                        references: column.references.map(|table| table.as_str().to_string()),
                    })
                    .collect(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CataloguePolicyValue {
    Literal { value: CatalogueWasmValue },
    SessionRef { path: Vec<String> },
}

impl From<PolicyValue> for CataloguePolicyValue {
    fn from(value: PolicyValue) -> Self {
        match value {
            PolicyValue::Literal(value) => Self::Literal {
                value: value.into(),
            },
            PolicyValue::SessionRef(path) => Self::SessionRef { path },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CatalogueCmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl From<CmpOp> for CatalogueCmpOp {
    fn from(op: CmpOp) -> Self {
        match op {
            CmpOp::Eq => Self::Eq,
            CmpOp::Ne => Self::Ne,
            CmpOp::Lt => Self::Lt,
            CmpOp::Le => Self::Le,
            CmpOp::Gt => Self::Gt,
            CmpOp::Ge => Self::Ge,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CataloguePolicyOperation {
    Select,
    Insert,
    Update,
    Delete,
}

impl From<Operation> for CataloguePolicyOperation {
    fn from(operation: Operation) -> Self {
        match operation {
            Operation::Select => Self::Select,
            Operation::Insert => Self::Insert,
            Operation::Update => Self::Update,
            Operation::Delete => Self::Delete,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CataloguePolicyExpr {
    Cmp {
        column: String,
        op: CatalogueCmpOp,
        value: CataloguePolicyValue,
    },
    IsNull {
        column: String,
    },
    IsNotNull {
        column: String,
    },
    Contains {
        column: String,
        value: CataloguePolicyValue,
    },
    In {
        column: String,
        session_path: Vec<String>,
    },
    InList {
        column: String,
        values: Vec<CataloguePolicyValue>,
    },
    Exists {
        table: String,
        condition: Box<CataloguePolicyExpr>,
    },
    ExistsRel {
        rel: serde_json::Value,
    },
    Inherits {
        operation: CataloguePolicyOperation,
        via_column: String,
        max_depth: Option<u32>,
    },
    InheritsReferencing {
        operation: CataloguePolicyOperation,
        source_table: String,
        via_column: String,
        max_depth: Option<u32>,
    },
    And {
        exprs: Vec<CataloguePolicyExpr>,
    },
    Or {
        exprs: Vec<CataloguePolicyExpr>,
    },
    Not {
        expr: Box<CataloguePolicyExpr>,
    },
    True,
    False,
}

impl From<PolicyExpr> for CataloguePolicyExpr {
    fn from(expr: PolicyExpr) -> Self {
        match expr {
            PolicyExpr::Cmp { column, op, value } => Self::Cmp {
                column,
                op: op.into(),
                value: value.into(),
            },
            PolicyExpr::IsNull { column } => Self::IsNull { column },
            PolicyExpr::IsNotNull { column } => Self::IsNotNull { column },
            PolicyExpr::Contains { column, value } => Self::Contains {
                column,
                value: value.into(),
            },
            PolicyExpr::In {
                column,
                session_path,
            } => Self::In {
                column,
                session_path,
            },
            PolicyExpr::InList { column, values } => Self::InList {
                column,
                values: values.into_iter().map(Into::into).collect(),
            },
            PolicyExpr::Exists { table, condition } => Self::Exists {
                table,
                condition: Box::new((*condition).into()),
            },
            PolicyExpr::ExistsRel { rel } => Self::ExistsRel {
                rel: serde_json::to_value(rel).unwrap_or(serde_json::Value::Null),
            },
            PolicyExpr::Inherits {
                operation,
                via_column,
                max_depth,
            } => Self::Inherits {
                operation: operation.into(),
                via_column,
                max_depth: max_depth.map(|value| value as u32),
            },
            PolicyExpr::InheritsReferencing {
                operation,
                source_table,
                via_column,
                max_depth,
            } => Self::InheritsReferencing {
                operation: operation.into(),
                source_table,
                via_column,
                max_depth: max_depth.map(|value| value as u32),
            },
            PolicyExpr::And(exprs) => Self::And {
                exprs: exprs.into_iter().map(Into::into).collect(),
            },
            PolicyExpr::Or(exprs) => Self::Or {
                exprs: exprs.into_iter().map(Into::into).collect(),
            },
            PolicyExpr::Not(expr) => Self::Not {
                expr: Box::new((*expr).into()),
            },
            PolicyExpr::True => Self::True,
            PolicyExpr::False => Self::False,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct CatalogueOperationPolicy {
    pub using: Option<CataloguePolicyExpr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub with_check: Option<CataloguePolicyExpr>,
}

impl From<crate::query_manager::types::OperationPolicy> for CatalogueOperationPolicy {
    fn from(policy: crate::query_manager::types::OperationPolicy) -> Self {
        Self {
            using: policy.using.map(Into::into),
            with_check: policy.with_check.map(Into::into),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct CatalogueTablePolicies {
    pub select: Option<CatalogueOperationPolicy>,
    pub insert: Option<CatalogueOperationPolicy>,
    pub update: Option<CatalogueOperationPolicy>,
    pub delete: Option<CatalogueOperationPolicy>,
}

impl From<TablePolicies> for CatalogueTablePolicies {
    fn from(policies: TablePolicies) -> Self {
        Self {
            select: Some(policies.select.into()),
            insert: Some(policies.insert.into()),
            update: Some(policies.update.into()),
            delete: Some(policies.delete.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatalogueTableSchema {
    pub columns: Vec<CatalogueColumnDescriptor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policies: Option<CatalogueTablePolicies>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatalogueSchemaResponse {
    pub tables: HashMap<String, CatalogueTableSchema>,
}

impl From<&Schema> for CatalogueSchemaResponse {
    fn from(schema: &Schema) -> Self {
        let tables = schema
            .iter()
            .map(|(name, table_schema)| {
                let columns = table_schema
                    .descriptor
                    .columns
                    .iter()
                    .map(|column| CatalogueColumnDescriptor {
                        name: column.name.as_str().to_string(),
                        column_type: column.column_type.clone().into(),
                        nullable: column.nullable,
                        references: column.references.map(|table| table.as_str().to_string()),
                    })
                    .collect();
                let policies = if table_schema.policies == TablePolicies::default() {
                    None
                } else {
                    Some(table_schema.policies.clone().into())
                };
                (
                    name.as_str().to_string(),
                    CatalogueTableSchema { columns, policies },
                )
            })
            .collect();

        Self { tables }
    }
}

#[cfg(test)]
mod tests {
    use super::CatalogueSchemaResponse;
    use crate::query_manager::types::{ColumnType, SchemaBuilder, TableSchema};

    #[test]
    fn catalogue_schema_response_serializes_tables_and_columns() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .nullable_column("email", ColumnType::Text),
            )
            .build();

        let response = CatalogueSchemaResponse::from(&schema);
        let json = serde_json::to_value(response).expect("serialize schema response");

        let users = &json["tables"]["users"];
        assert_eq!(users["columns"][0]["name"], "id");
        assert_eq!(users["columns"][0]["column_type"]["type"], "Uuid");
        assert_eq!(users["columns"][1]["name"], "email");
        assert_eq!(users["columns"][1]["column_type"]["type"], "Text");
        assert_eq!(users["columns"][1]["nullable"], true);
    }
}
