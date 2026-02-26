//! jazz-nitro — React Native (Nitro Modules) binding for Jazz runtime.
//!
//! Provides `JazzRuntimeImpl` wrapping `RuntimeCore<SurrealKvStorage>` via Nitro's
//! Rust FFI bridge. The generated `HybridJazzRuntimeSpec` trait defines the FFI
//! surface; this crate provides the implementation.
//!
//! # Architecture
//!
//! - `SurrealKvStorage` provides persistent on-disk storage
//! - `NitroScheduler` implements `Scheduler` via a JS callback
//! - `NitroSyncSender` implements `SyncSender` via a JS callback
//! - `JazzRuntimeImpl` wraps `Mutex<Option<RuntimeCore<...>>>`
//!   (Option for two-step init: factory creates empty, `open()` initializes)

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use futures::executor::block_on;

use groove::object::ObjectId;
use groove::query_manager::encoding::decode_row;
use groove::query_manager::parse_query_json;
use groove::query_manager::query::Query;
use groove::query_manager::session::Session;
use groove::query_manager::types::{Schema, SchemaHash, Value};
use groove::runtime_core::{
    RuntimeCore, Scheduler, SubscriptionDelta, SubscriptionHandle, SyncSender,
};
use groove::schema_manager::{AppId, SchemaManager};
use groove::storage::SurrealKvStorage;
use groove::sync_manager::{
    ClientId, InboxEntry, OutboxEntry, PersistenceTier, ServerId, Source, SyncManager, SyncPayload,
};

// ============================================================================
// JSON boundary types (mirrors jazz-rn + jazz-napi)
// ============================================================================

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", content = "value")]
enum NitroValue {
    Integer(i32),
    BigInt(i64),
    Double(f64),
    Boolean(bool),
    Text(String),
    Timestamp(u64),
    Uuid(String),
    Bytea(Vec<u8>),
    Array(Vec<NitroValue>),
    Row(Vec<NitroValue>),
    Null,
}

impl From<Value> for NitroValue {
    fn from(v: Value) -> Self {
        match v {
            Value::Integer(i) => NitroValue::Integer(i),
            Value::BigInt(i) => NitroValue::BigInt(i),
            Value::Double(f) => NitroValue::Double(f),
            Value::Boolean(b) => NitroValue::Boolean(b),
            Value::Text(s) => NitroValue::Text(s),
            Value::Timestamp(t) => NitroValue::Timestamp(t),
            Value::Uuid(id) => NitroValue::Uuid(id.uuid().to_string()),
            Value::Bytea(bytes) => NitroValue::Bytea(bytes),
            Value::Array(arr) => NitroValue::Array(arr.into_iter().map(Into::into).collect()),
            Value::Row(row) => NitroValue::Row(row.into_iter().map(Into::into).collect()),
            Value::Null => NitroValue::Null,
        }
    }
}

fn nitro_value_to_groove(v: NitroValue) -> Result<Value, String> {
    Ok(match v {
        NitroValue::Integer(i) => Value::Integer(i),
        NitroValue::BigInt(i) => Value::BigInt(i),
        NitroValue::Double(f) => Value::Double(f),
        NitroValue::Boolean(b) => Value::Boolean(b),
        NitroValue::Text(s) => Value::Text(s),
        NitroValue::Timestamp(t) => Value::Timestamp(t),
        NitroValue::Uuid(s) => {
            let uuid = uuid::Uuid::parse_str(&s).map_err(|e| format!("Invalid UUID: {e}"))?;
            Value::Uuid(ObjectId::from_uuid(uuid))
        }
        NitroValue::Bytea(bytes) => Value::Bytea(bytes),
        NitroValue::Array(arr) => Value::Array(
            arr.into_iter()
                .map(nitro_value_to_groove)
                .collect::<Result<_, _>>()?,
        ),
        NitroValue::Row(row) => Value::Row(
            row.into_iter()
                .map(nitro_value_to_groove)
                .collect::<Result<_, _>>()?,
        ),
        NitroValue::Null => Value::Null,
    })
}

fn convert_values(values_json: &str) -> Result<Vec<Value>, String> {
    let js_values: Vec<NitroValue> =
        serde_json::from_str(values_json).map_err(|e| format!("Invalid values JSON: {e}"))?;
    js_values.into_iter().map(nitro_value_to_groove).collect()
}

fn convert_updates(values_json: &str) -> Result<Vec<(String, Value)>, String> {
    let partial: HashMap<String, NitroValue> =
        serde_json::from_str(values_json).map_err(|e| format!("Invalid values JSON: {e}"))?;
    partial
        .into_iter()
        .map(|(k, v)| Ok((k, nitro_value_to_groove(v)?)))
        .collect()
}

// ============================================================================
// Schema types for JSON deserialization (includes policies from jazz-napi)
// ============================================================================

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct JsColumnType {
    #[serde(rename = "type")]
    type_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    element: Option<Box<JsColumnType>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    variants: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    columns: Option<Vec<JsColumnDescriptor>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct JsColumnDescriptor {
    name: String,
    column_type: JsColumnType,
    nullable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    references: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
enum JsPolicyValue {
    Literal { value: NitroValue },
    SessionRef { path: Vec<String> },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum JsCmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum JsPolicyOperation {
    Select,
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
enum JsPolicyExpr {
    Cmp {
        column: String,
        op: JsCmpOp,
        value: JsPolicyValue,
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
        condition: Box<JsPolicyExpr>,
    },
    ExistsRel {
        rel: serde_json::Value,
    },
    Inherits {
        operation: JsPolicyOperation,
        via_column: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_depth: Option<usize>,
    },
    InheritsReferencing {
        operation: JsPolicyOperation,
        source_table: String,
        via_column: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_depth: Option<usize>,
    },
    And {
        exprs: Vec<JsPolicyExpr>,
    },
    Or {
        exprs: Vec<JsPolicyExpr>,
    },
    Not {
        expr: Box<JsPolicyExpr>,
    },
    True,
    False,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
struct JsOperationPolicy {
    #[serde(skip_serializing_if = "Option::is_none")]
    using: Option<JsPolicyExpr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    with_check: Option<JsPolicyExpr>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
struct JsTablePolicies {
    #[serde(skip_serializing_if = "Option::is_none")]
    select: Option<JsOperationPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    insert: Option<JsOperationPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    update: Option<JsOperationPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delete: Option<JsOperationPolicy>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct JsTableSchema {
    columns: Vec<JsColumnDescriptor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    policies: Option<JsTablePolicies>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct JsSchema {
    tables: HashMap<String, JsTableSchema>,
}

fn js_column_type_to_groove(ct: JsColumnType) -> groove::query_manager::types::ColumnType {
    use groove::query_manager::types::{ColumnDescriptor, ColumnType, RowDescriptor};

    match ct.type_name.as_str() {
        "Integer" => ColumnType::Integer,
        "BigInt" => ColumnType::BigInt,
        "Double" => ColumnType::Double,
        "Boolean" => ColumnType::Boolean,
        "Text" => ColumnType::Text,
        "Enum" => {
            let Some(variants) = ct.variants else {
                log::error!("Enum type missing variants, defaulting to Text");
                return ColumnType::Text;
            };
            ColumnType::Enum(variants)
        }
        "Timestamp" => ColumnType::Timestamp,
        "Uuid" => ColumnType::Uuid,
        "Bytea" => ColumnType::Bytea,
        "Array" => {
            let Some(elem) = ct.element else {
                log::error!("Array type missing element, defaulting to Text");
                return ColumnType::Text;
            };
            ColumnType::Array(Box::new(js_column_type_to_groove(*elem)))
        }
        "Row" => {
            let Some(cols) = ct.columns else {
                log::error!("Row type missing columns, defaulting to Text");
                return ColumnType::Text;
            };
            let descriptors = cols
                .into_iter()
                .map(|c| {
                    let mut cd =
                        ColumnDescriptor::new(&c.name, js_column_type_to_groove(c.column_type));
                    if c.nullable {
                        cd = cd.nullable();
                    }
                    if let Some(ref_table) = c.references {
                        cd = cd.references(&ref_table);
                    }
                    cd
                })
                .collect();
            ColumnType::Row(Box::new(RowDescriptor::new(descriptors)))
        }
        other => {
            log::error!("Unknown column type: {other}, defaulting to Text");
            ColumnType::Text
        }
    }
}

fn js_schema_to_groove(js: JsSchema) -> Schema {
    use groove::query_manager::policy::{CmpOp, Operation, PolicyExpr, PolicyValue};
    use groove::query_manager::types::{
        ColumnDescriptor, OperationPolicy, RowDescriptor, TableName, TablePolicies, TableSchema,
    };

    fn js_policy_value_to_groove(value: JsPolicyValue) -> PolicyValue {
        match value {
            JsPolicyValue::Literal { value } => match nitro_value_to_groove(value) {
                Ok(v) => PolicyValue::Literal(v),
                Err(e) => {
                    log::error!("invalid policy literal: {e}, using Null");
                    PolicyValue::Literal(Value::Null)
                }
            },
            JsPolicyValue::SessionRef { path } => PolicyValue::SessionRef(path),
        }
    }

    fn js_policy_expr_to_groove(expr: JsPolicyExpr) -> PolicyExpr {
        match expr {
            JsPolicyExpr::Cmp { column, op, value } => PolicyExpr::Cmp {
                column,
                op: match op {
                    JsCmpOp::Eq => CmpOp::Eq,
                    JsCmpOp::Ne => CmpOp::Ne,
                    JsCmpOp::Lt => CmpOp::Lt,
                    JsCmpOp::Le => CmpOp::Le,
                    JsCmpOp::Gt => CmpOp::Gt,
                    JsCmpOp::Ge => CmpOp::Ge,
                },
                value: js_policy_value_to_groove(value),
            },
            JsPolicyExpr::IsNull { column } => PolicyExpr::IsNull { column },
            JsPolicyExpr::IsNotNull { column } => PolicyExpr::IsNotNull { column },
            JsPolicyExpr::In {
                column,
                session_path,
            } => PolicyExpr::In {
                column,
                session_path,
            },
            JsPolicyExpr::Exists { table, condition } => PolicyExpr::Exists {
                table,
                condition: Box::new(js_policy_expr_to_groove(*condition)),
            },
            JsPolicyExpr::ExistsRel { rel } => match serde_json::from_value(rel) {
                Ok(rel) => PolicyExpr::ExistsRel { rel },
                Err(_) => PolicyExpr::False,
            },
            JsPolicyExpr::Inherits {
                operation,
                via_column,
                max_depth,
            } => PolicyExpr::Inherits {
                operation: match operation {
                    JsPolicyOperation::Select => Operation::Select,
                    JsPolicyOperation::Insert => Operation::Insert,
                    JsPolicyOperation::Update => Operation::Update,
                    JsPolicyOperation::Delete => Operation::Delete,
                },
                via_column,
                max_depth,
            },
            JsPolicyExpr::InheritsReferencing {
                operation,
                source_table,
                via_column,
                max_depth,
            } => PolicyExpr::InheritsReferencing {
                operation: match operation {
                    JsPolicyOperation::Select => Operation::Select,
                    JsPolicyOperation::Insert => Operation::Insert,
                    JsPolicyOperation::Update => Operation::Update,
                    JsPolicyOperation::Delete => Operation::Delete,
                },
                source_table,
                via_column,
                max_depth,
            },
            JsPolicyExpr::And { exprs } => {
                PolicyExpr::And(exprs.into_iter().map(js_policy_expr_to_groove).collect())
            }
            JsPolicyExpr::Or { exprs } => {
                PolicyExpr::Or(exprs.into_iter().map(js_policy_expr_to_groove).collect())
            }
            JsPolicyExpr::Not { expr } => {
                PolicyExpr::Not(Box::new(js_policy_expr_to_groove(*expr)))
            }
            JsPolicyExpr::True => PolicyExpr::True,
            JsPolicyExpr::False => PolicyExpr::False,
        }
    }

    fn js_operation_policy_to_groove(policy: JsOperationPolicy) -> OperationPolicy {
        OperationPolicy {
            using: policy.using.map(js_policy_expr_to_groove),
            with_check: policy.with_check.map(js_policy_expr_to_groove),
        }
    }

    fn js_table_policies_to_groove(policies: Option<JsTablePolicies>) -> TablePolicies {
        let Some(policies) = policies else {
            return TablePolicies::default();
        };
        TablePolicies {
            select: policies
                .select
                .map(js_operation_policy_to_groove)
                .unwrap_or_default(),
            insert: policies
                .insert
                .map(js_operation_policy_to_groove)
                .unwrap_or_default(),
            update: policies
                .update
                .map(js_operation_policy_to_groove)
                .unwrap_or_default(),
            delete: policies
                .delete
                .map(js_operation_policy_to_groove)
                .unwrap_or_default(),
        }
    }

    let mut schema = Schema::new();
    for (table_name, table_schema) in js.tables {
        let columns = table_schema
            .columns
            .into_iter()
            .map(|c| {
                let mut cd =
                    ColumnDescriptor::new(&c.name, js_column_type_to_groove(c.column_type));
                if c.nullable {
                    cd = cd.nullable();
                }
                if let Some(ref_table) = c.references {
                    cd = cd.references(&ref_table);
                }
                cd
            })
            .collect();
        let policies = js_table_policies_to_groove(table_schema.policies);
        schema.insert(
            TableName::new(&table_name),
            TableSchema::with_policies(RowDescriptor::new(columns), policies),
        );
    }
    schema
}

fn groove_schema_to_js(schema: &Schema) -> JsSchema {
    use groove::query_manager::{
        policy::{CmpOp, Operation, PolicyExpr, PolicyValue},
        types::{ColumnType, OperationPolicy, TablePolicies},
    };

    fn ct_to_js(ct: &ColumnType) -> JsColumnType {
        match ct {
            ColumnType::Integer => JsColumnType {
                type_name: "Integer".into(),
                element: None,
                variants: None,
                columns: None,
            },
            ColumnType::BigInt => JsColumnType {
                type_name: "BigInt".into(),
                element: None,
                variants: None,
                columns: None,
            },
            ColumnType::Double => JsColumnType {
                type_name: "Double".into(),
                element: None,
                variants: None,
                columns: None,
            },
            ColumnType::Boolean => JsColumnType {
                type_name: "Boolean".into(),
                element: None,
                variants: None,
                columns: None,
            },
            ColumnType::Text => JsColumnType {
                type_name: "Text".into(),
                element: None,
                variants: None,
                columns: None,
            },
            ColumnType::Enum(variants) => JsColumnType {
                type_name: "Enum".into(),
                element: None,
                variants: Some(variants.clone()),
                columns: None,
            },
            ColumnType::Timestamp => JsColumnType {
                type_name: "Timestamp".into(),
                element: None,
                variants: None,
                columns: None,
            },
            ColumnType::Uuid => JsColumnType {
                type_name: "Uuid".into(),
                element: None,
                variants: None,
                columns: None,
            },
            ColumnType::Bytea => JsColumnType {
                type_name: "Bytea".into(),
                element: None,
                variants: None,
                columns: None,
            },
            ColumnType::Array(elem) => JsColumnType {
                type_name: "Array".into(),
                element: Some(Box::new(ct_to_js(elem))),
                variants: None,
                columns: None,
            },
            ColumnType::Row(desc) => JsColumnType {
                type_name: "Row".into(),
                element: None,
                variants: None,
                columns: Some(
                    desc.columns
                        .iter()
                        .map(|c| JsColumnDescriptor {
                            name: c.name.as_str().to_string(),
                            column_type: ct_to_js(&c.column_type),
                            nullable: c.nullable,
                            references: c.references.map(|r| r.as_str().to_string()),
                        })
                        .collect(),
                ),
            },
        }
    }

    fn policy_value_to_js(value: &PolicyValue) -> JsPolicyValue {
        match value {
            PolicyValue::Literal(value) => JsPolicyValue::Literal {
                value: NitroValue::from(value.clone()),
            },
            PolicyValue::SessionRef(path) => JsPolicyValue::SessionRef { path: path.clone() },
        }
    }

    fn policy_expr_to_js(expr: &PolicyExpr) -> JsPolicyExpr {
        match expr {
            PolicyExpr::Cmp { column, op, value } => JsPolicyExpr::Cmp {
                column: column.clone(),
                op: match op {
                    CmpOp::Eq => JsCmpOp::Eq,
                    CmpOp::Ne => JsCmpOp::Ne,
                    CmpOp::Lt => JsCmpOp::Lt,
                    CmpOp::Le => JsCmpOp::Le,
                    CmpOp::Gt => JsCmpOp::Gt,
                    CmpOp::Ge => JsCmpOp::Ge,
                },
                value: policy_value_to_js(value),
            },
            PolicyExpr::IsNull { column } => JsPolicyExpr::IsNull {
                column: column.clone(),
            },
            PolicyExpr::IsNotNull { column } => JsPolicyExpr::IsNotNull {
                column: column.clone(),
            },
            PolicyExpr::In {
                column,
                session_path,
            } => JsPolicyExpr::In {
                column: column.clone(),
                session_path: session_path.clone(),
            },
            PolicyExpr::Exists { table, condition } => JsPolicyExpr::Exists {
                table: table.clone(),
                condition: Box::new(policy_expr_to_js(condition)),
            },
            PolicyExpr::ExistsRel { rel } => JsPolicyExpr::ExistsRel {
                rel: serde_json::to_value(rel).unwrap_or(serde_json::Value::Null),
            },
            PolicyExpr::Inherits {
                operation,
                via_column,
                max_depth,
            } => JsPolicyExpr::Inherits {
                operation: match operation {
                    Operation::Select => JsPolicyOperation::Select,
                    Operation::Insert => JsPolicyOperation::Insert,
                    Operation::Update => JsPolicyOperation::Update,
                    Operation::Delete => JsPolicyOperation::Delete,
                },
                via_column: via_column.clone(),
                max_depth: *max_depth,
            },
            PolicyExpr::InheritsReferencing {
                operation,
                source_table,
                via_column,
                max_depth,
            } => JsPolicyExpr::InheritsReferencing {
                operation: match operation {
                    Operation::Select => JsPolicyOperation::Select,
                    Operation::Insert => JsPolicyOperation::Insert,
                    Operation::Update => JsPolicyOperation::Update,
                    Operation::Delete => JsPolicyOperation::Delete,
                },
                source_table: source_table.clone(),
                via_column: via_column.clone(),
                max_depth: *max_depth,
            },
            PolicyExpr::And(exprs) => JsPolicyExpr::And {
                exprs: exprs.iter().map(policy_expr_to_js).collect(),
            },
            PolicyExpr::Or(exprs) => JsPolicyExpr::Or {
                exprs: exprs.iter().map(policy_expr_to_js).collect(),
            },
            PolicyExpr::Not(expr) => JsPolicyExpr::Not {
                expr: Box::new(policy_expr_to_js(expr)),
            },
            PolicyExpr::True => JsPolicyExpr::True,
            PolicyExpr::False => JsPolicyExpr::False,
        }
    }

    fn operation_policy_to_js(policy: &OperationPolicy) -> Option<JsOperationPolicy> {
        if policy.using.is_none() && policy.with_check.is_none() {
            return None;
        }
        Some(JsOperationPolicy {
            using: policy.using.as_ref().map(policy_expr_to_js),
            with_check: policy.with_check.as_ref().map(policy_expr_to_js),
        })
    }

    fn table_policies_to_js(policies: &TablePolicies) -> Option<JsTablePolicies> {
        if *policies == TablePolicies::default() {
            return None;
        }
        Some(JsTablePolicies {
            select: operation_policy_to_js(&policies.select),
            insert: operation_policy_to_js(&policies.insert),
            update: operation_policy_to_js(&policies.update),
            delete: operation_policy_to_js(&policies.delete),
        })
    }

    let tables = schema
        .iter()
        .map(|(name, ts)| {
            let columns = ts
                .descriptor
                .columns
                .iter()
                .map(|c| JsColumnDescriptor {
                    name: c.name.as_str().to_string(),
                    column_type: ct_to_js(&c.column_type),
                    nullable: c.nullable,
                    references: c.references.map(|r| r.as_str().to_string()),
                })
                .collect();
            (
                name.as_str().to_string(),
                JsTableSchema {
                    columns,
                    policies: table_policies_to_js(&ts.policies),
                },
            )
        })
        .collect();
    JsSchema { tables }
}

// ============================================================================
// Helper functions
// ============================================================================

fn parse_query(query_json: &str) -> Result<Query, String> {
    parse_query_json(query_json)
}

fn parse_session(session_json: Option<String>) -> Result<Option<Session>, String> {
    match session_json {
        Some(json) => {
            let s: Session =
                serde_json::from_str(&json).map_err(|e| format!("Invalid session JSON: {e}"))?;
            Ok(Some(s))
        }
        None => Ok(None),
    }
}

fn parse_tier(tier: &str) -> Result<PersistenceTier, String> {
    match tier {
        "worker" => Ok(PersistenceTier::Worker),
        "edge" => Ok(PersistenceTier::EdgeServer),
        "core" => Ok(PersistenceTier::CoreServer),
        _ => Err(format!(
            "Invalid tier '{tier}'. Must be 'worker', 'edge', or 'core'."
        )),
    }
}

fn row_to_json(
    descriptor: &groove::query_manager::types::RowDescriptor,
    row: &groove::query_manager::types::Row,
) -> serde_json::Value {
    let values = decode_row(descriptor, &row.data)
        .map(|vals| vals.into_iter().map(NitroValue::from).collect::<Vec<_>>())
        .unwrap_or_default();
    serde_json::json!({
        "id": row.id.uuid().to_string(),
        "values": values,
    })
}

fn fallback_row_json(id: ObjectId) -> serde_json::Value {
    serde_json::json!({
        "id": id.uuid().to_string(),
        "values": [],
    })
}

fn build_delta_json(
    delta: &SubscriptionDelta,
    rows_by_id: &mut HashMap<ObjectId, groove::query_manager::types::Row>,
) -> serde_json::Value {
    let mut removed = Vec::with_capacity(delta.ordered_delta.removed.len());
    for change in &delta.ordered_delta.removed {
        let row_json = rows_by_id
            .remove(&change.id)
            .map(|row| row_to_json(&delta.descriptor, &row))
            .unwrap_or_else(|| fallback_row_json(change.id));
        removed.push(serde_json::json!({ "row": row_json, "index": change.index }));
    }

    let mut updated = Vec::with_capacity(delta.ordered_delta.updated.len());
    for change in &delta.ordered_delta.updated {
        let old_row_json = rows_by_id
            .get(&change.id)
            .map(|row| row_to_json(&delta.descriptor, row))
            .unwrap_or_else(|| {
                change
                    .row
                    .as_ref()
                    .map(|row| row_to_json(&delta.descriptor, row))
                    .unwrap_or_else(|| fallback_row_json(change.id))
            });

        let new_row_json = match &change.row {
            Some(row) => {
                rows_by_id.insert(change.id, row.clone());
                row_to_json(&delta.descriptor, row)
            }
            None => rows_by_id
                .get(&change.id)
                .map(|row| row_to_json(&delta.descriptor, row))
                .unwrap_or_else(|| fallback_row_json(change.id)),
        };

        updated.push(serde_json::json!({
            "old_row": old_row_json,
            "new_row": new_row_json,
            "old_index": change.old_index,
            "new_index": change.new_index,
        }));
    }

    let mut added = Vec::with_capacity(delta.ordered_delta.added.len());
    for change in &delta.ordered_delta.added {
        rows_by_id.insert(change.id, change.row.clone());
        let row_json = row_to_json(&delta.descriptor, &change.row);
        added.push(serde_json::json!({ "row": row_json, "index": change.index }));
    }

    serde_json::json!({
        "added": added,
        "removed": removed,
        "updated": updated,
        "pending": delta.ordered_delta.pending,
    })
}

// ============================================================================
// NitroScheduler

// ============================================================================

type NitroCoreType = RuntimeCore<SurrealKvStorage, NitroScheduler, NitroSyncSender>;
type TickCallback = Arc<Mutex<Option<Box<dyn Fn() + Send + Sync>>>>;
type SyncCallback = Arc<Mutex<Option<Box<dyn Fn(String) + Send + Sync>>>>;

struct NitroScheduler {
    scheduled: Arc<AtomicBool>,
    callback: TickCallback,
}

impl NitroScheduler {
    fn new() -> Self {
        Self {
            scheduled: Arc::new(AtomicBool::new(false)),
            callback: Arc::new(Mutex::new(None)),
        }
    }

    fn set_callback(&self, cb: Option<Box<dyn Fn() + Send + Sync>>) {
        if let Ok(mut slot) = self.callback.lock() {
            *slot = cb;
        }
    }

    fn clear_scheduled(&self) {
        self.scheduled.store(false, Ordering::SeqCst);
    }
}

impl Scheduler for NitroScheduler {
    fn schedule_batched_tick(&self) {
        if !self.scheduled.swap(true, Ordering::SeqCst) {
            let called = if let Ok(guard) = self.callback.lock() {
                if let Some(cb) = guard.as_ref() {
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(cb)).is_ok()
                } else {
                    false
                }
            } else {
                false
            };

            if !called {
                self.scheduled.store(false, Ordering::SeqCst);
            }
        }
    }
}

// ============================================================================
// NitroSyncSender
// ============================================================================

struct NitroSyncSender {
    callback: SyncCallback,
}

impl NitroSyncSender {
    fn new() -> Self {
        Self {
            callback: Arc::new(Mutex::new(None)),
        }
    }

    fn set_callback(&self, cb: Option<Box<dyn Fn(String) + Send + Sync>>) {
        if let Ok(mut slot) = self.callback.lock() {
            *slot = cb;
        }
    }
}

impl SyncSender for NitroSyncSender {
    fn send_sync_message(&self, message: OutboxEntry) {
        let Ok(json) = serde_json::to_string(&message) else {
            return;
        };

        if let Ok(guard) = self.callback.lock()
            && let Some(cb) = guard.as_ref()
        {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cb(json)));
        }
    }
}

// ============================================================================
// JazzRuntimeImpl
// ============================================================================

/// Format an error as a JSON string for methods that return String across FFI.
fn error_json(msg: String) -> String {
    serde_json::json!({ "error": msg }).to_string()
}

pub struct JazzRuntimeImpl {
    core: Mutex<Option<NitroCoreType>>,
    upstream_server_id: Mutex<Option<ServerId>>,
}

impl Default for JazzRuntimeImpl {
    fn default() -> Self {
        Self::new()
    }
}

impl JazzRuntimeImpl {
    pub fn new() -> Self {
        Self {
            core: Mutex::new(None),
            upstream_server_id: Mutex::new(None),
        }
    }

    fn with_core<T, F>(&self, op: &str, f: F) -> Result<T, String>
    where
        F: FnOnce(&mut NitroCoreType) -> T,
    {
        let mut guard = self.core.lock().unwrap_or_else(|e| e.into_inner());
        let core = guard
            .as_mut()
            .ok_or_else(|| format!("{op}: runtime not initialized (call open() first)"))?;
        Ok(f(core))
    }

    // --- Lifecycle ---

    fn open_inner(
        &mut self,
        schema_json: String,
        app_id: String,
        env: String,
        user_branch: String,
        data_path: String,
        tier: Option<String>,
    ) -> Result<(), String> {
        let js_schema: JsSchema =
            serde_json::from_str(&schema_json).map_err(|e| format!("Invalid schema JSON: {e}"))?;
        let schema = js_schema_to_groove(js_schema);

        let persistence_tier = tier.as_deref().map(parse_tier).transpose()?;

        let mut sync_manager = SyncManager::new();
        if let Some(t) = persistence_tier {
            sync_manager = sync_manager.with_tier(t);
        }

        let app_id_obj = AppId::from_string(&app_id).unwrap_or_else(|_| AppId::from_name(&app_id));
        let schema_manager =
            SchemaManager::new(sync_manager, schema, app_id_obj, &env, &user_branch)
                .map_err(|e| format!("Failed to create SchemaManager: {e}"))?;

        let cache_size = 64 * 1024 * 1024; // 64MB
        let storage = SurrealKvStorage::open(&data_path, cache_size)
            .map_err(|e| format!("Failed to open SurrealKV at '{data_path}': {e:?}"))?;

        let scheduler = NitroScheduler::new();
        let sync_sender = NitroSyncSender::new();

        let mut core = RuntimeCore::new(schema_manager, storage, scheduler, sync_sender);
        core.persist_schema();

        let mut guard = self.core.lock().unwrap_or_else(|e| e.into_inner());
        *guard = Some(core);
        Ok(())
    }

    pub fn open(
        &mut self,
        schema_json: String,
        app_id: String,
        env: String,
        user_branch: String,
        data_path: String,
        tier: Option<String>,
    ) {
        if let Err(e) = self.open_inner(schema_json, app_id, env, user_branch, data_path, tier) {
            log::error!("open failed: {e}");
        }
    }

    pub fn flush(&mut self) {
        if let Err(e) = self.with_core("flush", |core| {
            core.flush_storage();
        }) {
            log::error!("flush failed: {e}");
        }
    }

    pub fn close(&mut self) {
        if let Err(e) = self.with_core("close", |core| {
            core.flush_storage();
            let _ = core.storage().close();
        }) {
            log::error!("close failed: {e}");
        }
    }

    // --- CRUD ---

    fn insert_inner(&mut self, table: String, values_json: String) -> Result<String, String> {
        let values = convert_values(&values_json)?;
        self.with_core("insert", |core| {
            core.insert(&table, values, None)
                .map(|id| id.uuid().to_string())
                .map_err(|e| format!("Insert failed: {e}"))
        })?
    }

    pub fn insert(&mut self, table: String, values_json: String) -> String {
        self.insert_inner(table, values_json)
            .unwrap_or_else(error_json)
    }

    fn update_inner(&mut self, object_id: String, values_json: String) -> Result<(), String> {
        let uuid =
            uuid::Uuid::parse_str(&object_id).map_err(|e| format!("Invalid ObjectId: {e}"))?;
        let oid = ObjectId::from_uuid(uuid);
        let updates = convert_updates(&values_json)?;
        self.with_core("update", |core| {
            core.update(oid, updates, None)
                .map_err(|e| format!("Update failed: {e}"))
        })?
    }

    pub fn update(&mut self, object_id: String, values_json: String) {
        if let Err(e) = self.update_inner(object_id, values_json) {
            log::error!("update failed: {e}");
        }
    }

    fn delete_row_inner(&mut self, object_id: String) -> Result<(), String> {
        let uuid =
            uuid::Uuid::parse_str(&object_id).map_err(|e| format!("Invalid ObjectId: {e}"))?;
        let oid = ObjectId::from_uuid(uuid);
        self.with_core("delete", |core| {
            core.delete(oid, None)
                .map_err(|e| format!("Delete failed: {e}"))
        })?
    }

    pub fn delete_row(&mut self, object_id: String) {
        if let Err(e) = self.delete_row_inner(object_id) {
            log::error!("delete_row failed: {e}");
        }
    }

    // --- Queries ---

    fn query_inner(
        &mut self,
        query_json: String,
        session_json: Option<String>,
        settled_tier: Option<String>,
    ) -> Result<String, String> {
        let query = parse_query(&query_json)?;
        let session = parse_session(session_json)?;
        let tier = settled_tier.as_deref().map(parse_tier).transpose()?;

        let fut = self.with_core("query", |core| core.query(query, session, tier))?;

        let results = block_on(fut).map_err(|e| format!("Query failed: {e}"))?;

        let rows_json: Vec<serde_json::Value> = results
            .into_iter()
            .map(|(id, values)| {
                let js_values: Vec<NitroValue> = values.into_iter().map(Into::into).collect();
                serde_json::json!({
                    "id": id.uuid().to_string(),
                    "values": js_values,
                })
            })
            .collect();

        serde_json::to_string(&rows_json).map_err(|e| format!("Failed to serialize results: {e}"))
    }

    pub fn query(
        &mut self,
        query_json: String,
        session_json: Option<String>,
        settled_tier: Option<String>,
    ) -> String {
        self.query_inner(query_json, session_json, settled_tier)
            .unwrap_or_else(error_json)
    }

    // --- Subscriptions ---

    fn subscribe_inner(
        &mut self,
        query_json: String,
        on_update: Box<dyn Fn(String)>,
        session_json: Option<String>,
        settled_tier: Option<String>,
    ) -> Result<f64, String> {
        let query = parse_query(&query_json)?;
        let session = parse_session(session_json)?;
        let tier = settled_tier.as_deref().map(parse_tier).transpose()?;

        // The generated Func_void_std__string wrapper implements Send + Sync,
        // so this transmute is safe — the underlying closure is already
        // thread-safe via FFI function pointer + opaque userdata.
        let on_update: Box<dyn Fn(String) + Send + Sync> = unsafe {
            std::mem::transmute::<Box<dyn Fn(String)>, Box<dyn Fn(String) + Send + Sync>>(on_update)
        };

        let rows_by_id = Arc::new(Mutex::new(HashMap::<
            ObjectId,
            groove::query_manager::types::Row,
        >::new()));

        let handle = self.with_core("subscribe", |core| {
            core.subscribe_with_settled_tier(
                query,
                {
                    let rows_by_id = Arc::clone(&rows_by_id);
                    move |delta: SubscriptionDelta| {
                        let Ok(mut cached_rows) = rows_by_id.lock() else {
                            return;
                        };
                        let payload = build_delta_json(&delta, &mut cached_rows);
                        if let Ok(json) = serde_json::to_string(&payload) {
                            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                on_update(json);
                            }));
                        }
                    }
                },
                session,
                tier,
            )
            .map_err(|e| format!("Subscribe failed: {e}"))
        })??;

        Ok(handle.0 as f64)
    }

    pub fn subscribe(
        &mut self,
        query_json: String,
        on_update: Box<dyn Fn(String)>,
        session_json: Option<String>,
        settled_tier: Option<String>,
    ) -> f64 {
        self.subscribe_inner(query_json, on_update, session_json, settled_tier)
            .unwrap_or_else(|e| {
                log::error!("subscribe failed: {e}");
                -1.0
            })
    }

    pub fn unsubscribe(&mut self, handle: f64) {
        if let Err(e) = self.with_core("unsubscribe", |core| {
            core.unsubscribe(SubscriptionHandle(handle as u64));
        }) {
            log::error!("unsubscribe failed: {e}");
        }
    }

    // --- Persisted CRUD ---

    fn insert_persisted_inner(
        &mut self,
        table: String,
        values_json: String,
        tier: String,
    ) -> Result<String, String> {
        let persistence_tier = parse_tier(&tier)?;
        let values = convert_values(&values_json)?;

        let (object_id, receiver) = self.with_core("insert_persisted", |core| {
            core.insert_persisted(&table, values, None, persistence_tier)
                .map_err(|e| format!("Insert failed: {e}"))
        })??;

        let _ = block_on(receiver);
        Ok(object_id.uuid().to_string())
    }

    pub fn insert_persisted(&mut self, table: String, values_json: String, tier: String) -> String {
        self.insert_persisted_inner(table, values_json, tier)
            .unwrap_or_else(error_json)
    }

    fn update_persisted_inner(
        &mut self,
        object_id: String,
        values_json: String,
        tier: String,
    ) -> Result<(), String> {
        let persistence_tier = parse_tier(&tier)?;
        let uuid =
            uuid::Uuid::parse_str(&object_id).map_err(|e| format!("Invalid ObjectId: {e}"))?;
        let oid = ObjectId::from_uuid(uuid);
        let updates = convert_updates(&values_json)?;

        let receiver = self.with_core("update_persisted", |core| {
            core.update_persisted(oid, updates, None, persistence_tier)
                .map_err(|e| format!("Update failed: {e}"))
        })??;

        let _ = block_on(receiver);
        Ok(())
    }

    pub fn update_persisted(&mut self, object_id: String, values_json: String, tier: String) {
        if let Err(e) = self.update_persisted_inner(object_id, values_json, tier) {
            log::error!("update_persisted failed: {e}");
        }
    }

    fn delete_persisted_inner(&mut self, object_id: String, tier: String) -> Result<(), String> {
        let persistence_tier = parse_tier(&tier)?;
        let uuid =
            uuid::Uuid::parse_str(&object_id).map_err(|e| format!("Invalid ObjectId: {e}"))?;
        let oid = ObjectId::from_uuid(uuid);

        let receiver = self.with_core("delete_persisted", |core| {
            core.delete_persisted(oid, None, persistence_tier)
                .map_err(|e| format!("Delete failed: {e}"))
        })??;

        let _ = block_on(receiver);
        Ok(())
    }

    pub fn delete_persisted(&mut self, object_id: String, tier: String) {
        if let Err(e) = self.delete_persisted_inner(object_id, tier) {
            log::error!("delete_persisted failed: {e}");
        }
    }

    // --- Sync ---

    fn on_sync_message_received_inner(&mut self, message_json: String) -> Result<(), String> {
        let payload: SyncPayload = serde_json::from_str(&message_json)
            .map_err(|e| format!("Invalid sync message JSON: {e}"))?;
        let entry = InboxEntry {
            source: Source::Server(ServerId::new()),
            payload,
        };
        self.with_core("on_sync_message_received", |core| {
            core.park_sync_message(entry);
        })
    }

    pub fn on_sync_message_received(&mut self, message_json: String) {
        if let Err(e) = self.on_sync_message_received_inner(message_json) {
            log::error!("on_sync_message_received failed: {e}");
        }
    }

    pub fn on_sync_message_to_send(&mut self, callback: Box<dyn Fn(String)>) {
        // The generated Func_void_std__string wrapper implements Send + Sync,
        // so this transmute is safe — the underlying closure is already
        // thread-safe via FFI function pointer + opaque userdata.
        let cb: Box<dyn Fn(String) + Send + Sync> = unsafe {
            std::mem::transmute::<Box<dyn Fn(String)>, Box<dyn Fn(String) + Send + Sync>>(callback)
        };
        if let Err(e) = self.with_core("on_sync_message_to_send", |core| {
            core.sync_sender().set_callback(Some(cb));
        }) {
            log::error!("on_sync_message_to_send failed: {e}");
        }
    }

    fn on_sync_message_received_from_client_inner(
        &mut self,
        client_id: String,
        message_json: String,
    ) -> Result<(), String> {
        let uuid =
            uuid::Uuid::parse_str(&client_id).map_err(|e| format!("Invalid client ID: {e}"))?;
        let cid = ClientId(uuid);
        let payload: SyncPayload = serde_json::from_str(&message_json)
            .map_err(|e| format!("Invalid sync message JSON: {e}"))?;
        let entry = InboxEntry {
            source: Source::Client(cid),
            payload,
        };
        self.with_core("on_sync_message_received_from_client", |core| {
            core.park_sync_message(entry);
        })
    }

    pub fn on_sync_message_received_from_client(
        &mut self,
        client_id: String,
        message_json: String,
    ) {
        if let Err(e) = self.on_sync_message_received_from_client_inner(client_id, message_json) {
            log::error!("on_sync_message_received_from_client failed: {e}");
        }
    }

    // --- Server/Client management ---

    pub fn add_server(&mut self) {
        let server_id = {
            let mut slot = self
                .upstream_server_id
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(server_id) = *slot {
                server_id
            } else {
                let server_id = ServerId::new();
                *slot = Some(server_id);
                server_id
            }
        };
        if let Err(e) = self.with_core("add_server", |core| {
            core.remove_server(server_id);
            core.add_server(server_id);
        }) {
            log::error!("add_server failed: {e}");
        }
    }

    pub fn remove_server(&mut self) {
        let server_id = {
            let slot = self
                .upstream_server_id
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *slot
        };
        let Some(server_id) = server_id else {
            return;
        };
        if let Err(e) = self.with_core("remove_server", |core| {
            core.remove_server(server_id);
        }) {
            log::error!("remove_server failed: {e}");
        }
    }

    pub fn add_client(&mut self) -> String {
        let client_id = ClientId::new();
        if let Err(e) = self.with_core("add_client", |core| {
            core.add_client(client_id, None);
        }) {
            return error_json(e);
        }
        client_id.0.to_string()
    }

    fn set_client_role_inner(&mut self, client_id: String, role: String) -> Result<(), String> {
        use groove::sync_manager::ClientRole;

        let uuid =
            uuid::Uuid::parse_str(&client_id).map_err(|e| format!("Invalid client ID: {e}"))?;
        let cid = ClientId(uuid);

        let client_role = match role.as_str() {
            "user" => ClientRole::User,
            "admin" => ClientRole::Admin,
            "peer" => ClientRole::Peer,
            _ => {
                return Err(format!(
                    "Invalid role '{role}'. Must be 'user', 'admin', or 'peer'."
                ));
            }
        };

        self.with_core("set_client_role", |core| {
            core.set_client_role_by_name(cid, client_role);
        })
    }

    pub fn set_client_role(&mut self, client_id: String, role: String) {
        if let Err(e) = self.set_client_role_inner(client_id, role) {
            log::error!("set_client_role failed: {e}");
        }
    }

    // --- Scheduling ---

    pub fn on_batched_tick_needed(&mut self, callback: Box<dyn Fn()>) {
        // The generated Func_void wrapper implements Send + Sync,
        // so this transmute is safe.
        let cb: Box<dyn Fn() + Send + Sync> =
            unsafe { std::mem::transmute::<Box<dyn Fn()>, Box<dyn Fn() + Send + Sync>>(callback) };
        if let Err(e) = self.with_core("on_batched_tick_needed", |core| {
            core.scheduler_mut().set_callback(Some(cb));
        }) {
            log::error!("on_batched_tick_needed failed: {e}");
        }
    }

    pub fn batched_tick(&mut self) {
        if let Err(e) = self.with_core("batched_tick", |core| {
            core.scheduler_mut().clear_scheduled();
            core.batched_tick();
        }) {
            log::error!("batched_tick failed: {e}");
        }
    }

    // --- Schema ---

    fn get_schema_json_inner(&mut self) -> Result<String, String> {
        self.with_core("get_schema_json", |core| {
            let schema = core.current_schema();
            let js_schema = groove_schema_to_js(schema);
            serde_json::to_string(&js_schema)
                .map_err(|e| format!("Failed to serialize schema: {e}"))
        })?
    }

    pub fn get_schema_json(&mut self) -> String {
        self.get_schema_json_inner().unwrap_or_else(error_json)
    }

    pub fn get_schema_hash(&mut self) -> String {
        self.with_core("get_schema_hash", |core| {
            let schema = core.current_schema();
            SchemaHash::compute(schema).to_string()
        })
        .unwrap_or_else(error_json)
    }

    // --- Utilities ---

    pub fn generate_id(&mut self) -> String {
        ObjectId::new().uuid().to_string()
    }

    pub fn current_timestamp_ms(&mut self) -> f64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as f64
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema_json() -> String {
        serde_json::json!({
            "tables": {
                "todos": {
                    "columns": [
                        { "name": "title", "column_type": { "type": "Text" }, "nullable": false },
                        { "name": "done", "column_type": { "type": "Boolean" }, "nullable": false }
                    ]
                }
            }
        })
        .to_string()
    }

    fn create_runtime(dir: &std::path::Path) -> JazzRuntimeImpl {
        let mut rt = JazzRuntimeImpl::new();
        rt.open(
            test_schema_json(),
            "test-app".to_string(),
            "dev".to_string(),
            "main".to_string(),
            dir.to_string_lossy().into_owned(),
            None,
        );
        rt
    }

    #[test]
    fn insert_and_query() {
        let dir = tempfile::tempdir().unwrap();
        let mut rt = create_runtime(dir.path());

        let values_json = serde_json::json!([
            { "type": "Text", "value": "Buy milk" },
            { "type": "Boolean", "value": false }
        ])
        .to_string();

        let id = rt.insert("todos".into(), values_json);
        assert!(!id.is_empty());

        let query_json = serde_json::json!({ "table": "todos", "relation_ir": { "TableScan": { "table": "todos" } } }).to_string();
        let result = rt.query(query_json, None, None);
        let rows: Vec<serde_json::Value> = serde_json::from_str(&result).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"].as_str().unwrap(), id);

        rt.close();
    }

    #[test]
    fn subscribe_and_tick() {
        let dir = tempfile::tempdir().unwrap();
        let mut rt = create_runtime(dir.path());

        let deltas = Arc::new(Mutex::new(Vec::<String>::new()));

        let query_json = serde_json::json!({ "table": "todos", "relation_ir": { "TableScan": { "table": "todos" } } }).to_string();

        let deltas_clone = Arc::clone(&deltas);
        let callback: Box<dyn Fn(String)> = Box::new(move |delta_json: String| {
            deltas_clone.lock().unwrap().push(delta_json);
        });

        let handle = rt.subscribe(query_json, callback, None, None);
        assert!(handle >= 0.0);

        let values_json = serde_json::json!([
            { "type": "Text", "value": "Test todo" },
            { "type": "Boolean", "value": false }
        ])
        .to_string();
        rt.insert("todos".into(), values_json);
        rt.batched_tick();

        let captured = deltas.lock().unwrap();
        assert!(
            !captured.is_empty(),
            "Expected subscription callback to fire after insert + tick"
        );

        rt.unsubscribe(handle);
        rt.close();
    }

    #[test]
    fn flush_and_close() {
        let dir = tempfile::tempdir().unwrap();
        let mut rt = create_runtime(dir.path());

        let values_json = serde_json::json!([
            { "type": "Text", "value": "Persistent" },
            { "type": "Boolean", "value": true }
        ])
        .to_string();
        rt.insert("todos".into(), values_json);
        rt.flush();
        rt.close();
    }

    #[test]
    fn schema_access() {
        let dir = tempfile::tempdir().unwrap();
        let mut rt = create_runtime(dir.path());

        let schema_json = rt.get_schema_json();
        let schema: serde_json::Value = serde_json::from_str(&schema_json).unwrap();
        assert!(schema["tables"]["todos"].is_object());

        let hash = rt.get_schema_hash();
        assert_eq!(hash.len(), 64);

        rt.close();
    }

    #[test]
    fn generate_id_and_timestamp() {
        let mut rt = JazzRuntimeImpl::new();
        let id = rt.generate_id();
        assert!(!id.is_empty());
        uuid::Uuid::parse_str(&id).expect("Should be valid UUID");

        let ts = rt.current_timestamp_ms();
        assert!(ts > 0.0);
    }

    // --- Error path tests ---

    fn assert_error_json(result: &str, expected_substring: &str) {
        let v: serde_json::Value = serde_json::from_str(result)
            .unwrap_or_else(|_| panic!("Expected JSON error, got: {result}"));
        let err = v["error"]
            .as_str()
            .unwrap_or_else(|| panic!("Expected 'error' field in: {result}"));
        assert!(
            err.contains(expected_substring),
            "Error '{err}' should contain '{expected_substring}'"
        );
    }

    #[test]
    fn insert_before_open_returns_error() {
        let mut rt = JazzRuntimeImpl::new();
        let result = rt.insert(
            "todos".into(),
            serde_json::json!([{"type":"Text","value":"x"},{"type":"Boolean","value":false}])
                .to_string(),
        );
        assert_error_json(&result, "not initialized");
    }

    #[test]
    fn query_before_open_returns_error() {
        let mut rt = JazzRuntimeImpl::new();
        let result = rt.query(
            serde_json::json!({"table":"todos","relation_ir":{"TableScan":{"table":"todos"}}})
                .to_string(),
            None,
            None,
        );
        assert_error_json(&result, "not initialized");
    }

    #[test]
    fn insert_with_invalid_json_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut rt = create_runtime(dir.path());
        let result = rt.insert("todos".into(), "not valid json".into());
        assert_error_json(&result, "Invalid values JSON");
        rt.close();
    }

    #[test]
    fn query_with_invalid_json_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut rt = create_runtime(dir.path());
        let result = rt.query("{{bad json".into(), None, None);
        assert_error_json(&result, "");
        rt.close();
    }

    #[test]
    fn update_with_invalid_uuid_does_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let mut rt = create_runtime(dir.path());
        // Should log error, not panic
        rt.update("not-a-uuid".into(), "{}".into());
        rt.close();
    }

    #[test]
    fn delete_with_invalid_uuid_does_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let mut rt = create_runtime(dir.path());
        // Should log error, not panic
        rt.delete_row("not-a-uuid".into());
        rt.close();
    }

    #[test]
    fn subscribe_with_invalid_tier_returns_sentinel() {
        let dir = tempfile::tempdir().unwrap();
        let mut rt = create_runtime(dir.path());
        let cb: Box<dyn Fn(String)> = Box::new(|_| {});
        let handle = rt.subscribe(
            serde_json::json!({"table":"todos","relation_ir":{"TableScan":{"table":"todos"}}})
                .to_string(),
            cb,
            None,
            Some("invalid_tier".into()),
        );
        assert_eq!(handle, -1.0, "Invalid tier should return -1.0 sentinel");
        rt.close();
    }

    #[test]
    fn set_client_role_invalid_role_does_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let mut rt = create_runtime(dir.path());
        let client_id = rt.add_client();
        // Should log error, not panic
        rt.set_client_role(client_id, "superadmin".into());
        rt.close();
    }

    #[test]
    fn set_client_role_invalid_uuid_does_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let mut rt = create_runtime(dir.path());
        // Should log error, not panic
        rt.set_client_role("not-a-uuid".into(), "user".into());
        rt.close();
    }

    #[test]
    fn open_with_invalid_schema_does_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let mut rt = JazzRuntimeImpl::new();
        // Should log error, not panic
        rt.open(
            "not valid json".into(),
            "test-app".into(),
            "dev".into(),
            "main".into(),
            dir.path().to_string_lossy().into_owned(),
            None,
        );
        // Runtime should still be uninitialized after failed open
        let result = rt.insert("todos".into(), "[]".into());
        assert_error_json(&result, "not initialized");
    }

    #[test]
    fn open_with_invalid_tier_does_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let mut rt = JazzRuntimeImpl::new();
        // Should log error, not panic
        rt.open(
            test_schema_json(),
            "test-app".into(),
            "dev".into(),
            "main".into(),
            dir.path().to_string_lossy().into_owned(),
            Some("invalid_tier".into()),
        );
    }

    // --- CRUD lifecycle test ---

    #[test]
    fn crud_lifecycle() {
        //  alice inserts → updates → queries → deletes → queries empty
        let dir = tempfile::tempdir().unwrap();
        let mut alice = create_runtime(dir.path());

        // Insert
        let values = serde_json::json!([
            { "type": "Text", "value": "Walk the dog" },
            { "type": "Boolean", "value": false }
        ])
        .to_string();
        let id = alice.insert("todos".into(), values);
        assert!(
            uuid::Uuid::parse_str(&id).is_ok(),
            "Insert should return valid UUID"
        );

        // Update: mark done
        let updates =
            serde_json::json!({ "done": { "type": "Boolean", "value": true } }).to_string();
        alice.update(id.clone(), updates);

        // Query: should show updated value
        let query = serde_json::json!({ "table": "todos", "relation_ir": { "TableScan": { "table": "todos" } } }).to_string();
        let result = alice.query(query.clone(), None, None);
        let rows: Vec<serde_json::Value> = serde_json::from_str(&result).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"].as_str().unwrap(), id);

        // Delete
        alice.delete_row(id.clone());

        // Query: should be empty
        let result = alice.query(query, None, None);
        let rows: Vec<serde_json::Value> = serde_json::from_str(&result).unwrap();
        assert_eq!(rows.len(), 0);

        alice.close();
    }

    // --- Persisted CRUD error paths ---

    #[test]
    fn insert_persisted_invalid_tier_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut rt = create_runtime(dir.path());
        let values = serde_json::json!([
            { "type": "Text", "value": "test" },
            { "type": "Boolean", "value": false }
        ])
        .to_string();
        let result = rt.insert_persisted("todos".into(), values, "invalid_tier".into());
        assert_error_json(&result, "Invalid tier");
        rt.close();
    }

    #[test]
    fn update_persisted_invalid_uuid_does_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let mut rt = create_runtime(dir.path());
        rt.update_persisted("not-a-uuid".into(), "{}".into(), "worker".into());
        rt.close();
    }

    #[test]
    fn delete_persisted_invalid_tier_does_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let mut rt = create_runtime(dir.path());
        let id = rt.generate_id();
        rt.delete_persisted(id, "bad_tier".into());
        rt.close();
    }

    // --- Sync error paths ---

    #[test]
    fn on_sync_message_received_invalid_json_does_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let mut rt = create_runtime(dir.path());
        rt.on_sync_message_received("not json".into());
        rt.close();
    }

    #[test]
    fn on_sync_message_received_from_client_invalid_id_does_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let mut rt = create_runtime(dir.path());
        rt.on_sync_message_received_from_client("not-a-uuid".into(), "{}".into());
        rt.close();
    }
}
