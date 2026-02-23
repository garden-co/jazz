//! jazz-napi — Native Node.js bindings for Jazz.
//!
//! Provides `NapiRuntime` wrapping `RuntimeCore<SurrealKvStorage>` via napi-rs.
//! Exposed as the `jazz-napi` npm package for server-side TypeScript apps.
//!
//! # Architecture
//!
//! - `SurrealKvStorage` provides persistent on-disk storage
//! - `NapiScheduler` implements `Scheduler` using `ThreadsafeFunction` to schedule
//!   `batched_tick()` on the Node.js event loop (debounced)
//! - `NapiSyncSender` implements `SyncSender` bridging to a JS callback
//! - `NapiRuntime` wraps `Arc<Mutex<RuntimeCore<...>>>`

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use napi::Env;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;

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
use groove::storage::{Storage, SurrealKvStorage};
use groove::sync_manager::{
    ClientId, InboxEntry, OutboxEntry, PersistenceTier, ServerId, Source, SyncManager, SyncPayload,
};

// ============================================================================
// Value conversion (mirrors jazz-wasm/src/types.rs WasmValue)
// ============================================================================

/// Tagged value type for JS boundary. Serde-serialized as `{ type: "Text", value: "..." }`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", content = "value")]
enum NapiValue {
    Integer(i32),
    BigInt(i64),
    Boolean(bool),
    Text(String),
    Timestamp(u64),
    Uuid(String),
    Array(Vec<NapiValue>),
    Row(Vec<NapiValue>),
    Null,
}

impl From<Value> for NapiValue {
    fn from(v: Value) -> Self {
        match v {
            Value::Integer(i) => NapiValue::Integer(i),
            Value::BigInt(i) => NapiValue::BigInt(i),
            Value::Boolean(b) => NapiValue::Boolean(b),
            Value::Text(s) => NapiValue::Text(s),
            Value::Timestamp(t) => NapiValue::Timestamp(t),
            Value::Uuid(id) => NapiValue::Uuid(id.uuid().to_string()),
            Value::Array(arr) => NapiValue::Array(arr.into_iter().map(Into::into).collect()),
            Value::Row(row) => NapiValue::Row(row.into_iter().map(Into::into).collect()),
            Value::Null => NapiValue::Null,
        }
    }
}

fn napi_value_to_groove(v: NapiValue) -> Result<Value, String> {
    Ok(match v {
        NapiValue::Integer(i) => Value::Integer(i),
        NapiValue::BigInt(i) => Value::BigInt(i),
        NapiValue::Boolean(b) => Value::Boolean(b),
        NapiValue::Text(s) => Value::Text(s),
        NapiValue::Timestamp(t) => Value::Timestamp(t),
        NapiValue::Uuid(s) => {
            let uuid = uuid::Uuid::parse_str(&s).map_err(|e| format!("Invalid UUID: {}", e))?;
            Value::Uuid(ObjectId::from_uuid(uuid))
        }
        NapiValue::Array(arr) => {
            let converted: Result<Vec<_>, _> = arr.into_iter().map(napi_value_to_groove).collect();
            Value::Array(converted?)
        }
        NapiValue::Row(row) => {
            let converted: Result<Vec<_>, _> = row.into_iter().map(napi_value_to_groove).collect();
            Value::Row(converted?)
        }
        NapiValue::Null => Value::Null,
    })
}

fn convert_values(js_values: Vec<NapiValue>) -> napi::Result<Vec<Value>> {
    js_values
        .into_iter()
        .map(|v| napi_value_to_groove(v).map_err(napi::Error::from_reason))
        .collect()
}

fn convert_updates(partial: HashMap<String, NapiValue>) -> napi::Result<Vec<(String, Value)>> {
    partial
        .into_iter()
        .map(|(k, v)| {
            let groove_value = napi_value_to_groove(v).map_err(napi::Error::from_reason)?;
            Ok((k, groove_value))
        })
        .collect()
}

// ============================================================================
// Schema types for JSON deserialization
// ============================================================================

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct JsColumnType {
    #[serde(rename = "type")]
    type_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    element: Option<Box<JsColumnType>>,
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
    Literal { value: NapiValue },
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
        "Boolean" => ColumnType::Boolean,
        "Text" => ColumnType::Text,
        "Timestamp" => ColumnType::Timestamp,
        "Uuid" => ColumnType::Uuid,
        "Array" => {
            let elem = ct.element.expect("Array type requires element");
            ColumnType::Array(Box::new(js_column_type_to_groove(*elem)))
        }
        "Row" => {
            let cols = ct.columns.expect("Row type requires columns");
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
        other => panic!("Unknown column type: {}", other),
    }
}

fn js_schema_to_groove(js: JsSchema) -> Schema {
    use groove::query_manager::policy::{CmpOp, Operation, PolicyExpr, PolicyValue};
    use groove::query_manager::types::{
        ColumnDescriptor, OperationPolicy, RowDescriptor, TableName, TablePolicies, TableSchema,
    };

    fn js_policy_value_to_groove(value: JsPolicyValue) -> PolicyValue {
        match value {
            JsPolicyValue::Literal { value } => {
                PolicyValue::Literal(napi_value_to_groove(value).expect("invalid policy literal"))
            }
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
                columns: None,
            },
            ColumnType::BigInt => JsColumnType {
                type_name: "BigInt".into(),
                element: None,
                columns: None,
            },
            ColumnType::Boolean => JsColumnType {
                type_name: "Boolean".into(),
                element: None,
                columns: None,
            },
            ColumnType::Text => JsColumnType {
                type_name: "Text".into(),
                element: None,
                columns: None,
            },
            ColumnType::Timestamp => JsColumnType {
                type_name: "Timestamp".into(),
                element: None,
                columns: None,
            },
            ColumnType::Uuid => JsColumnType {
                type_name: "Uuid".into(),
                element: None,
                columns: None,
            },
            ColumnType::Array(elem) => JsColumnType {
                type_name: "Array".into(),
                element: Some(Box::new(ct_to_js(elem))),
                columns: None,
            },
            ColumnType::Row(desc) => JsColumnType {
                type_name: "Row".into(),
                element: None,
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
                value: NapiValue::from(value.clone()),
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

fn parse_tier(tier: &str) -> napi::Result<PersistenceTier> {
    match tier {
        "worker" => Ok(PersistenceTier::Worker),
        "edge" => Ok(PersistenceTier::EdgeServer),
        "core" => Ok(PersistenceTier::CoreServer),
        _ => Err(napi::Error::from_reason(format!(
            "Invalid tier '{}'. Must be 'worker', 'edge', or 'core'.",
            tier
        ))),
    }
}

fn parse_query(json: &str) -> napi::Result<Query> {
    parse_query_json(json).map_err(napi::Error::from_reason)
}

// ============================================================================
// NapiScheduler
// ============================================================================

type NapiCoreType = RuntimeCore<SurrealKvStorage, NapiScheduler, NapiSyncSender>;

/// Scheduler that schedules `batched_tick()` on the Node.js event loop via a
/// ThreadsafeFunction wrapping a noop JS function. The TSFN callback closure
/// does the actual work. Debounced: only one tick is pending at a time.
pub struct NapiScheduler {
    scheduled: Arc<AtomicBool>,
    core_ref: Weak<Mutex<NapiCoreType>>,
    tsfn: Option<ThreadsafeFunction<(), ErrorStrategy::CalleeHandled>>,
}

impl NapiScheduler {
    fn new() -> Self {
        Self {
            scheduled: Arc::new(AtomicBool::new(false)),
            core_ref: Weak::new(),
            tsfn: None,
        }
    }

    fn set_core_ref(&mut self, core_ref: Weak<Mutex<NapiCoreType>>) {
        self.core_ref = core_ref;
    }

    fn set_tsfn(&mut self, tsfn: ThreadsafeFunction<(), ErrorStrategy::CalleeHandled>) {
        self.tsfn = Some(tsfn);
    }
}

impl Scheduler for NapiScheduler {
    fn schedule_batched_tick(&self) {
        if !self.scheduled.swap(true, Ordering::SeqCst) {
            if let Some(ref tsfn) = self.tsfn {
                tsfn.call(Ok(()), ThreadsafeFunctionCallMode::NonBlocking);
            } else {
                self.scheduled.store(false, Ordering::SeqCst);
            }
        }
    }
}

// ============================================================================
// NapiSyncSender
// ============================================================================

pub struct NapiSyncSender {
    callback: Arc<Mutex<Option<ThreadsafeFunction<String, ErrorStrategy::CalleeHandled>>>>,
}

impl NapiSyncSender {
    fn new() -> Self {
        Self {
            callback: Arc::new(Mutex::new(None)),
        }
    }

    fn set_callback(&self, tsfn: ThreadsafeFunction<String, ErrorStrategy::CalleeHandled>) {
        if let Ok(mut cb) = self.callback.lock() {
            *cb = Some(tsfn);
        }
    }
}

impl SyncSender for NapiSyncSender {
    fn send_sync_message(&self, message: OutboxEntry) {
        let cb = match self.callback.lock() {
            Ok(cb) => cb,
            Err(_) => return,
        };
        let tsfn = match cb.as_ref() {
            Some(tsfn) => tsfn,
            None => return,
        };
        let json = match serde_json::to_string(&message) {
            Ok(json) => json,
            Err(_) => return,
        };

        tsfn.call(Ok(json), ThreadsafeFunctionCallMode::NonBlocking);
    }
}

// ============================================================================
// NapiRuntime
// ============================================================================

#[napi]
pub struct NapiRuntime {
    core: Arc<Mutex<NapiCoreType>>,
    upstream_server_id: Mutex<Option<ServerId>>,
}

#[napi]
impl NapiRuntime {
    /// Create a new NapiRuntime with SurrealKV-backed persistent storage.
    #[napi(constructor)]
    pub fn new(
        env: Env,
        schema_json: String,
        app_id: String,
        groove_env: String,
        user_branch: String,
        data_path: String,
        tier: Option<String>,
    ) -> napi::Result<Self> {
        // Parse schema
        let js_schema: JsSchema = serde_json::from_str(&schema_json)
            .map_err(|e| napi::Error::from_reason(format!("Invalid schema JSON: {}", e)))?;
        let schema = js_schema_to_groove(js_schema);

        // Parse optional tier
        let persistence_tier = tier.as_deref().map(parse_tier).transpose()?;

        // Create sync manager
        let mut sync_manager = SyncManager::new();
        if let Some(t) = persistence_tier {
            sync_manager = sync_manager.with_tier(t);
        }

        // Create schema manager
        let schema_manager = SchemaManager::new(
            sync_manager,
            schema,
            AppId::from_string(&app_id).unwrap_or_else(|_| AppId::from_name(&app_id)),
            &groove_env,
            &user_branch,
        )
        .map_err(|e| {
            napi::Error::from_reason(format!("Failed to create SchemaManager: {:?}", e))
        })?;

        // Create SurrealKvStorage
        let cache_size = 64 * 1024 * 1024; // 64MB default
        let storage = SurrealKvStorage::open(&data_path, cache_size)
            .map_err(|e| napi::Error::from_reason(format!("Failed to open storage: {:?}", e)))?;

        // Create components
        let scheduler = NapiScheduler::new();
        let sync_sender = NapiSyncSender::new();

        // Create RuntimeCore and wrap
        let core = RuntimeCore::new(schema_manager, storage, scheduler, sync_sender);
        let core_arc = Arc::new(Mutex::new(core));

        // Set up the scheduler's TSFN
        {
            let core_weak = Arc::downgrade(&core_arc);
            let scheduled_flag = {
                let core_guard = core_arc
                    .lock()
                    .map_err(|_| napi::Error::from_reason("lock"))?;
                core_guard.scheduler().scheduled.clone()
            };

            // Create a noop JS function to wrap in a TSFN.
            // The TSFN callback closure does the real work (batched_tick).
            // The noop function receives the return value but ignores it.
            let noop_fn = env.create_function_from_closure("__groove_tick", |_ctx| Ok(()))?;

            let core_ref_for_tsfn = core_weak.clone();
            let flag_for_tsfn = scheduled_flag;

            let mut tsfn = env.create_threadsafe_function(
                &noop_fn,
                0, // max_queue_size: 0 = unlimited
                move |_ctx: napi::threadsafe_function::ThreadSafeCallContext<()>| {
                    // Reset flag first so new ticks can be scheduled
                    flag_for_tsfn.store(false, Ordering::SeqCst);
                    let Some(core_arc) = core_ref_for_tsfn.upgrade() else {
                        // Return empty vec — noop function doesn't use args
                        return Ok(Vec::<napi::JsUnknown>::new());
                    };
                    if let Ok(mut core) = core_arc.lock() {
                        core.batched_tick();
                    }
                    // Return empty vec — noop function doesn't use args
                    Ok(Vec::<napi::JsUnknown>::new())
                },
            )?;

            // Don't keep the Node.js event loop alive for the scheduler
            tsfn.unref(&env)?;

            // Set on scheduler
            let mut core_guard = core_arc
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            core_guard.scheduler_mut().set_core_ref(core_weak);
            core_guard.scheduler_mut().set_tsfn(tsfn);

            // Persist schema to catalogue for server sync
            core_guard.persist_schema();
        }

        Ok(NapiRuntime {
            core: core_arc,
            upstream_server_id: Mutex::new(None),
        })
    }

    // =========================================================================
    // CRUD Operations
    // =========================================================================

    #[napi]
    pub fn insert(
        &self,
        table: String,
        #[napi(ts_arg_type = "any")] values: serde_json::Value,
    ) -> napi::Result<String> {
        let js_values: Vec<NapiValue> = serde_json::from_value(values)
            .map_err(|e| napi::Error::from_reason(format!("Invalid values: {}", e)))?;
        let groove_values = convert_values(js_values)?;

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let result = core
            .insert(&table, groove_values, None)
            .map_err(|e| napi::Error::from_reason(format!("Insert failed: {:?}", e)))?;

        Ok(result.uuid().to_string())
    }

    #[napi]
    pub fn update(
        &self,
        object_id: String,
        #[napi(ts_arg_type = "any")] values: serde_json::Value,
    ) -> napi::Result<()> {
        let uuid = uuid::Uuid::parse_str(&object_id)
            .map_err(|e| napi::Error::from_reason(format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let partial_values: HashMap<String, NapiValue> = serde_json::from_value(values)
            .map_err(|e| napi::Error::from_reason(format!("Invalid values: {}", e)))?;
        let updates = convert_updates(partial_values)?;

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.update(oid, updates, None)
            .map_err(|e| napi::Error::from_reason(format!("Update failed: {:?}", e)))?;

        Ok(())
    }

    #[napi(js_name = "delete")]
    pub fn delete_row(&self, object_id: String) -> napi::Result<()> {
        let uuid = uuid::Uuid::parse_str(&object_id)
            .map_err(|e| napi::Error::from_reason(format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.delete(oid, None)
            .map_err(|e| napi::Error::from_reason(format!("Delete failed: {:?}", e)))?;

        Ok(())
    }

    // =========================================================================
    // Queries
    // =========================================================================

    #[napi(ts_return_type = "Promise<any>")]
    pub fn query(
        &self,
        env: Env,
        query_json: String,
        session_json: Option<String>,
        settled_tier: Option<String>,
    ) -> napi::Result<napi::JsObject> {
        let query = parse_query(&query_json)?;

        let session =
            if let Some(json) = session_json {
                Some(serde_json::from_str::<Session>(&json).map_err(|e| {
                    napi::Error::from_reason(format!("Invalid session JSON: {}", e))
                })?)
            } else {
                None
            };

        let tier = settled_tier.as_deref().map(parse_tier).transpose()?;

        let future = {
            let mut core = self
                .core
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            core.query(query, session, tier)
        };

        // Create a deferred/promise pair
        let (deferred, promise) = env.create_deferred()?;

        // Spawn a thread to block on the oneshot receiver
        std::thread::spawn(move || {
            let result = futures::executor::block_on(future);

            match result {
                Ok(rows) => {
                    let json_rows: Vec<serde_json::Value> = rows
                        .into_iter()
                        .map(|(id, values)| {
                            let js_values: Vec<NapiValue> =
                                values.into_iter().map(Into::into).collect();
                            serde_json::json!({
                                "id": id.uuid().to_string(),
                                "values": js_values
                            })
                        })
                        .collect();

                    deferred.resolve(move |env| env.to_js_value(&json_rows));
                }
                Err(e) => {
                    deferred.reject(napi::Error::from_reason(format!("Query failed: {:?}", e)));
                }
            }
        });

        Ok(promise)
    }

    // =========================================================================
    // Subscriptions
    // =========================================================================

    #[napi]
    pub fn subscribe(
        &self,
        query_json: String,
        #[napi(ts_arg_type = "(...args: any[]) => any")] on_update: napi::JsFunction,
        session_json: Option<String>,
        settled_tier: Option<String>,
    ) -> napi::Result<f64> {
        let query = parse_query(&query_json)?;

        let session =
            if let Some(json) = session_json {
                Some(serde_json::from_str::<Session>(&json).map_err(|e| {
                    napi::Error::from_reason(format!("Invalid session JSON: {}", e))
                })?)
            } else {
                None
            };

        let tier = settled_tier.as_deref().map(parse_tier).transpose()?;

        // Create a ThreadsafeFunction for the JS callback.
        // The closure converts our serde_json::Value into a JsUnknown to pass to JS.
        let tsfn: ThreadsafeFunction<serde_json::Value, ErrorStrategy::CalleeHandled> =
            on_update.create_threadsafe_function(0, |ctx| {
                let val = ctx.env.to_js_value(&ctx.value)?;
                Ok(vec![val])
            })?;

        let callback = move |delta: SubscriptionDelta| {
            let row_to_json = |row: &groove::query_manager::types::Row,
                               descriptor: &groove::query_manager::types::RowDescriptor|
             -> serde_json::Value {
                let values = decode_row(descriptor, &row.data)
                    .map(|vals| vals.into_iter().map(NapiValue::from).collect::<Vec<_>>())
                    .unwrap_or_default();
                serde_json::json!({
                    "id": row.id.uuid().to_string(),
                    "values": values
                })
            };

            let descriptor = &delta.descriptor;

            let delta_obj = serde_json::json!({
                "added": delta.delta.added.iter()
                    .map(|row| row_to_json(row, descriptor))
                    .collect::<Vec<_>>(),
                "removed": delta.delta.removed.iter()
                    .map(|row| row_to_json(row, descriptor))
                    .collect::<Vec<_>>(),
                "updated": delta.delta.updated.iter()
                    .map(|(old, new)| [row_to_json(old, descriptor), row_to_json(new, descriptor)])
                    .collect::<Vec<_>>()
            });

            tsfn.call(Ok(delta_obj), ThreadsafeFunctionCallMode::NonBlocking);
        };

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let handle = core
            .subscribe_with_settled_tier(query, callback, session, tier)
            .map_err(|e| napi::Error::from_reason(format!("Subscribe failed: {:?}", e)))?;

        Ok(handle.0 as f64)
    }

    #[napi]
    pub fn unsubscribe(&self, handle: f64) -> napi::Result<()> {
        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.unsubscribe(SubscriptionHandle(handle as u64));
        Ok(())
    }

    // =========================================================================
    // Persisted CRUD Operations
    // =========================================================================

    #[napi(js_name = "insertPersisted", ts_return_type = "Promise<string>")]
    pub fn insert_persisted(
        &self,
        env: Env,
        table: String,
        #[napi(ts_arg_type = "any")] values: serde_json::Value,
        tier: String,
    ) -> napi::Result<napi::JsObject> {
        let persistence_tier = parse_tier(&tier)?;

        let js_values: Vec<NapiValue> = serde_json::from_value(values)
            .map_err(|e| napi::Error::from_reason(format!("Invalid values: {}", e)))?;
        let groove_values = convert_values(js_values)?;

        let (object_id, receiver) = {
            let mut core = self
                .core
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            core.insert_persisted(&table, groove_values, None, persistence_tier)
                .map_err(|e| napi::Error::from_reason(format!("Insert failed: {:?}", e)))?
        };

        let id_str = object_id.uuid().to_string();
        let (deferred, promise) = env.create_deferred()?;
        std::thread::spawn(move || {
            let _ = futures::executor::block_on(receiver);
            deferred.resolve(move |env| env.create_string(&id_str));
        });

        Ok(promise)
    }

    #[napi(js_name = "updatePersisted", ts_return_type = "Promise<void>")]
    pub fn update_persisted(
        &self,
        env: Env,
        object_id: String,
        #[napi(ts_arg_type = "any")] values: serde_json::Value,
        tier: String,
    ) -> napi::Result<napi::JsObject> {
        let persistence_tier = parse_tier(&tier)?;

        let uuid = uuid::Uuid::parse_str(&object_id)
            .map_err(|e| napi::Error::from_reason(format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let partial_values: HashMap<String, NapiValue> = serde_json::from_value(values)
            .map_err(|e| napi::Error::from_reason(format!("Invalid values: {}", e)))?;
        let updates = convert_updates(partial_values)?;

        let receiver = {
            let mut core = self
                .core
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            core.update_persisted(oid, updates, None, persistence_tier)
                .map_err(|e| napi::Error::from_reason(format!("Update failed: {:?}", e)))?
        };

        let (deferred, promise) = env.create_deferred()?;
        std::thread::spawn(move || {
            let _ = futures::executor::block_on(receiver);
            deferred.resolve(move |env| env.get_undefined());
        });

        Ok(promise)
    }

    #[napi(js_name = "deletePersisted", ts_return_type = "Promise<void>")]
    pub fn delete_persisted(
        &self,
        env: Env,
        object_id: String,
        tier: String,
    ) -> napi::Result<napi::JsObject> {
        let persistence_tier = parse_tier(&tier)?;

        let uuid = uuid::Uuid::parse_str(&object_id)
            .map_err(|e| napi::Error::from_reason(format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let receiver = {
            let mut core = self
                .core
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            core.delete_persisted(oid, None, persistence_tier)
                .map_err(|e| napi::Error::from_reason(format!("Delete failed: {:?}", e)))?
        };

        let (deferred, promise) = env.create_deferred()?;
        std::thread::spawn(move || {
            let _ = futures::executor::block_on(receiver);
            deferred.resolve(move |env| env.get_undefined());
        });

        Ok(promise)
    }

    // =========================================================================
    // Sync Operations
    // =========================================================================

    #[napi(js_name = "onSyncMessageReceived")]
    pub fn on_sync_message_received(&self, message_json: String) -> napi::Result<()> {
        let payload: SyncPayload = serde_json::from_str(&message_json)
            .map_err(|e| napi::Error::from_reason(format!("Invalid sync message: {}", e)))?;

        let entry = InboxEntry {
            source: Source::Server(ServerId::new()),
            payload,
        };

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.park_sync_message(entry);
        Ok(())
    }

    /// Called by JS when a sync message arrives from a client (not a server).
    #[napi(js_name = "onSyncMessageReceivedFromClient")]
    pub fn on_sync_message_received_from_client(
        &self,
        client_id: String,
        message_json: String,
    ) -> napi::Result<()> {
        let uuid = uuid::Uuid::parse_str(&client_id)
            .map_err(|e| napi::Error::from_reason(format!("Invalid client ID: {}", e)))?;
        let cid = ClientId(uuid);

        let payload: SyncPayload = serde_json::from_str(&message_json)
            .map_err(|e| napi::Error::from_reason(format!("Invalid sync message: {}", e)))?;

        let entry = InboxEntry {
            source: Source::Client(cid),
            payload,
        };

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.park_sync_message(entry);
        Ok(())
    }

    #[napi(js_name = "onSyncMessageToSend")]
    pub fn on_sync_message_to_send(
        &self,
        #[napi(ts_arg_type = "(...args: any[]) => any")] callback: napi::JsFunction,
    ) -> napi::Result<()> {
        let tsfn: ThreadsafeFunction<String, ErrorStrategy::CalleeHandled> = callback
            .create_threadsafe_function(0, |ctx| {
                let val = ctx.env.create_string_from_std(ctx.value)?;
                Ok(vec![val])
            })?;

        let core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.sync_sender().set_callback(tsfn);
        Ok(())
    }

    #[napi(js_name = "addServer")]
    pub fn add_server(&self) -> napi::Result<()> {
        let server_id = {
            let mut slot = self
                .upstream_server_id
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            if let Some(server_id) = *slot {
                server_id
            } else {
                let server_id = ServerId::new();
                *slot = Some(server_id);
                server_id
            }
        };
        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        // Re-attach semantics: remove existing upstream edge then add again so
        // replay/full-sync runs on every successful reconnect.
        core.remove_server(server_id);
        core.add_server(server_id);
        Ok(())
    }

    #[napi(js_name = "removeServer")]
    pub fn remove_server(&self) -> napi::Result<()> {
        let Some(server_id) = *self
            .upstream_server_id
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?
        else {
            return Ok(());
        };

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.remove_server(server_id);
        Ok(())
    }

    #[napi(js_name = "addClient")]
    pub fn add_client(&self) -> napi::Result<String> {
        let client_id = ClientId::new();
        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.add_client(client_id, None);
        Ok(client_id.0.to_string())
    }

    /// Set a client's role ("user", "admin", or "peer").
    #[napi(js_name = "setClientRole")]
    pub fn set_client_role(&self, client_id: String, role: String) -> napi::Result<()> {
        use groove::sync_manager::ClientRole;

        let uuid = uuid::Uuid::parse_str(&client_id)
            .map_err(|e| napi::Error::from_reason(format!("Invalid client ID: {}", e)))?;
        let cid = ClientId(uuid);

        let client_role = match role.as_str() {
            "user" => ClientRole::User,
            "admin" => ClientRole::Admin,
            "peer" => ClientRole::Peer,
            _ => {
                return Err(napi::Error::from_reason(format!(
                    "Invalid role '{}'. Must be 'user', 'admin', or 'peer'.",
                    role
                )));
            }
        };

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.set_client_role_by_name(cid, client_role);
        Ok(())
    }

    // =========================================================================
    // Schema Access
    // =========================================================================

    #[napi(js_name = "getSchema", ts_return_type = "any")]
    pub fn get_schema(&self, env: Env) -> napi::Result<napi::JsUnknown> {
        let core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let schema = core.current_schema();
        let js_schema = groove_schema_to_js(schema);
        env.to_js_value(&js_schema)
    }

    #[napi(js_name = "getSchemaHash")]
    pub fn get_schema_hash(&self) -> napi::Result<String> {
        let core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let schema = core.current_schema();
        Ok(SchemaHash::compute(schema).to_string())
    }

    #[napi]
    pub fn flush(&self) -> napi::Result<()> {
        let core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.storage().flush();
        Ok(())
    }

    /// Flush and close the underlying storage, releasing filesystem locks.
    #[napi]
    pub fn close(&self) -> napi::Result<()> {
        let core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.storage().flush();
        core.storage()
            .close()
            .map_err(|e| napi::Error::from_reason(format!("Failed to close storage: {:?}", e)))?;
        Ok(())
    }
}

// ============================================================================
// Module-level utility functions
// ============================================================================

#[napi(js_name = "generateId")]
pub fn generate_id() -> String {
    ObjectId::new().uuid().to_string()
}

#[napi(js_name = "currentTimestamp")]
pub fn current_timestamp() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[napi(js_name = "parseSchema", ts_return_type = "any")]
pub fn parse_schema_fn(env: Env, json: String) -> napi::Result<napi::JsUnknown> {
    let js_schema: JsSchema = serde_json::from_str(&json)
        .map_err(|e| napi::Error::from_reason(format!("Invalid schema JSON: {}", e)))?;
    let _groove_schema = js_schema_to_groove(js_schema.clone());
    env.to_js_value(&js_schema)
}
