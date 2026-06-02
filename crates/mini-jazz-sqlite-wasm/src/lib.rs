use std::collections::{BTreeMap, BTreeSet};

use mini_jazz_sqlite::{
    BuiltQuery, RowsSubscription, Runtime, SchemaDef, SqliteQueryPlan, SqliteQueryPlanRow, Storage,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

const SLOW_QUERY_LOG_THRESHOLD_MS: f64 = 10.0;
const SQLITE_TIMING_LOGS_FLAG: &str = "sqliteTimingLogs";

#[wasm_bindgen(start)]
pub fn install_panic_hook() {
    console_error_panic_hook::set_once();
}

#[wasm_bindgen]
pub struct MiniJazzRuntime {
    runtime: Runtime,
    flags: MiniJazzFlags,
    subscriptions: BTreeMap<u32, WasmRowsSubscription>,
    next_subscription_id: u32,
    notifying_subscriptions: bool,
    notify_subscriptions_again: bool,
}

#[derive(Default)]
struct MiniJazzFlags {
    sqlite_timing_logs: bool,
}

struct WasmRowsSubscription {
    subscription: RowsSubscription,
    callback: js_sys::Function,
}

struct PendingNotification {
    id: u32,
    previous_subscription: RowsSubscription,
    next_subscription: RowsSubscription,
    callback: js_sys::Function,
    value: JsValue,
}

struct NotificationError {
    error: JsValue,
    failed_ids: Vec<u32>,
}

#[derive(Deserialize)]
struct WasmRowMutation {
    id: String,
    values: BTreeMap<String, JsonValue>,
}

impl From<JsValue> for NotificationError {
    fn from(error: JsValue) -> Self {
        Self {
            error,
            failed_ids: Vec::new(),
        }
    }
}

#[wasm_bindgen]
impl MiniJazzRuntime {
    #[wasm_bindgen(js_name = openMemory)]
    pub fn open_memory(node_id: &str, user: &str) -> Result<MiniJazzRuntime, JsValue> {
        Runtime::open(Storage::Memory, node_id, user)
            .map(MiniJazzRuntime::new)
            .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = openTodoMemory)]
    pub fn open_todo_memory(node_id: &str, user: &str) -> Result<MiniJazzRuntime, JsValue> {
        Runtime::open_with_schema(
            Storage::Memory,
            node_id,
            user,
            SchemaDef::mini_sqlite_todo_fixture(),
        )
        .map(MiniJazzRuntime::new)
        .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = openTrustedTodoMemory)]
    pub fn open_trusted_todo_memory(node_id: &str) -> Result<MiniJazzRuntime, JsValue> {
        Runtime::open_trusted_with_schema(
            Storage::Memory,
            node_id,
            SchemaDef::mini_sqlite_todo_fixture(),
        )
        .map(MiniJazzRuntime::new)
        .map_err(to_js_error)
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = openOpfs)]
    pub async fn open_opfs(
        db_name: &str,
        node_id: &str,
        user: &str,
    ) -> Result<MiniJazzRuntime, JsValue> {
        install_opfs_vfs(db_name).await?;

        Runtime::open(Storage::File(db_name.into()), node_id, user)
            .map(MiniJazzRuntime::new)
            .map_err(to_js_error)
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = openTodoOpfs)]
    pub async fn open_todo_opfs(
        db_name: &str,
        node_id: &str,
        user: &str,
    ) -> Result<MiniJazzRuntime, JsValue> {
        install_opfs_vfs(db_name).await?;

        Runtime::open_with_schema(
            Storage::File(db_name.into()),
            node_id,
            user,
            SchemaDef::mini_sqlite_todo_fixture(),
        )
        .map(MiniJazzRuntime::new)
        .map_err(to_js_error)
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = openTrustedTodoOpfs)]
    pub async fn open_trusted_todo_opfs(
        db_name: &str,
        node_id: &str,
    ) -> Result<MiniJazzRuntime, JsValue> {
        install_opfs_vfs(db_name).await?;

        Runtime::open_trusted_with_schema(
            Storage::File(db_name.into()),
            node_id,
            SchemaDef::mini_sqlite_todo_fixture(),
        )
        .map(MiniJazzRuntime::new)
        .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = insertRow)]
    pub fn insert_row(
        &mut self,
        table_name: &str,
        id: &str,
        values: JsValue,
    ) -> Result<String, JsValue> {
        let tx_id = self
            .runtime
            .insert_row(table_name, id, parse_values(values)?)
            .map_err(to_js_error)?;
        self.notify_subscriptions()?;
        Ok(tx_id)
    }

    #[wasm_bindgen(js_name = upsertRowsAsUser)]
    pub fn upsert_rows_as_user(
        &mut self,
        user: &str,
        table_name: &str,
        rows: JsValue,
    ) -> Result<String, JsValue> {
        let rows = parse_row_mutations(rows)?;
        let tx_id = self
            .runtime
            .run_attributing_to_user(user, |runtime| {
                let mut tx = runtime.transaction();
                for row in rows {
                    tx = tx.upsert_row(table_name, &row.id, row.values);
                }
                tx.commit()
            })
            .map_err(to_js_error)?;
        self.notify_subscriptions()?;
        Ok(tx_id)
    }

    #[wasm_bindgen(js_name = updateRow)]
    pub fn update_row(
        &mut self,
        table_name: &str,
        id: &str,
        values: JsValue,
    ) -> Result<String, JsValue> {
        let tx_id = self
            .runtime
            .update_row(table_name, id, parse_values(values)?)
            .map_err(to_js_error)?;
        self.notify_subscriptions()?;
        Ok(tx_id)
    }

    #[wasm_bindgen(js_name = deleteRow)]
    pub fn delete_row(&mut self, table_name: &str, id: &str) -> Result<String, JsValue> {
        let tx_id = self
            .runtime
            .delete_row(table_name, id)
            .map_err(to_js_error)?;
        self.notify_subscriptions()?;
        Ok(tx_id)
    }

    #[wasm_bindgen(js_name = setMiniJazzFlag)]
    pub fn set_mini_jazz_flag(&mut self, flag: &str, enabled: bool) -> Result<(), JsValue> {
        match flag {
            SQLITE_TIMING_LOGS_FLAG => {
                self.flags.sqlite_timing_logs = enabled;
                Ok(())
            }
            _ => Err(JsValue::from_str(&format!(
                "unknown mini-jazz flag: {flag}"
            ))),
        }
    }

    #[wasm_bindgen(js_name = getMiniJazzFlag)]
    pub fn get_mini_jazz_flag(&self, flag: &str) -> Result<bool, JsValue> {
        match flag {
            SQLITE_TIMING_LOGS_FLAG => Ok(self.flags.sqlite_timing_logs),
            _ => Err(JsValue::from_str(&format!(
                "unknown mini-jazz flag: {flag}"
            ))),
        }
    }

    #[wasm_bindgen(js_name = query)]
    pub fn query(&self, query: JsValue) -> Result<JsValue, JsValue> {
        let query = parse_built_query(query)?;
        let debug = self.debug_query_sql_for_log(&query);
        let table = query.table.clone();
        let started_at = js_sys::Date::now();
        match self.runtime.query(query.clone()) {
            Ok(rows) => {
                let duration_ms = js_sys::Date::now() - started_at;
                let row_count = rows.len();
                let value = to_js_value(rows)?;
                if self.should_log_sqlite_timing(duration_ms, false) {
                    let plan = explain_query_for_log(&self.runtime, &query);
                    log_sqlite_query(
                        "query",
                        table,
                        debug,
                        plan,
                        duration_ms,
                        Some(row_count),
                        None,
                    );
                }
                Ok(value)
            }
            Err(error) => {
                let duration_ms = js_sys::Date::now() - started_at;
                let message = error.to_string();
                if self.should_log_sqlite_timing(duration_ms, true) {
                    let plan = explain_query_for_log(&self.runtime, &query);
                    log_sqlite_query(
                        "query",
                        table,
                        debug,
                        plan,
                        duration_ms,
                        None,
                        Some(message),
                    );
                }
                Err(to_js_error(error))
            }
        }
    }

    #[wasm_bindgen(js_name = one)]
    pub fn one(&self, query: JsValue) -> Result<JsValue, JsValue> {
        let query = parse_built_query(query)?;
        let debug = self.debug_query_sql_for_log(&query);
        let table = query.table.clone();
        let started_at = js_sys::Date::now();
        match self.runtime.one(query.clone()) {
            Ok(row) => {
                let duration_ms = js_sys::Date::now() - started_at;
                let row_count = usize::from(row.is_some());
                let value = to_js_value(row)?;
                if self.should_log_sqlite_timing(duration_ms, false) {
                    let plan = explain_query_for_log(&self.runtime, &query);
                    log_sqlite_query(
                        "one",
                        table,
                        debug,
                        plan,
                        duration_ms,
                        Some(row_count),
                        None,
                    );
                }
                Ok(value)
            }
            Err(error) => {
                let duration_ms = js_sys::Date::now() - started_at;
                let message = error.to_string();
                if self.should_log_sqlite_timing(duration_ms, true) {
                    let plan = explain_query_for_log(&self.runtime, &query);
                    log_sqlite_query("one", table, debug, plan, duration_ms, None, Some(message));
                }
                Err(to_js_error(error))
            }
        }
    }

    #[wasm_bindgen(js_name = explainQuery)]
    pub fn explain_query(&self, query: JsValue) -> Result<JsValue, JsValue> {
        let query = parse_built_query(query)?;
        let table = query.table.clone();
        let started_at = js_sys::Date::now();
        match self.runtime.explain_query_plan(&query) {
            Ok(plan) => {
                let duration_ms = js_sys::Date::now() - started_at;
                let plan_rows = plan.plan.len();
                let value = to_js_value(plan)?;
                if self.should_log_sqlite_timing(duration_ms, false) {
                    log_sqlite_operation("explainQuery", table, duration_ms, Some(plan_rows), None);
                }
                Ok(value)
            }
            Err(error) => {
                let duration_ms = js_sys::Date::now() - started_at;
                let message = error.to_string();
                if self.should_log_sqlite_timing(duration_ms, true) {
                    log_sqlite_operation("explainQuery", table, duration_ms, None, Some(message));
                }
                Err(to_js_error(error))
            }
        }
    }

    #[wasm_bindgen(js_name = subscribe)]
    pub fn subscribe(
        &mut self,
        query: JsValue,
        callback: js_sys::Function,
    ) -> Result<u32, JsValue> {
        let query = parse_built_query(query)?;
        let debug = self.debug_query_sql_for_log(&query);
        let table = query.table.clone();
        let started_at = js_sys::Date::now();
        let subscription = match self.runtime.subscribe_query(query.clone()) {
            Ok(subscription) => subscription,
            Err(error) => {
                let duration_ms = js_sys::Date::now() - started_at;
                let message = error.to_string();
                if self.should_log_sqlite_timing(duration_ms, true) {
                    let plan = explain_query_for_log(&self.runtime, &query);
                    log_sqlite_query(
                        "subscribe",
                        table,
                        debug,
                        plan,
                        duration_ms,
                        None,
                        Some(message),
                    );
                }
                return Err(to_js_error(error));
            }
        };
        let duration_ms = js_sys::Date::now() - started_at;
        if self.should_log_sqlite_timing(duration_ms, false) {
            let plan = explain_query_for_log(&self.runtime, &query);
            log_sqlite_query(
                "subscribe",
                table,
                debug,
                plan,
                duration_ms,
                Some(subscription.initial_delta().all.len()),
                None,
            );
        }
        let initial = subscription.initial_delta();
        let initial = to_js_value(initial)?;
        let id = self.next_subscription_id;
        self.next_subscription_id = self
            .next_subscription_id
            .checked_add(1)
            .ok_or_else(|| JsValue::from_str("subscription id overflow"))?;
        self.subscriptions.insert(
            id,
            WasmRowsSubscription {
                subscription,
                callback,
            },
        );
        let initial_callback = self
            .subscriptions
            .get(&id)
            .map(|entry| entry.callback.clone())
            .ok_or_else(|| JsValue::from_str("subscription disappeared before initial callback"))?;
        if let Err(error) = initial_callback.call1(&JsValue::UNDEFINED, &initial) {
            self.subscriptions.remove(&id);
            return Err(error);
        }
        Ok(id)
    }

    #[wasm_bindgen(js_name = unsubscribe)]
    pub fn unsubscribe(&mut self, handle: u32) {
        self.subscriptions.remove(&handle);
    }

    #[wasm_bindgen(js_name = readRows)]
    pub fn read_rows(&self, table_name: &str) -> Result<JsValue, JsValue> {
        let started_at = js_sys::Date::now();
        match self.runtime.read_rows(table_name) {
            Ok(rows) => {
                let duration_ms = js_sys::Date::now() - started_at;
                let row_count = rows.len();
                let value = to_js_value(rows)?;
                if self.should_log_sqlite_timing(duration_ms, false) {
                    log_sqlite_operation(
                        "readRows",
                        table_name.to_owned(),
                        duration_ms,
                        Some(row_count),
                        None,
                    );
                }
                Ok(value)
            }
            Err(error) => {
                let duration_ms = js_sys::Date::now() - started_at;
                let message = error.to_string();
                if self.should_log_sqlite_timing(duration_ms, true) {
                    log_sqlite_operation(
                        "readRows",
                        table_name.to_owned(),
                        duration_ms,
                        None,
                        Some(message),
                    );
                }
                Err(to_js_error(error))
            }
        }
    }

    #[wasm_bindgen(js_name = storageStats)]
    pub fn storage_stats(&self) -> Result<JsValue, JsValue> {
        to_js_value(self.runtime.storage_stats().map_err(to_js_error)?)
    }

    #[wasm_bindgen(js_name = storageFormatVersion)]
    pub fn storage_format_version(&self) -> Result<i64, JsValue> {
        self.runtime.storage_format_version().map_err(to_js_error)
    }
}

impl MiniJazzRuntime {
    fn new(runtime: Runtime) -> Self {
        Self {
            runtime,
            flags: MiniJazzFlags::default(),
            subscriptions: BTreeMap::new(),
            next_subscription_id: 0,
            notifying_subscriptions: false,
            notify_subscriptions_again: false,
        }
    }

    fn should_log_sqlite_timing(&self, duration_ms: f64, is_error: bool) -> bool {
        self.flags.sqlite_timing_logs && (is_error || duration_ms >= SLOW_QUERY_LOG_THRESHOLD_MS)
    }

    fn debug_query_sql_for_log(
        &self,
        query: &BuiltQuery,
    ) -> Option<mini_jazz_sqlite::SqliteQueryDebug> {
        if self.flags.sqlite_timing_logs {
            self.runtime.debug_query_sql(query).ok()
        } else {
            None
        }
    }

    fn notify_subscriptions(&mut self) -> Result<(), JsValue> {
        if self.notifying_subscriptions {
            self.notify_subscriptions_again = true;
            return Ok(());
        }

        self.notifying_subscriptions = true;
        let mut first_error = None;
        let mut skip_retry_ids = BTreeSet::new();
        loop {
            self.notify_subscriptions_again = false;
            if let Err(error) = self.notify_subscriptions_once(&skip_retry_ids) {
                skip_retry_ids.extend(error.failed_ids);
                if first_error.is_none() {
                    first_error = Some(error.error);
                }
            }
            if !self.notify_subscriptions_again {
                break;
            }
        }
        self.notifying_subscriptions = false;
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn notify_subscriptions_once(
        &mut self,
        skip_retry_ids: &BTreeSet<u32>,
    ) -> Result<(), NotificationError> {
        let runtime = &self.runtime;
        let ids = self.subscriptions.keys().copied().collect::<Vec<_>>();
        let mut pending = Vec::new();

        for id in ids {
            if skip_retry_ids.contains(&id) {
                continue;
            }
            let Some(entry) = self.subscriptions.get(&id) else {
                continue;
            };
            let previous_subscription = entry.subscription.clone();
            let mut next_subscription = entry.subscription.clone();
            let delta = runtime
                .subscription_delta(&mut next_subscription)
                .map_err(to_js_error)?;
            if !delta.delta.is_empty() {
                pending.push(PendingNotification {
                    id,
                    previous_subscription,
                    next_subscription,
                    callback: entry.callback.clone(),
                    value: to_js_value(delta)?,
                });
            } else if let Some(entry) = self.subscriptions.get_mut(&id) {
                entry.subscription = next_subscription;
            }
        }

        let mut first_error = None;
        let mut failed_ids = Vec::new();
        for notification in pending {
            if let Some(entry) = self.subscriptions.get_mut(&notification.id) {
                entry.subscription = notification.next_subscription.clone();
            } else {
                continue;
            }

            if let Err(error) = notification
                .callback
                .call1(&JsValue::UNDEFINED, &notification.value)
            {
                if let Some(entry) = self.subscriptions.get_mut(&notification.id) {
                    entry.subscription = notification.previous_subscription;
                }
                failed_ids.push(notification.id);
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }

        match first_error {
            Some(error) => Err(NotificationError { error, failed_ids }),
            None => Ok(()),
        }
    }
}

#[derive(Serialize)]
struct SqliteTimingLog {
    table: String,
    duration_ms: f64,
    row_count: Option<usize>,
    sql: Option<String>,
    params: Option<Vec<JsonValue>>,
    plan: Option<String>,
    plan_rows: Option<Vec<SqliteQueryPlanRow>>,
    error: Option<String>,
}

fn log_sqlite_query(
    operation: &str,
    table: String,
    debug: Option<mini_jazz_sqlite::SqliteQueryDebug>,
    plan: Option<SqliteQueryPlan>,
    duration_ms: f64,
    row_count: Option<usize>,
    error: Option<String>,
) {
    let plan_text = plan
        .as_ref()
        .map(|plan| format_sqlite_query_plan(&plan.plan));
    let log = SqliteTimingLog {
        table,
        duration_ms,
        row_count,
        sql: debug.as_ref().map(|debug| debug.sql.clone()),
        params: debug.map(|debug| debug.params),
        plan: plan_text,
        plan_rows: plan.map(|plan| plan.plan),
        error,
    };
    log_to_console(&format!("[mini-jazz-sqlite] {operation}"), &log);
}

fn log_sqlite_operation(
    operation: &str,
    table: String,
    duration_ms: f64,
    row_count: Option<usize>,
    error: Option<String>,
) {
    let log = SqliteTimingLog {
        table,
        duration_ms,
        row_count,
        sql: None,
        params: None,
        plan: None,
        plan_rows: None,
        error,
    };
    log_to_console(&format!("[mini-jazz-sqlite] {operation}"), &log);
}

fn explain_query_for_log(runtime: &Runtime, query: &BuiltQuery) -> Option<SqliteQueryPlan> {
    runtime.explain_query_plan(query).ok()
}

fn format_sqlite_query_plan(rows: &[SqliteQueryPlanRow]) -> String {
    let mut children_by_parent = BTreeMap::<i64, Vec<&SqliteQueryPlanRow>>::new();
    let mut ids = BTreeSet::new();
    for row in rows {
        ids.insert(row.id);
        children_by_parent.entry(row.parent).or_default().push(row);
    }

    let roots = rows
        .iter()
        .filter(|row| row.parent == 0 || !ids.contains(&row.parent))
        .collect::<Vec<_>>();
    let mut lines = vec!["QUERY PLAN".to_owned()];
    append_plan_rows(&mut lines, &roots, &children_by_parent, "");
    lines.join("\n")
}

fn append_plan_rows(
    lines: &mut Vec<String>,
    rows: &[&SqliteQueryPlanRow],
    children_by_parent: &BTreeMap<i64, Vec<&SqliteQueryPlanRow>>,
    prefix: &str,
) {
    for (idx, row) in rows.iter().enumerate() {
        let is_last = idx + 1 == rows.len();
        let connector = if is_last { "`--" } else { "|--" };
        lines.push(format!("{prefix}{connector}{}", row.detail));
        if let Some(children) = children_by_parent.get(&row.id) {
            let next_prefix = format!("{}{}", prefix, if is_last { "   " } else { "|  " });
            append_plan_rows(lines, children, children_by_parent, &next_prefix);
        }
    }
}

fn log_to_console(label: &str, payload: &impl Serialize) {
    let Ok(value) = to_js_value(payload) else {
        return;
    };
    let global = js_sys::global();
    let Ok(console) = js_sys::Reflect::get(&global, &JsValue::from_str("console")) else {
        return;
    };
    let method = js_sys::Reflect::get(&console, &JsValue::from_str("debug"))
        .or_else(|_| js_sys::Reflect::get(&console, &JsValue::from_str("log")));
    let Ok(method) = method else {
        return;
    };
    let Ok(method) = method.dyn_into::<js_sys::Function>() else {
        return;
    };
    let _ = method.call2(&console, &JsValue::from_str(label), &value);
}

fn parse_values(value: JsValue) -> Result<BTreeMap<String, JsonValue>, JsValue> {
    serde_wasm_bindgen::from_value(value)
        .map_err(|error| JsValue::from_str(&format!("invalid row values: {error}")))
}

fn parse_row_mutations(value: JsValue) -> Result<Vec<WasmRowMutation>, JsValue> {
    serde_wasm_bindgen::from_value(value)
        .map_err(|error| JsValue::from_str(&format!("invalid row mutations: {error}")))
}

fn parse_built_query(value: JsValue) -> Result<BuiltQuery, JsValue> {
    if let Some(query) = value.as_string() {
        return BuiltQuery::from_json_str(&query).map_err(to_js_error);
    }
    BuiltQuery::from_json_value(parse_json_value(value)?).map_err(to_js_error)
}

fn parse_json_value(value: JsValue) -> Result<JsonValue, JsValue> {
    serde_wasm_bindgen::from_value(value)
        .map_err(|error| JsValue::from_str(&format!("invalid JSON value: {error}")))
}

fn to_js_value<T: Serialize>(value: T) -> Result<JsValue, JsValue> {
    value
        .serialize(&serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true))
        .map_err(|error| JsValue::from_str(&format!("serialize result: {error}")))
}

fn to_js_error(error: mini_jazz_sqlite::Error) -> JsValue {
    JsValue::from_str(&error.to_string())
}

#[cfg(test)]
mod native_tests {
    use super::*;

    #[test]
    fn sqlite_timing_logs_require_explicit_flag() {
        let runtime = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
        let mut runtime = MiniJazzRuntime::new(runtime);

        assert!(!runtime.should_log_sqlite_timing(SLOW_QUERY_LOG_THRESHOLD_MS, false));
        assert!(!runtime.should_log_sqlite_timing(0.0, true));
        assert!(!runtime.get_mini_jazz_flag(SQLITE_TIMING_LOGS_FLAG).unwrap());

        runtime
            .set_mini_jazz_flag(SQLITE_TIMING_LOGS_FLAG, true)
            .unwrap();

        assert!(runtime.get_mini_jazz_flag(SQLITE_TIMING_LOGS_FLAG).unwrap());
        assert!(!runtime.should_log_sqlite_timing(SLOW_QUERY_LOG_THRESHOLD_MS - 1.0, false));
        assert!(runtime.should_log_sqlite_timing(SLOW_QUERY_LOG_THRESHOLD_MS, false));
        assert!(runtime.should_log_sqlite_timing(0.0, true));
    }
}

#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;
    use serde_json::json;
    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::rc::Rc;
    use wasm_bindgen::JsCast;
    use wasm_bindgen_test::*;

    fn reset_test_globals() {
        let global = js_sys::global();
        js_sys::Reflect::set(
            &global,
            &JsValue::from_str("__miniJazzDeltas"),
            &js_sys::Array::new(),
        )
        .unwrap();
        js_sys::Reflect::set(
            &global,
            &JsValue::from_str("__miniJazzThrowNext"),
            &JsValue::FALSE,
        )
        .unwrap();
        js_sys::Reflect::set(
            &global,
            &JsValue::from_str("__miniJazzSeenInitial"),
            &JsValue::FALSE,
        )
        .unwrap();
    }

    fn observed_deltas() -> js_sys::Array {
        js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("__miniJazzDeltas"))
            .unwrap()
            .dyn_into()
            .unwrap()
    }

    fn set_throw_next() {
        js_sys::Reflect::set(
            &js_sys::global(),
            &JsValue::from_str("__miniJazzThrowNext"),
            &JsValue::TRUE,
        )
        .unwrap();
    }

    fn project_values(title: &str) -> JsValue {
        to_js_value(BTreeMap::from([("title".to_owned(), json!(title))])).unwrap()
    }

    #[wasm_bindgen_test]
    fn subscription_callback_error_is_returned_and_delta_retried() {
        reset_test_globals();
        let mut runtime = MiniJazzRuntime::open_memory("alice-node", "alice").unwrap();
        let callback = js_sys::Function::new_with_args(
            "delta",
            r#"
            if (globalThis.__miniJazzThrowNext) {
                globalThis.__miniJazzThrowNext = false;
                throw new Error("callback failed");
            }
            globalThis.__miniJazzDeltas.push({
                all: delta.all.map((row) => row.id),
                kinds: delta.delta.map((change) => change.kind),
                titles: delta.all.map((row) => row.values.title),
            });
            "#,
        );

        runtime
            .subscribe(JsValue::from_str(r#"{"table":"projects"}"#), callback)
            .unwrap();
        assert_eq!(observed_deltas().length(), 1);

        set_throw_next();
        let err = runtime
            .insert_row("projects", "project-1", project_values("First"))
            .unwrap_err();
        let message = js_sys::Reflect::get(&err, &JsValue::from_str("message"))
            .unwrap()
            .as_string()
            .unwrap();
        assert_eq!(message, "callback failed");
        assert_eq!(observed_deltas().length(), 1);

        runtime
            .update_row("projects", "project-1", project_values("Retried"))
            .unwrap();
        let deltas = observed_deltas();
        assert_eq!(deltas.length(), 2);
        let retried = deltas.get(1);
        let kinds: js_sys::Array = js_sys::Reflect::get(&retried, &JsValue::from_str("kinds"))
            .unwrap()
            .dyn_into()
            .unwrap();
        let titles: js_sys::Array = js_sys::Reflect::get(&retried, &JsValue::from_str("titles"))
            .unwrap()
            .dyn_into()
            .unwrap();

        assert_eq!(kinds.get(0).as_f64(), Some(0.0));
        assert_eq!(titles.get(0).as_string().as_deref(), Some("Retried"));
    }

    #[wasm_bindgen_test]
    fn throwing_subscription_callback_does_not_block_later_callbacks() {
        reset_test_globals();
        let mut runtime = MiniJazzRuntime::open_memory("alice-node", "alice").unwrap();
        let throwing = js_sys::Function::new_with_args(
            "delta",
            r#"
            if (globalThis.__miniJazzThrowNext) {
                globalThis.__miniJazzThrowNext = false;
                throw new Error("callback failed");
            }
            "#,
        );
        let observer = js_sys::Function::new_with_args(
            "delta",
            r#"
            globalThis.__miniJazzDeltas.push({
                all: delta.all.map((row) => row.id),
                kinds: delta.delta.map((change) => change.kind),
            });
            "#,
        );

        runtime
            .subscribe(JsValue::from_str(r#"{"table":"projects"}"#), throwing)
            .unwrap();
        runtime
            .subscribe(JsValue::from_str(r#"{"table":"projects"}"#), observer)
            .unwrap();
        assert_eq!(observed_deltas().length(), 1);

        set_throw_next();
        let err = runtime
            .insert_row("projects", "project-1", project_values("First"))
            .unwrap_err();
        let message = js_sys::Reflect::get(&err, &JsValue::from_str("message"))
            .unwrap()
            .as_string()
            .unwrap();
        assert_eq!(message, "callback failed");

        let deltas = observed_deltas();
        assert_eq!(deltas.length(), 2);
        let delivered = deltas.get(1);
        let all: js_sys::Array = js_sys::Reflect::get(&delivered, &JsValue::from_str("all"))
            .unwrap()
            .dyn_into()
            .unwrap();
        assert_eq!(all.get(0).as_string().as_deref(), Some("project-1"));
    }

    #[wasm_bindgen_test]
    fn subscription_initial_callback_write_is_reported_to_new_subscription() {
        reset_test_globals();
        let mut runtime = MiniJazzRuntime::open_memory("alice-node", "alice").unwrap();
        let runtime_ptr = &mut runtime as *mut MiniJazzRuntime;
        let write_closure = Closure::<dyn FnMut()>::new(move || unsafe {
            (*runtime_ptr)
                .insert_row("projects", "project-1", project_values("Initial write"))
                .unwrap();
        });
        js_sys::Reflect::set(
            &js_sys::global(),
            &JsValue::from_str("__miniJazzInitialWrite"),
            write_closure.as_ref(),
        )
        .unwrap();
        let callback = js_sys::Function::new_with_args(
            "delta",
            r#"
            globalThis.__miniJazzDeltas.push({
                allLength: delta.all.length,
            });
            if (!globalThis.__miniJazzSeenInitial) {
                globalThis.__miniJazzSeenInitial = true;
                globalThis.__miniJazzInitialWrite();
            }
            "#,
        );

        runtime
            .subscribe(JsValue::from_str(r#"{"table":"projects"}"#), callback)
            .unwrap();

        let deltas = observed_deltas();
        assert_eq!(deltas.length(), 2);
        let refreshed = deltas.get(1);
        assert_eq!(
            js_sys::Reflect::get(&refreshed, &JsValue::from_str("allLength"))
                .unwrap()
                .as_f64(),
            Some(1.0)
        );
    }

    #[wasm_bindgen_test]
    fn subscription_nested_write_still_notifies_when_later_callback_throws() {
        reset_test_globals();
        let mut runtime = MiniJazzRuntime::open_memory("alice-node", "alice").unwrap();
        let seen_initial = Rc::new(Cell::new(false));
        let wrote_nested_row = Rc::new(Cell::new(false));
        let writer_callback_count = Rc::new(Cell::new(0));
        let runtime_ptr = &mut runtime as *mut MiniJazzRuntime;
        let writer_closure = Closure::<dyn FnMut(JsValue)>::new({
            let seen_initial = Rc::clone(&seen_initial);
            let wrote_nested_row = Rc::clone(&wrote_nested_row);
            let writer_callback_count = Rc::clone(&writer_callback_count);
            move |_| {
                writer_callback_count.set(writer_callback_count.get() + 1);
                if seen_initial.replace(true) && !wrote_nested_row.replace(true) {
                    unsafe {
                        (*runtime_ptr)
                            .insert_row("projects", "project-2", project_values("Nested"))
                            .unwrap();
                    }
                }
            }
        });
        let writer = writer_closure
            .as_ref()
            .unchecked_ref::<js_sys::Function>()
            .clone();
        let throwing = js_sys::Function::new_with_args(
            "delta",
            r#"
            if (globalThis.__miniJazzThrowNext) {
                globalThis.__miniJazzThrowNext = false;
                throw new Error("callback failed");
            }
            "#,
        );

        runtime
            .subscribe(JsValue::from_str(r#"{"table":"projects"}"#), writer)
            .unwrap();
        runtime
            .subscribe(JsValue::from_str(r#"{"table":"projects"}"#), throwing)
            .unwrap();
        assert_eq!(writer_callback_count.get(), 1);

        set_throw_next();
        let err = runtime
            .insert_row("projects", "project-1", project_values("First"))
            .unwrap_err();
        let message = js_sys::Reflect::get(&err, &JsValue::from_str("message"))
            .unwrap()
            .as_string()
            .unwrap();
        assert_eq!(message, "callback failed");
        assert_eq!(writer_callback_count.get(), 3);
    }

    #[wasm_bindgen_test]
    fn subscription_callback_can_unsubscribe_current_handle() {
        let mut runtime = MiniJazzRuntime::open_memory("alice-node", "alice").unwrap();
        let handle = Rc::new(Cell::new(None::<u32>));
        let seen_initial = Rc::new(Cell::new(false));
        let callback_count = Rc::new(Cell::new(0));
        let runtime_ptr = &mut runtime as *mut MiniJazzRuntime;
        let callback_closure = Closure::<dyn FnMut(JsValue)>::new({
            let handle = Rc::clone(&handle);
            let seen_initial = Rc::clone(&seen_initial);
            let callback_count = Rc::clone(&callback_count);
            move |_| {
                callback_count.set(callback_count.get() + 1);
                if seen_initial.replace(true) {
                    unsafe {
                        (*runtime_ptr).unsubscribe(handle.get().expect("handle is set"));
                    }
                }
            }
        });
        let callback = callback_closure
            .as_ref()
            .unchecked_ref::<js_sys::Function>()
            .clone();

        let subscription_handle = runtime
            .subscribe(JsValue::from_str(r#"{"table":"projects"}"#), callback)
            .unwrap();
        handle.set(Some(subscription_handle));
        assert_eq!(callback_count.get(), 1);

        runtime
            .insert_row("projects", "project-1", project_values("First"))
            .unwrap();
        assert_eq!(callback_count.get(), 2);

        runtime
            .update_row("projects", "project-1", project_values("Second"))
            .unwrap();
        assert_eq!(callback_count.get(), 2);
    }

    #[wasm_bindgen_test]
    fn subscription_callback_can_write_another_row() {
        let mut runtime = MiniJazzRuntime::open_memory("alice-node", "alice").unwrap();
        let seen_initial = Rc::new(Cell::new(false));
        let wrote_nested_row = Rc::new(Cell::new(false));
        let callback_count = Rc::new(Cell::new(0));
        let runtime_ptr = &mut runtime as *mut MiniJazzRuntime;
        let callback_closure = Closure::<dyn FnMut(JsValue)>::new({
            let seen_initial = Rc::clone(&seen_initial);
            let wrote_nested_row = Rc::clone(&wrote_nested_row);
            let callback_count = Rc::clone(&callback_count);
            move |_| {
                callback_count.set(callback_count.get() + 1);
                if seen_initial.replace(true) && !wrote_nested_row.replace(true) {
                    unsafe {
                        (*runtime_ptr)
                            .insert_row("projects", "project-2", project_values("Nested"))
                            .unwrap();
                    }
                }
            }
        });
        let callback = callback_closure
            .as_ref()
            .unchecked_ref::<js_sys::Function>()
            .clone();

        runtime
            .subscribe(JsValue::from_str(r#"{"table":"projects"}"#), callback)
            .unwrap();
        assert_eq!(callback_count.get(), 1);

        runtime
            .insert_row("projects", "project-1", project_values("First"))
            .unwrap();
        assert_eq!(callback_count.get(), 3);
        let rows: js_sys::Array = runtime.read_rows("projects").unwrap().dyn_into().unwrap();
        assert_eq!(rows.length(), 2);
    }
}

#[cfg(target_arch = "wasm32")]
async fn install_opfs_vfs(db_name: &str) -> Result<(), JsValue> {
    let pool_name = opfs_pool_name(db_name);
    let pool_directory = format!(".{pool_name}");
    let config = sqlite_wasm_vfs::sahpool::OpfsSAHPoolCfgBuilder::new()
        .vfs_name(&pool_name)
        .directory(&pool_directory)
        .build();
    sqlite_wasm_vfs::sahpool::install::<sqlite_wasm_rs::WasmOsCallback>(&config, true)
        .await
        .map_err(|error| JsValue::from_str(&format!("install OPFS SQLite VFS: {error}")))?;
    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn opfs_pool_name(db_name: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in db_name.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("mini-jazz-sqlite-{hash:016x}")
}
