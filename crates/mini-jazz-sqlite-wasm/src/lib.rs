use std::collections::BTreeMap;

use mini_jazz_sqlite::sync::Bundle;
use mini_jazz_sqlite::{BuiltQuery, RowsSubscription, Runtime, Storage, SubscriptionDelta};
use serde::Serialize;
use serde_json::Value as JsonValue;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(start)]
pub fn install_panic_hook() {
    console_error_panic_hook::set_once();
}

#[wasm_bindgen]
pub struct MiniJazzRuntime {
    runtime: Runtime,
    subscriptions: BTreeMap<u32, WasmRowsSubscription>,
    next_subscription_id: u32,
}

struct WasmRowsSubscription {
    subscription: RowsSubscription,
    callback: js_sys::Function,
}

#[wasm_bindgen]
impl MiniJazzRuntime {
    #[wasm_bindgen(js_name = openMemory)]
    pub fn open_memory(node_id: &str, user: &str) -> Result<MiniJazzRuntime, JsValue> {
        Runtime::open(Storage::Memory, node_id, user)
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
        let pool_name = opfs_pool_name(db_name);
        let pool_directory = format!(".{pool_name}");
        let config = sqlite_wasm_vfs::sahpool::OpfsSAHPoolCfgBuilder::new()
            .vfs_name(&pool_name)
            .directory(&pool_directory)
            .build();
        sqlite_wasm_vfs::sahpool::install::<sqlite_wasm_rs::WasmOsCallback>(&config, true)
            .await
            .map_err(|error| JsValue::from_str(&format!("install OPFS SQLite VFS: {error}")))?;

        Runtime::open(Storage::File(db_name.into()), node_id, user)
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

    #[wasm_bindgen(js_name = query)]
    pub fn query(&self, query: JsValue) -> Result<JsValue, JsValue> {
        to_js_value(
            self.runtime
                .query(parse_built_query(query)?)
                .map_err(to_js_error)?,
        )
    }

    #[wasm_bindgen(js_name = one)]
    pub fn one(&self, query: JsValue) -> Result<JsValue, JsValue> {
        to_js_value(
            self.runtime
                .one(parse_built_query(query)?)
                .map_err(to_js_error)?,
        )
    }

    #[wasm_bindgen(js_name = subscribe)]
    pub fn subscribe(
        &mut self,
        query: JsValue,
        callback: js_sys::Function,
    ) -> Result<u32, JsValue> {
        let subscription = self
            .runtime
            .subscribe_query(parse_built_query(query)?)
            .map_err(to_js_error)?;
        let initial = subscription.initial_delta();
        callback.call1(&JsValue::UNDEFINED, &to_js_value(initial)?)?;

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
        Ok(id)
    }

    #[wasm_bindgen(js_name = unsubscribe)]
    pub fn unsubscribe(&mut self, handle: u32) {
        self.subscriptions.remove(&handle);
    }

    #[wasm_bindgen(js_name = readRows)]
    pub fn read_rows(&self, table_name: &str) -> Result<JsValue, JsValue> {
        to_js_value(self.runtime.read_rows(table_name).map_err(to_js_error)?)
    }

    #[wasm_bindgen(js_name = readRowsWhereEq)]
    pub fn read_rows_where_eq(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsValue,
    ) -> Result<JsValue, JsValue> {
        let value = parse_json_value(value)?;
        to_js_value(
            self.runtime
                .read_rows_where_eq(table_name, field_name, value)
                .map_err(to_js_error)?,
        )
    }

    #[wasm_bindgen(js_name = readRowsWhereEqTopCreatedAtDesc)]
    pub fn read_rows_where_eq_top_created_at_desc(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsValue,
        limit: usize,
    ) -> Result<JsValue, JsValue> {
        let value = parse_json_value(value)?;
        to_js_value(
            self.runtime
                .read_rows_where_eq_top_created_at_desc(table_name, field_name, value, limit)
                .map_err(to_js_error)?,
        )
    }

    #[wasm_bindgen(js_name = exportTableHistory)]
    pub fn export_table_history(&self, table_name: &str) -> Result<JsValue, JsValue> {
        to_js_value(
            self.runtime
                .export_table_history(table_name)
                .map_err(to_js_error)?,
        )
    }

    #[wasm_bindgen(js_name = applyBundle)]
    pub fn apply_bundle(&mut self, bundle: JsValue) -> Result<(), JsValue> {
        let bundle: Bundle = serde_wasm_bindgen::from_value(bundle)
            .map_err(|error| JsValue::from_str(&format!("invalid bundle: {error}")))?;
        self.runtime.apply_bundle(&bundle).map_err(to_js_error)?;
        self.notify_subscriptions()
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
            subscriptions: BTreeMap::new(),
            next_subscription_id: 0,
        }
    }

    fn notify_subscriptions(&mut self) -> Result<(), JsValue> {
        let runtime = &self.runtime;
        let mut notifications: Vec<(js_sys::Function, SubscriptionDelta)> = Vec::new();

        for entry in self.subscriptions.values_mut() {
            let delta = runtime
                .subscription_delta(&mut entry.subscription)
                .map_err(to_js_error)?;
            if !delta.delta.is_empty() {
                notifications.push((entry.callback.clone(), delta));
            }
        }

        for (callback, delta) in notifications {
            let value = to_js_value(delta)?;
            let _ = callback.call1(&JsValue::UNDEFINED, &value);
        }

        Ok(())
    }
}

fn parse_values(value: JsValue) -> Result<BTreeMap<String, JsonValue>, JsValue> {
    serde_wasm_bindgen::from_value(value)
        .map_err(|error| JsValue::from_str(&format!("invalid row values: {error}")))
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

#[cfg(target_arch = "wasm32")]
fn opfs_pool_name(db_name: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in db_name.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("mini-jazz-sqlite-{hash:016x}")
}
