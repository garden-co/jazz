use std::collections::BTreeMap;

use mini_jazz_sqlite::{BuiltQuery, RowsSubscription, Runtime, Storage};
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
    notifying_subscriptions: bool,
    notify_subscriptions_again: bool,
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
            notifying_subscriptions: false,
            notify_subscriptions_again: false,
        }
    }

    fn notify_subscriptions(&mut self) -> Result<(), JsValue> {
        if self.notifying_subscriptions {
            self.notify_subscriptions_again = true;
            return Ok(());
        }

        self.notifying_subscriptions = true;
        loop {
            self.notify_subscriptions_again = false;
            if let Err(error) = self.notify_subscriptions_once() {
                self.notifying_subscriptions = false;
                self.notify_subscriptions_again = false;
                return Err(error);
            }
            if !self.notify_subscriptions_again {
                break;
            }
        }
        self.notifying_subscriptions = false;
        Ok(())
    }

    fn notify_subscriptions_once(&mut self) -> Result<(), JsValue> {
        let runtime = &self.runtime;
        let ids = self.subscriptions.keys().copied().collect::<Vec<_>>();
        let mut pending = Vec::new();

        for id in ids {
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
                return Err(error);
            }
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
fn opfs_pool_name(db_name: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in db_name.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("mini-jazz-sqlite-{hash:016x}")
}
