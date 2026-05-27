use std::collections::BTreeMap;

use mini_jazz_sqlite::sync::Bundle;
use mini_jazz_sqlite::{Runtime, Storage};
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
}

#[wasm_bindgen]
impl MiniJazzRuntime {
    #[wasm_bindgen(js_name = openMemory)]
    pub fn open_memory(node_id: &str, user: &str) -> Result<MiniJazzRuntime, JsValue> {
        Runtime::open(Storage::Memory, node_id, user)
            .map(|runtime| MiniJazzRuntime { runtime })
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
            .map(|runtime| MiniJazzRuntime { runtime })
            .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = insertRow)]
    pub fn insert_row(
        &mut self,
        table_name: &str,
        id: &str,
        values: JsValue,
    ) -> Result<String, JsValue> {
        self.runtime
            .insert_row(table_name, id, parse_values(values)?)
            .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = updateRow)]
    pub fn update_row(
        &mut self,
        table_name: &str,
        id: &str,
        values: JsValue,
    ) -> Result<String, JsValue> {
        self.runtime
            .update_row(table_name, id, parse_values(values)?)
            .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = deleteRow)]
    pub fn delete_row(&mut self, table_name: &str, id: &str) -> Result<String, JsValue> {
        self.runtime.delete_row(table_name, id).map_err(to_js_error)
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
        self.runtime.apply_bundle(&bundle).map_err(to_js_error)
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

fn parse_values(value: JsValue) -> Result<BTreeMap<String, JsonValue>, JsValue> {
    serde_wasm_bindgen::from_value(value)
        .map_err(|error| JsValue::from_str(&format!("invalid row values: {error}")))
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
