use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use jazz_tools::binding_support::parse_external_object_id;
use jazz_tools::client_core::{
    ClientConfig, ClientRuntimeFlavor, JazzClientCore, LocalRuntimeHost, WriteBatchContextCore,
    WriteOptions, WriteResultCore,
};
use jazz_tools::query_manager::types::{RowPolicyMode, Value};
use jazz_tools::runtime_core::{NoopScheduler, RuntimeCore};
use jazz_tools::schema_manager::{AppId, SchemaManager};
use jazz_tools::storage::MemoryStorage;
use jazz_tools::sync_manager::SyncManager;
use serde::Serialize;
use wasm_bindgen::prelude::*;

type WasmJazzClientCore = JazzClientCore<LocalRuntimeHost<MemoryStorage, NoopScheduler>>;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WasmClientInsertResult {
    id: String,
    values: Vec<Value>,
    batch_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WasmClientMutationResult {
    batch_id: String,
}

fn serialize_insert_result(result: WriteResultCore) -> Result<JsValue, JsError> {
    let payload = WasmClientInsertResult {
        id: result.row.id.uuid().to_string(),
        values: result.row.values,
        batch_id: result.handle.batch_id.to_string(),
    };
    let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
    payload
        .serialize(&serializer)
        .map_err(|error| JsError::new(&format!("Serialization failed: {error:?}")))
}

#[wasm_bindgen]
pub struct WasmJazzClient {
    inner: WasmJazzClientCore,
}

#[wasm_bindgen]
pub struct WasmJazzClientBatch {
    inner: WasmJazzClientCore,
    context: Option<WriteBatchContextCore>,
}

#[wasm_bindgen]
impl WasmJazzClient {
    #[wasm_bindgen(constructor)]
    pub fn new(
        schema_json: &str,
        app_id: &str,
        env: &str,
        user_branch: &str,
    ) -> Result<Self, JsError> {
        let runtime_schema =
            jazz_tools::binding_support::parse_runtime_schema_input(schema_json)
                .map_err(|error| JsError::new(&format!("Invalid schema JSON: {error}")))?;
        let schema = runtime_schema.schema;
        let app = AppId::from_string(app_id).unwrap_or_else(|_| AppId::from_name(app_id));
        let schema_manager = SchemaManager::new_with_policy_mode(
            SyncManager::new(),
            schema.clone(),
            app,
            env,
            user_branch,
            if runtime_schema.loaded_policy_bundle {
                RowPolicyMode::Enforcing
            } else {
                RowPolicyMode::PermissiveLocal
            },
        )
        .map_err(|error| JsError::new(&format!("Failed to create SchemaManager: {error:?}")))?;

        let runtime = RuntimeCore::new(schema_manager, MemoryStorage::new(), NoopScheduler);
        let host = LocalRuntimeHost::new(Rc::new(RefCell::new(runtime)));
        let mut config = ClientConfig::memory_for_test(app_id, schema);
        config.env = env.to_string();
        config.user_branch = user_branch.to_string();
        config.runtime_flavor = ClientRuntimeFlavor::BrowserMainThread;

        Ok(Self {
            inner: JazzClientCore::from_runtime_host(config, host)
                .map_err(|error| JsError::new(&error.to_string()))?,
        })
    }

    #[wasm_bindgen(js_name = beginDirectBatch)]
    pub fn begin_direct_batch(&self) -> WasmJazzClientBatch {
        WasmJazzClientBatch {
            inner: self.inner.clone(),
            context: Some(self.inner.begin_direct_batch_context()),
        }
    }

    #[wasm_bindgen]
    pub fn insert(
        &mut self,
        table: &str,
        values: JsValue,
        object_id: Option<String>,
    ) -> Result<JsValue, JsError> {
        let values: HashMap<String, Value> = serde_wasm_bindgen::from_value(values)?;
        let object_id = parse_external_object_id(object_id.as_deref())
            .map_err(|message| JsError::new(&message))?;
        let result = self
            .inner
            .insert(
                table,
                values,
                Some(WriteOptions {
                    object_id,
                    ..Default::default()
                }),
            )
            .map_err(|error| JsError::new(&error.to_string()))?;
        serialize_insert_result(result)
    }
}

#[wasm_bindgen]
impl WasmJazzClientBatch {
    #[wasm_bindgen]
    pub fn insert(
        &mut self,
        table: &str,
        values: JsValue,
        object_id: Option<String>,
    ) -> Result<JsValue, JsError> {
        let context = self
            .context
            .as_ref()
            .ok_or_else(|| JsError::new("Direct batch has already been committed"))?;
        let values: HashMap<String, Value> = serde_wasm_bindgen::from_value(values)?;
        let object_id = parse_external_object_id(object_id.as_deref())
            .map_err(|message| JsError::new(&message))?;
        let result = self
            .inner
            .insert_in_batch(
                context,
                table,
                values,
                Some(WriteOptions {
                    object_id,
                    ..Default::default()
                }),
            )
            .map_err(|error| JsError::new(&error.to_string()))?;
        serialize_insert_result(result)
    }

    #[wasm_bindgen]
    pub fn update(&mut self, object_id: &str, values: JsValue) -> Result<JsValue, JsError> {
        let context = self
            .context
            .as_ref()
            .ok_or_else(|| JsError::new("Direct batch has already been committed"))?;
        let object_id = parse_external_object_id(Some(object_id))
            .map_err(|message| JsError::new(&message))?
            .ok_or_else(|| JsError::new("Object id is required"))?;
        let values: HashMap<String, Value> = serde_wasm_bindgen::from_value(values)?;
        let handle = self
            .inner
            .update_in_batch(context, object_id, values.into_iter().collect(), None)
            .map_err(|error| JsError::new(&error.to_string()))?;
        let payload = WasmClientMutationResult {
            batch_id: handle.batch_id.to_string(),
        };
        let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
        payload
            .serialize(&serializer)
            .map_err(|error| JsError::new(&format!("Serialization failed: {error:?}")))
    }

    #[wasm_bindgen(js_name = delete)]
    pub fn delete_row(&mut self, object_id: &str) -> Result<JsValue, JsError> {
        let context = self
            .context
            .as_ref()
            .ok_or_else(|| JsError::new("Direct batch has already been committed"))?;
        let object_id = parse_external_object_id(Some(object_id))
            .map_err(|message| JsError::new(&message))?
            .ok_or_else(|| JsError::new("Object id is required"))?;
        let handle = self
            .inner
            .delete_in_batch(context, object_id, None)
            .map_err(|error| JsError::new(&error.to_string()))?;
        let payload = WasmClientMutationResult {
            batch_id: handle.batch_id.to_string(),
        };
        let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
        payload
            .serialize(&serializer)
            .map_err(|error| JsError::new(&format!("Serialization failed: {error:?}")))
    }

    #[wasm_bindgen]
    pub fn commit(&mut self) -> Result<JsValue, JsError> {
        let context = self
            .context
            .take()
            .ok_or_else(|| JsError::new("Direct batch has already been committed"))?;
        let handle = self
            .inner
            .commit_batch_context(context)
            .map_err(|error| JsError::new(&error.to_string()))?;
        let payload = WasmClientMutationResult {
            batch_id: handle.batch_id.to_string(),
        };
        let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
        payload
            .serialize(&serializer)
            .map_err(|error| JsError::new(&format!("Serialization failed: {error:?}")))
    }
}
