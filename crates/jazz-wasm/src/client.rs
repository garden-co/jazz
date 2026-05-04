use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use jazz_tools::binding_support::parse_external_object_id;
use jazz_tools::client_core::{ClientConfig, JazzClientCore, LocalRuntimeHost, WriteOptions};
use jazz_tools::query_manager::types::{RowPolicyMode, Value};
use jazz_tools::runtime_core::DirectInsertResult;
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

fn serialize_insert_result(result: DirectInsertResult) -> Result<JsValue, JsError> {
    let ((id, values), batch_id) = result;
    let payload = WasmClientInsertResult {
        id: id.uuid().to_string(),
        values,
        batch_id: batch_id.to_string(),
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
        let config = ClientConfig::new(env, user_branch);

        Ok(Self {
            inner: JazzClientCore::from_runtime_host(config, host),
        })
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
