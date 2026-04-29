//! jazz-wasm - WebAssembly bindings for the Jazz database engine.
//!
//! This crate provides JavaScript bindings for the Jazz local-first database engine,
//! enabling TypeScript/JavaScript applications to use Jazz with custom storage backends.
//!
//! # Architecture
//!
//! - **WasmRuntime**: Main entry point that wraps SchemaManager and provides CRUD operations
//! - **WasmQueryBuilder**: Fluent query builder exposed to JavaScript
//! - **JsStorageDriver**: Interface for JavaScript storage implementations (IndexedDB, node:sqlite)
//! - **Type bridges**: Serialization between Rust and JavaScript types
//!
//! # Usage
//!
//! ```javascript
//! import { WasmRuntime, WasmQueryBuilder } from 'jazz-wasm';
//!
//! // Create a storage driver (e.g., IndexedDB)
//! const driver = {
//!   async process(requests) {
//!     // Handle storage requests
//!     return responses;
//!   }
//! };
//!
//! // Create runtime
//! const schema = { todos: { columns: [...] } };
//! const runtime = new WasmRuntime(driver, JSON.stringify(schema), 'my-app', 'dev', 'main');
//!
//! // Insert a row
//! const id = await runtime.insert('todos', {
//!   title: { type: 'Text', value: 'Buy milk' },
//!   done: { type: 'Boolean', value: false },
//! });
//!
//! // Query with builder
//! const query = new WasmQueryBuilder('todos').branch('main').build();
//! const results = await runtime.query(query);
//!
//! // Subscribe to changes
//! const subId = await runtime.subscribe(query, (delta) => {
//!   console.log('Changes:', delta);
//! });
//!
//! // Tick must be called periodically
//! setInterval(() => runtime.tick(), 100);
//! ```

#![allow(clippy::new_without_default)]

pub mod driver_bridge;
pub mod query;
pub mod runtime;
pub mod types;
#[cfg(target_arch = "wasm32")]
pub mod ws_stream;

// Re-export main types for JavaScript
pub use driver_bridge::JsStorageDriver;
pub use query::WasmQueryBuilder;
pub use runtime::WasmRuntime;

use jazz_tools::sync_manager::SyncPayload;
use js_sys::Uint8Array;
use wasm_bindgen::prelude::*;

/// Initialize the WASM module.
///
/// Sets up panic hook for better error messages in the browser console.
#[wasm_bindgen(start)]
pub fn init() {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}

/// Parse a schema from JSON string.
///
/// Returns the schema as a JsValue for inspection.
#[wasm_bindgen(js_name = parseSchema)]
pub fn parse_schema(json: &str) -> Result<JsValue, JsError> {
    let schema: types::Schema =
        serde_json::from_str(json).map_err(|e| JsError::new(&format!("Parse error: {}", e)))?;
    Ok(serde_wasm_bindgen::to_value(&schema)?)
}

/// Generate a new UUID v7 (time-ordered).
///
/// Useful when a caller wants the default generated row-id shape.
#[wasm_bindgen(js_name = generateId)]
pub fn generate_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

/// Get the current timestamp in microseconds since Unix epoch.
#[wasm_bindgen(js_name = currentTimestamp)]
pub fn current_timestamp() -> u64 {
    use web_time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// Decode a sync payload into telemetry records for JS OTLP export.
#[wasm_bindgen(js_name = decodeSyncPayloadTelemetry)]
pub fn decode_sync_payload_telemetry(payload: JsValue) -> Result<JsValue, JsError> {
    let payload = if let Some(json) = payload.as_string() {
        SyncPayload::from_json(&json)
            .map_err(|e| JsError::new(&format!("Invalid sync payload JSON: {e}")))?
    } else if payload.is_instance_of::<Uint8Array>() {
        let bytes = Uint8Array::new(&payload).to_vec();
        SyncPayload::from_bytes(&bytes)
            .map_err(|e| JsError::new(&format!("Invalid sync payload postcard: {e}")))?
    } else {
        return Err(JsError::new(
            "Invalid sync payload type: expected Uint8Array or JSON string",
        ));
    };

    let context = jazz_tools::sync_payload_telemetry::SyncPayloadTelemetryContext {
        scope: jazz_tools::sync_payload_telemetry::SyncPayloadTelemetryScope::WorkerBridge,
        direction: jazz_tools::sync_payload_telemetry::SyncPayloadTelemetryDirection::MainToWorker,
        ..Default::default()
    };
    let records = jazz_tools::sync_payload_telemetry::records_for_payload(&context, &payload);
    serde_wasm_bindgen::to_value(&serde_json::json!({ "records": records }))
        .map_err(|e| JsError::new(&format!("Encode telemetry records failed: {e}")))
}
