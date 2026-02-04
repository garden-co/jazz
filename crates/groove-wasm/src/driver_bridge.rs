//! Bridge between Rust Driver trait and JavaScript StorageDriver interface.
//!
//! This module provides `WasmDriverBridge`, which implements the Groove `Driver` trait
//! by delegating storage operations to a JavaScript `StorageDriver` implementation.

use wasm_bindgen::prelude::*;

use crate::types::{
    storage_request_to_wasm, wasm_response_to_storage, WasmStorageRequest, WasmStorageResponse,
};

// ============================================================================
// JavaScript Driver Interface
// ============================================================================

/// External JavaScript interface for a storage driver.
///
/// JavaScript implementations must provide a `process` method that takes
/// an array of storage requests and returns a Promise resolving to an array
/// of storage responses.
#[wasm_bindgen]
extern "C" {
    /// JavaScript storage driver interface.
    pub type JsStorageDriver;

    /// Process a batch of storage requests.
    ///
    /// Takes a JSON-encoded array of `WasmStorageRequest` objects and returns
    /// a Promise that resolves to a JSON-encoded array of `WasmStorageResponse` objects.
    #[wasm_bindgen(structural, method, catch)]
    pub async fn process(this: &JsStorageDriver, requests: JsValue) -> Result<JsValue, JsValue>;
}

// ============================================================================
// Wasm Driver Bridge
// ============================================================================

/// Bridge that adapts a JavaScript `StorageDriver` to the Rust `Driver` trait.
///
/// This is used internally by `WasmRuntime` to execute storage operations
/// against the JavaScript-provided storage backend.
pub struct WasmDriverBridge {
    driver: JsStorageDriver,
}

impl WasmDriverBridge {
    /// Create a new bridge wrapping a JavaScript storage driver.
    pub fn new(driver: JsStorageDriver) -> Self {
        Self { driver }
    }

    /// Process storage requests asynchronously via the JavaScript driver.
    ///
    /// Converts Groove `StorageRequest`s to `WasmStorageRequest`s, calls the
    /// JavaScript driver, and converts the responses back.
    pub async fn process_async(
        &self,
        requests: Vec<groove::storage::StorageRequest>,
    ) -> Result<Vec<groove::storage::StorageResponse>, String> {
        // Convert requests to WASM-serializable format
        let wasm_requests: Vec<WasmStorageRequest> =
            requests.into_iter().map(storage_request_to_wasm).collect();

        // Serialize to JsValue
        let js_requests = serde_wasm_bindgen::to_value(&wasm_requests)
            .map_err(|e| format!("Failed to serialize requests: {}", e))?;

        // Call JavaScript driver
        let js_responses = self
            .driver
            .process(js_requests)
            .await
            .map_err(|e| format!("JavaScript driver error: {:?}", e))?;

        // Deserialize responses
        let wasm_responses: Vec<WasmStorageResponse> = serde_wasm_bindgen::from_value(js_responses)
            .map_err(|e| format!("Failed to deserialize responses: {}", e))?;

        // Convert back to Groove types
        wasm_responses
            .into_iter()
            .map(wasm_response_to_storage)
            .collect()
    }
}

// ============================================================================
// Synchronous Driver Adapter (for compatibility with existing code)
// ============================================================================

/// Storage requests pending execution by the JavaScript driver.
///
/// Since the Groove `Driver` trait is synchronous but JavaScript storage is async,
/// we buffer requests and process them in batches during `tick()` calls.
#[derive(Default)]
pub struct PendingStorage {
    /// Requests waiting to be sent to JavaScript driver.
    pub pending_requests: Vec<groove::storage::StorageRequest>,
    /// Responses received from JavaScript driver.
    pub pending_responses: Vec<groove::storage::StorageResponse>,
}

impl PendingStorage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue requests for async processing.
    pub fn queue_requests(&mut self, requests: Vec<groove::storage::StorageRequest>) {
        self.pending_requests.extend(requests);
    }

    /// Take any pending responses.
    pub fn take_responses(&mut self) -> Vec<groove::storage::StorageResponse> {
        std::mem::take(&mut self.pending_responses)
    }

    /// Check if there are pending requests.
    pub fn has_pending_requests(&self) -> bool {
        !self.pending_requests.is_empty()
    }

    /// Take pending requests for processing.
    pub fn take_requests(&mut self) -> Vec<groove::storage::StorageRequest> {
        std::mem::take(&mut self.pending_requests)
    }

    /// Store responses after async processing.
    pub fn store_responses(&mut self, responses: Vec<groove::storage::StorageResponse>) {
        self.pending_responses.extend(responses);
    }
}
