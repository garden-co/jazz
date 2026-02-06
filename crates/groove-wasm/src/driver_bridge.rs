//! Bridge between Rust and JavaScript for sync message transport.
//!
//! Storage is now synchronous (in-memory via MemoryStorage).
//! This module is retained for the JsStorageDriver extern type
//! which may be used by downstream consumers, but the async
//! storage bridge is no longer needed.

use wasm_bindgen::prelude::*;

// ============================================================================
// JavaScript Driver Interface (retained for API compatibility)
// ============================================================================

/// External JavaScript interface for a storage driver.
///
/// No longer used for core storage (which is synchronous in-memory),
/// but retained for potential external consumers.
#[wasm_bindgen]
extern "C" {
    /// JavaScript storage driver interface.
    pub type JsStorageDriver;

    /// Process a batch of storage requests.
    #[wasm_bindgen(structural, method, catch)]
    pub async fn process(this: &JsStorageDriver, requests: JsValue) -> Result<JsValue, JsValue>;
}
