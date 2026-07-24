//! Bindings for the tab-side browser-broker client state machine.
//!
//! The sans-IO core lives in the `jazz-browser-broker` crate; this wrapper is
//! deliberately dumb: JSON-safe events in, JSON-safe commands out. The
//! TypeScript shell (`browser-broker-client.ts`) owns the SharedWorker, all
//! timers, promises, and MessagePorts.

use jazz_browser_broker::{TabClientCore, TabClientEvent, TabClientOptions};
use serde::Serialize;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct WasmTabBrokerCore {
    inner: TabClientCore,
}

#[wasm_bindgen]
impl WasmTabBrokerCore {
    #[wasm_bindgen(constructor)]
    pub fn new(options: JsValue) -> Result<WasmTabBrokerCore, JsError> {
        let options: TabClientOptions = serde_wasm_bindgen::from_value(options)
            .map_err(|error| JsError::new(&error.to_string()))?;
        Ok(WasmTabBrokerCore {
            inner: TabClientCore::new(options),
        })
    }

    pub fn handle(&mut self, event: JsValue) -> Result<JsValue, JsError> {
        let event: TabClientEvent = serde_wasm_bindgen::from_value(event)
            .map_err(|error| JsError::new(&error.to_string()))?;
        self.inner
            .handle(event)
            .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
            .map_err(|error| JsError::new(&error.to_string()))
    }

    pub fn snapshot(&self) -> Result<JsValue, JsError> {
        self.inner
            .snapshot()
            .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
            .map_err(|error| JsError::new(&error.to_string()))
    }
}
