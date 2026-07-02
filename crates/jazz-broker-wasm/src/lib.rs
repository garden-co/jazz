#![forbid(unsafe_code)]

use jazz_browser_broker::{BrokerCore, BrokerEvent};
use serde::Serialize;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct WasmBrokerCore {
    inner: BrokerCore,
}

#[wasm_bindgen]
impl WasmBrokerCore {
    #[wasm_bindgen(constructor)]
    pub fn new(broker_instance_id: String) -> WasmBrokerCore {
        WasmBrokerCore {
            inner: BrokerCore::new(broker_instance_id),
        }
    }

    pub fn handle(&mut self, event: JsValue, now_ms: f64) -> Result<JsValue, JsError> {
        let event: BrokerEvent = serde_wasm_bindgen::from_value(event)
            .map_err(|error| JsError::new(&error.to_string()))?;
        let now_ms = if now_ms.is_finite() {
            now_ms.floor() as i64
        } else {
            0
        };
        let commands = self.inner.handle(event, now_ms);
        commands
            .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
            .map_err(|error| JsError::new(&error.to_string()))
    }
}
