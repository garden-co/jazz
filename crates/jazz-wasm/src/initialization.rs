//! Authenticated catalogue capture is implemented by SQLite. Initialization
//! transaction seals and journals remain separate, explicitly unsupported contracts.
use super::*;

#[wasm_bindgen]
impl WasmDb {
    #[wasm_bindgen(js_name = validateCatalogueCaptureReplacement)]
    pub fn validate_catalogue_capture_replacement(
        &self,
        previous: Vec<u8>,
        next: Vec<u8>,
    ) -> Result<(), JsValue> {
        self.open_inner()?;
        jazz::db::validate_catalogue_capture_replacement(&previous, &next).map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = hasAuthenticatedCatalogue)]
    pub fn has_authenticated_catalogue(&self) -> Result<js_sys::Promise, JsValue> {
        let ready = self
            .binding()?
            .db
            .has_authenticated_catalogue()
            .map_err(to_js_error)?;
        Ok(js_sys::Promise::resolve(&JsValue::from_bool(ready)))
    }

    #[wasm_bindgen(js_name = takeAuthenticatedCatalogueState)]
    pub fn take_authenticated_catalogue_state(&self) -> Result<js_sys::Promise, JsValue> {
        let result = js_sys::Object::new();
        let state = self
            .binding()?
            .db
            .take_authenticated_catalogue_state()
            .map_err(to_js_error)?;
        js_sys::Reflect::set(&result, &"ready".into(), &state.ready.into())?;
        if let Some(capture) = state.capture {
            js_sys::Reflect::set(&result, &"capture".into(), &bytes_to_js(capture)?)?;
        }
        let result: JsValue = result.into();
        Ok(js_sys::Promise::resolve(&result))
    }

    #[wasm_bindgen(js_name = sealInitializationTransaction)]
    pub fn seal_initialization_transaction(
        &self,
        _open_id: String,
    ) -> Result<js_sys::Promise, JsValue> {
        self.open_inner()?;
        Err(not_supported(
            "E2EE offline initialization: sealInitializationTransaction",
        ))
    }

    #[wasm_bindgen(js_name = publishInitializationTransaction)]
    pub fn publish_initialization_transaction(
        &self,
        _token: String,
    ) -> Result<js_sys::Promise, JsValue> {
        self.open_inner()?;
        Err(not_supported(
            "E2EE offline initialization: publishInitializationTransaction",
        ))
    }

    #[wasm_bindgen(js_name = cancelInitializationTransaction)]
    pub fn cancel_initialization_transaction(
        &self,
        _token: String,
    ) -> Result<js_sys::Promise, JsValue> {
        self.open_inner()?;
        Err(not_supported(
            "E2EE offline initialization: cancelInitializationTransaction",
        ))
    }

    #[wasm_bindgen(js_name = recordInitializationInsertAbsence)]
    pub fn record_initialization_insert_absence(
        &self,
        _open_id: String,
        _table: String,
        _row_id: Vec<u8>,
    ) -> Result<js_sys::Promise, JsValue> {
        self.open_inner()?;
        Err(not_supported(
            "E2EE offline initialization: recordInitializationInsertAbsence",
        ))
    }

    #[wasm_bindgen(js_name = initializationTransactionStatus)]
    pub fn initialization_transaction_status(
        &self,
        _ids: Vec<String>,
    ) -> Result<js_sys::Promise, JsValue> {
        self.open_inner()?;
        Err(not_supported(
            "E2EE offline initialization: initializationTransactionStatus",
        ))
    }
}
