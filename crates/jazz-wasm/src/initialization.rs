//! E14 keeps its public binding surface while its authenticated catalogue and
//! initialization journal contracts await a SQLite implementation. Never treat
//! an ordinary schema open or commit as proof of authenticated initialization.
use super::*;

#[wasm_bindgen]
impl WasmDb {
    #[wasm_bindgen(js_name = validateCatalogueCaptureReplacement)]
    pub fn validate_catalogue_capture_replacement(
        &self,
        _previous: Vec<u8>,
        _next: Vec<u8>,
    ) -> Result<(), JsValue> {
        self.open_inner()?;
        Err(not_supported(
            "E2EE offline initialization: validateCatalogueCaptureReplacement",
        ))
    }

    #[wasm_bindgen(js_name = hasAuthenticatedCatalogue)]
    pub fn has_authenticated_catalogue(&self) -> Result<js_sys::Promise, JsValue> {
        self.open_inner()?;
        Err(not_supported(
            "E2EE offline initialization: hasAuthenticatedCatalogue",
        ))
    }

    #[wasm_bindgen(js_name = takeAuthenticatedCatalogueState)]
    pub fn take_authenticated_catalogue_state(&self) -> Result<js_sys::Promise, JsValue> {
        self.open_inner()?;
        Err(not_supported(
            "E2EE offline initialization: takeAuthenticatedCatalogueState",
        ))
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
