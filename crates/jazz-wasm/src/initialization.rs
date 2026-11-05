//! Authenticated catalogue capture and owner-bound initialization on SQLite.
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

    #[wasm_bindgen(js_name = mergeCatalogueCaptures)]
    pub fn merge_catalogue_captures(
        &self,
        previous: Vec<u8>,
        next: Vec<u8>,
    ) -> Result<Vec<u8>, JsValue> {
        self.open_inner()?;
        jazz::db::merge_catalogue_captures(&previous, &next).map_err(to_js_error)
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
        open_id: String,
    ) -> Result<js_sys::Promise, JsValue> {
        let inner = self.open_inner()?;
        if inner.binding.db.is_non_durable_client() {
            return Ok(future_to_promise(async move {
                let seal = inner
                    .binding
                    .seal_initialization_transaction_with_owner(&open_id)
                    .await
                    .map_err(host_error)?;
                inner
                    .binding
                    .check_initialization_token(&seal.token)
                    .map_err(host_error)?;
                match serde_wasm_bindgen::to_value(&seal) {
                    Ok(value) => Ok(value),
                    Err(error) => {
                        let _ = inner.binding.cancel_initialization_transaction(&seal.token);
                        Err(js_sys::Error::new(&error.to_string()).into())
                    }
                }
            }));
        }
        let seal = inner
            .binding
            .seal_initialization_transaction(&open_id)
            .map_err(host_error)?;
        let through = inner.binding.db.seq_high_water();
        Ok(future_to_promise(async move {
            if let Err(error) = inner.host.flush(through).await {
                let _ = inner.binding.cancel_initialization_transaction(&seal.token);
                return Err(error);
            }
            inner
                .binding
                .check_initialization_token(&seal.token)
                .map_err(host_error)?;
            match serde_wasm_bindgen::to_value(&seal) {
                Ok(value) => Ok(value),
                Err(error) => {
                    let _ = inner.binding.cancel_initialization_transaction(&seal.token);
                    Err(js_sys::Error::new(&error.to_string()).into())
                }
            }
        }))
    }

    #[wasm_bindgen(js_name = publishInitializationTransaction)]
    pub fn publish_initialization_transaction(
        &self,
        token: String,
    ) -> Result<js_sys::Promise, JsValue> {
        let inner = self.open_inner()?;
        let write = inner.write(inner.binding.publish_initialization_transaction(&token))?;
        Ok(js_sys::Promise::resolve(&JsValue::from(write)))
    }

    #[wasm_bindgen(js_name = cancelInitializationTransaction)]
    pub fn cancel_initialization_transaction(
        &self,
        token: String,
    ) -> Result<js_sys::Promise, JsValue> {
        self.binding()?
            .cancel_initialization_transaction(&token)
            .map_err(host_error)?;
        Ok(js_sys::Promise::resolve(&JsValue::UNDEFINED))
    }

    #[wasm_bindgen(js_name = recordInitializationInsertAbsence)]
    pub fn record_initialization_insert_absence(
        &self,
        open_id: String,
        table: String,
        row_id: Vec<u8>,
    ) -> Result<js_sys::Promise, JsValue> {
        self.binding()?
            .record_initialization_insert_absence(&open_id, &table, &row_id)
            .map_err(host_error)?;
        Ok(js_sys::Promise::resolve(&JsValue::UNDEFINED))
    }

    #[wasm_bindgen(js_name = initializationTransactionStatus)]
    pub fn initialization_transaction_status(
        &self,
        ids: Vec<String>,
    ) -> Result<js_sys::Promise, JsValue> {
        let inner = self.open_inner()?;
        let statuses = inner
            .binding
            .initialization_transaction_status(&ids)
            .map_err(host_error)?;
        let through = inner.binding.db.seq_high_water();
        Ok(future_to_promise(async move {
            inner.host.flush(through).await?;
            Ok(JsValue::from_str(&statuses))
        }))
    }
}
