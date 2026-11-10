//! Trusted app metadata cache at the owner-open boundary; no row evidence.
use super::*;

#[wasm_bindgen]
impl WasmDb {
    #[wasm_bindgen(js_name = hasAuthenticatedCatalogue)]
    pub fn has_authenticated_catalogue(&self) -> Result<bool, JsValue> {
        self.binding()?
            .db
            .has_authenticated_catalogue()
            .map_err(|e| host_error(e.into()))
    }

    #[wasm_bindgen(js_name = takeAuthenticatedCatalogueState)]
    pub fn take_authenticated_catalogue_state(&self) -> Result<JsValue, JsValue> {
        let state = self
            .binding()?
            .db
            .take_authenticated_catalogue_state()
            .map_err(|e| host_error(e.into()))?;
        let result = js_sys::Object::new();
        js_sys::Reflect::set(&result, &"ready".into(), &state.ready.into())?;
        if let Some(capture) = state.capture {
            js_sys::Reflect::set(&result, &"capture".into(), &bytes_to_js(capture)?)?;
        }
        Ok(result.into())
    }

    #[wasm_bindgen(js_name = validateCatalogueCaptureReplacement)]
    pub fn validate_catalogue_capture_replacement(
        &self,
        previous: Vec<u8>,
        next: Vec<u8>,
    ) -> Result<(), JsValue> {
        self.binding()?
            .db
            .has_authenticated_catalogue()
            .map_err(|e| host_error(e.into()))?;
        jazz::db::validate_catalogue_capture_replacement(&previous, &next)
            .map_err(|e| host_error(e.into()))
    }

    /// Trusted same-app host cache, restored before replica/session admission.
    #[wasm_bindgen(js_name = openMemoryWithCatalogue)]
    pub fn open_memory_with_catalogue(
        schema: Vec<u8>,
        config: Vec<u8>,
        capture: Vec<u8>,
    ) -> Result<WasmDb, JsValue> {
        Self::open_memory_as_with_catalogue(&schema, &config, OpenAs::Untrusted, Some(&capture))
    }

    #[wasm_bindgen(js_name = openMemoryWithSelfSignedProofAndCatalogue)]
    pub fn open_memory_with_self_signed_proof_and_catalogue(
        schema: Vec<u8>,
        config: Vec<u8>,
        token: String,
        app_id: String,
        claimed_author: String,
        capture: Vec<u8>,
    ) -> Result<WasmDb, JsValue> {
        let how = OpenAs::SelfSigned {
            token: &token,
            app_id: &app_id,
            claimed_author: &claimed_author,
        };
        Self::open_memory_as_with_catalogue(&schema, &config, how, Some(&capture))
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = openBrowserWithCatalogue)]
    pub async fn open_browser_with_catalogue(
        storage: JsValue,
        schema: Vec<u8>,
        config: Vec<u8>,
        database: String,
        capture: Vec<u8>,
    ) -> Result<WasmDb, JsValue> {
        Self::open_browser_as_with_catalogue(
            storage,
            schema,
            config,
            database,
            OpenAs::Untrusted,
            Some(capture),
        )
        .await
    }

    #[cfg(target_arch = "wasm32")]
    // Match the existing explicit proof ABI and keep the cache argument separate.
    #[allow(clippy::too_many_arguments)]
    #[wasm_bindgen(js_name = openBrowserWithSelfSignedProofAndCatalogue)]
    pub async fn open_browser_with_self_signed_proof_and_catalogue(
        storage: JsValue,
        schema: Vec<u8>,
        config: Vec<u8>,
        token: String,
        app_id: String,
        claimed_author: String,
        database: String,
        capture: Vec<u8>,
    ) -> Result<WasmDb, JsValue> {
        let how = OpenAs::SelfSigned {
            token: &token,
            app_id: &app_id,
            claimed_author: &claimed_author,
        };
        Self::open_browser_as_with_catalogue(storage, schema, config, database, how, Some(capture))
            .await
    }
}
