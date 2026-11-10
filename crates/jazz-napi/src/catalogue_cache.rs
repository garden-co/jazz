//! Trusted app metadata cache at the owner-open boundary; no row evidence.
use super::*;

#[napi(object)]
pub struct NativeCatalogueState {
    pub capture: Option<Uint8Array>,
    pub ready: bool,
}

#[napi]
impl NapiDb {
    #[napi(js_name = "hasAuthenticatedCatalogue")]
    pub fn has_authenticated_catalogue(&self) -> js::Result<bool> {
        Ok(self
            .binding()?
            .db
            .has_authenticated_catalogue()
            .map_err(|e| host_error(e.into()))?)
    }

    #[napi(js_name = "takeAuthenticatedCatalogueState")]
    pub fn take_authenticated_catalogue_state(&self) -> js::Result<NativeCatalogueState> {
        let state = self
            .binding()?
            .db
            .take_authenticated_catalogue_state()
            .map_err(|e| host_error(e.into()))?;
        Ok(NativeCatalogueState {
            capture: state.capture.map(Uint8Array::new),
            ready: state.ready,
        })
    }

    #[napi(js_name = "validateCatalogueCaptureReplacement")]
    pub fn validate_catalogue_capture_replacement(
        &self,
        previous: Uint8Array,
        next: Uint8Array,
    ) -> js::Result<()> {
        self.binding()?
            .db
            .has_authenticated_catalogue()
            .map_err(|e| host_error(e.into()))?;
        jazz::db::validate_catalogue_capture_replacement(&previous, &next)
            .map_err(|e| host_error(e.into()))?;
        Ok(())
    }

    /// Trusted same-app host cache, restored before replica/session admission.
    #[napi(factory, js_name = "openMemoryWithCatalogue")]
    pub fn open_memory_with_catalogue(
        schema: Uint8Array,
        config: Uint8Array,
        capture: Uint8Array,
    ) -> js::Result<Self> {
        Ok(Self::open_as_with_catalogue(
            &schema,
            &config,
            None,
            OpenAs::Untrusted,
            Some(&capture),
        )?)
    }

    #[napi(factory, js_name = "openMemoryWithSelfSignedProofAndCatalogue")]
    pub fn open_memory_with_self_signed_proof_and_catalogue(
        schema: Uint8Array,
        config: Uint8Array,
        token: String,
        app_id: String,
        claimed_author: String,
        capture: Uint8Array,
    ) -> js::Result<Self> {
        let how = OpenAs::SelfSigned {
            token: &token,
            app_id: &app_id,
            claimed_author: &claimed_author,
        };
        Ok(Self::open_as_with_catalogue(
            &schema,
            &config,
            None,
            how,
            Some(&capture),
        )?)
    }

    #[napi(factory, js_name = "openPersistentWithCatalogue")]
    pub fn open_persistent_with_catalogue(
        data_path: String,
        schema: Uint8Array,
        config: Uint8Array,
        capture: Uint8Array,
    ) -> js::Result<Self> {
        Ok(Self::open_as_with_catalogue(
            &schema,
            &config,
            Some(&data_path),
            OpenAs::Untrusted,
            Some(&capture),
        )?)
    }

    #[napi(factory, js_name = "openPersistentWithSelfSignedProofAndCatalogue")]
    pub fn open_persistent_with_self_signed_proof_and_catalogue(
        data_path: String,
        schema: Uint8Array,
        config: Uint8Array,
        token: String,
        app_id: String,
        claimed_author: String,
        capture: Uint8Array,
    ) -> js::Result<Self> {
        let how = OpenAs::SelfSigned {
            token: &token,
            app_id: &app_id,
            claimed_author: &claimed_author,
        };
        Ok(Self::open_as_with_catalogue(
            &schema,
            &config,
            Some(&data_path),
            how,
            Some(&capture),
        )?)
    }
}
