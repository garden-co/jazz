//! Account catalogue observations and owner-bound initialization use the shared
//! Binding registry; Node only supplies pollable results and close admission.
use super::*;

#[napi(object)]
pub struct NativeInitializationSeal {
    pub token: String,
    pub reserved_tx_id: String,
}

#[napi(object)]
pub struct NativeCatalogueState {
    pub capture: Option<Uint8Array>,
    pub ready: bool,
}

#[napi]
pub struct PendingNativeCatalogueState {
    future: RefCell<Option<HostFuture<NativeCatalogueState>>>,
    inner: Inner,
}

#[napi]
impl PendingNativeCatalogueState {
    #[napi]
    pub fn poll(&self) -> js::Result<Option<NativeCatalogueState>> {
        if self.inner.host.closed.get() || self.inner.host.closing.get() || !self.inner.live.get() {
            self.cancel();
            return Err(closed().into());
        }
        let Some(mut future) = self.future.borrow_mut().take() else {
            return Err(napi::Error::from_reason(
                "native catalogue observation is complete or cancelled",
            )
            .into());
        };
        match future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(result) => Ok(Some(result.map_err(host_error)?)),
            Poll::Pending => {
                *self.future.borrow_mut() = Some(future);
                Ok(None)
            }
        }
    }
    #[napi]
    pub fn cancel(&self) {
        self.future.borrow_mut().take();
    }
}

#[napi]
pub struct PendingNativeInitializationSeal {
    future: lifecycle::Resource<HostFuture<NativeInitializationSeal>>,
    inner: Inner,
}

impl PendingNativeInitializationSeal {
    fn poll_once(&self) -> napi::Result<Option<NativeInitializationSeal>> {
        if self.inner.host.closed.get() || self.inner.host.closing.get() || !self.inner.live.get() {
            self.cancel();
            return Err(closed());
        }
        let Some(mut future) = self.future.borrow_mut().take() else {
            return Err(napi::Error::from_reason(
                "native initialization seal is complete or cancelled",
            ));
        };
        match future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(result) => match result {
                Ok(seal) => {
                    self.future.complete();
                    Ok(Some(seal))
                }
                Err(error) => {
                    self.cancel();
                    Err(host_error(error))
                }
            },
            Poll::Pending => {
                *self.future.borrow_mut() = Some(future);
                Ok(None)
            }
        }
    }
}

#[napi]
impl PendingNativeInitializationSeal {
    #[napi]
    pub fn poll(&self) -> js::Result<Option<NativeInitializationSeal>> {
        Ok(self.poll_once()?)
    }
    #[napi]
    pub fn cancel(&self) {
        self.future.cancel();
    }
}
impl Drop for PendingNativeInitializationSeal {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[napi]
impl NapiDb {
    #[napi(js_name = "validateCatalogueCaptureReplacement")]
    pub fn validate_catalogue_capture_replacement(
        &self,
        previous: Uint8Array,
        next: Uint8Array,
    ) -> js::Result<()> {
        self.open_inner()?;
        jazz::db::validate_catalogue_capture_replacement(&previous, &next)
            .map_err(|error| napi_core_error(&error))?;
        Ok(())
    }
    #[napi(js_name = "mergeCatalogueCaptures")]
    pub fn merge_catalogue_captures(
        &self,
        previous: Uint8Array,
        next: Uint8Array,
    ) -> js::Result<Uint8Array> {
        self.open_inner()?;
        Ok(jazz::db::merge_catalogue_captures(&previous, &next)
            .map_err(|error| napi_core_error(&error))?
            .into())
    }
    #[napi(js_name = "hasAuthenticatedCatalogue")]
    pub fn has_authenticated_catalogue(
        &self,
    ) -> js::Result<Either<bool, PendingNativeCatalogueState>> {
        Ok(Either::A(
            self.binding()?
                .db
                .has_authenticated_catalogue()
                .map_err(|error| napi_core_error(&error))?,
        ))
    }
    #[napi(js_name = "takeAuthenticatedCatalogueState")]
    pub fn take_authenticated_catalogue_state(
        &self,
    ) -> js::Result<Either<NativeCatalogueState, PendingNativeCatalogueState>> {
        if !self.owns_runtime {
            return Err(napi::Error::from_reason(
                "only the runtime owner can take authenticated catalogue state",
            )
            .into());
        }
        let state = self
            .binding()?
            .db
            .take_authenticated_catalogue_state()
            .map_err(|error| napi_core_error(&error))?;
        Ok(Either::A(NativeCatalogueState {
            capture: state.capture.map(Uint8Array::new),
            ready: state.ready,
        }))
    }
    #[napi(js_name = "sealInitializationTransaction")]
    pub fn seal_initialization_transaction(
        &self,
        open_id: String,
    ) -> js::Result<Either<NativeInitializationSeal, PendingNativeInitializationSeal>> {
        let inner = self.open_inner()?;
        let rollback = inner.binding.clone();
        let rollback_id = open_id.clone();
        let binding = inner.binding.clone();
        let live = inner.live.clone();
        let host = inner.host.clone();
        let view = inner.view.clone();
        let future: HostFuture<NativeInitializationSeal> = Box::pin(async move {
            let seal = if binding.db.is_non_durable_client() {
                binding
                    .seal_initialization_transaction_with_owner(&open_id)
                    .await
            } else {
                binding.seal_initialization_transaction(&open_id)
            }?;
            if host.closed.get() || host.closing.get() || !live.get() {
                let _ = binding.cancel_initialization_transaction(&seal.token);
                return Err(HostError::Argument("database is closed".into()));
            }
            binding.check_initialization_token(&seal.token)?;
            view.track_initialization(&seal.token, &binding);
            Ok(NativeInitializationSeal {
                token: seal.token,
                reserved_tx_id: seal.reserved_tx_id,
            })
        });
        let pending = PendingNativeInitializationSeal {
            inner: inner.clone(),
            future: lifecycle::Resource::new(Some(&inner.view), future).on_cancel(move || {
                let _ = rollback.rollback_transaction(&rollback_id);
            }),
        };
        // Preserve the persistent owner's cancellation turn before sealing.
        if inner.host.file_lock.borrow().is_some() {
            return Ok(Either::B(pending));
        }
        Ok(match pending.poll_once()? {
            Some(seal) => Either::A(seal),
            None => Either::B(pending),
        })
    }
    #[napi(js_name = "publishInitializationTransaction")]
    pub fn publish_initialization_transaction(&self, env: Env, token: String) -> js::Result<Write> {
        let inner = self.open_inner()?;
        let result = inner.binding.publish_initialization_transaction(&token);
        if inner.binding.check_initialization_token(&token).is_err() {
            inner.view.forget_initialization(&token);
        }
        Ok(inner.write(&env, result)?)
    }
    #[napi(js_name = "cancelInitializationTransaction")]
    pub fn cancel_initialization_transaction(&self, token: String) -> js::Result<()> {
        let inner = self.open_inner()?;
        let result = inner.binding.cancel_initialization_transaction(&token);
        if inner.binding.check_initialization_token(&token).is_err() {
            inner.view.forget_initialization(&token);
        }
        result.map_err(host_error)?;
        Ok(())
    }
    #[napi(js_name = "recordInitializationInsertAbsence")]
    pub fn record_initialization_insert_absence(
        &self,
        open_id: String,
        table: String,
        row_id: Uint8Array,
    ) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
        self.binding()?
            .record_initialization_insert_absence(&open_id, &table, &row_id)
            .map_err(host_error)?;
        Ok(Either::A(Vec::new().into()))
    }
    #[napi(js_name = "initializationTransactionStatus")]
    pub fn initialization_transaction_status(
        &self,
        ids: Vec<String>,
    ) -> js::Result<Either<String, PendingNativePermissionAdvice>> {
        Ok(Either::A(
            self.binding()?
                .initialization_transaction_status(&ids)
                .map_err(host_error)?,
        ))
    }
}
