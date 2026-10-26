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

/// A single owner observation, retained on the JavaScript thread until ready.
#[napi]
pub struct PendingNativeCatalogueState {
    future: RefCell<Option<LocalBoxFuture<'static, napi::Result<NativeCatalogueState>>>>,
    lifecycle: Rc<StreamingOwnerLifecycle>,
    view_id: u64,
}

#[napi]
impl PendingNativeCatalogueState {
    #[napi]
    pub fn poll(&self) -> js::Result<Option<NativeCatalogueState>> {
        self.poll_once().map_err(BindingError::from)
    }

    fn poll_once(&self) -> napi::Result<Option<NativeCatalogueState>> {
        if let Err(error) = self.lifecycle.ensure_admitted(self.view_id) {
            self.cancel();
            return Err(error);
        }
        let Some(mut future) = self.future.borrow_mut().take() else {
            return Err(napi::Error::from_reason(
                "native catalogue observation is complete or cancelled",
            ));
        };
        let mut context = Context::from_waker(Waker::noop());
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => result.map(Some),
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

impl Drop for PendingNativeCatalogueState {
    fn drop(&mut self) {
        self.cancel();
    }
}

/// Thread-affine initialization preparation, driven by the owner's normal ticks.
#[napi]
pub struct PendingNativeInitializationSeal {
    future: RefCell<Option<LocalBoxFuture<'static, napi::Result<NativeInitializationSeal>>>>,
    cleanup: RefCell<Option<Box<dyn FnOnce()>>>,
}

#[napi]
impl PendingNativeInitializationSeal {
    #[napi]
    pub fn poll(&self) -> js::Result<Option<NativeInitializationSeal>> {
        self.poll_once().map_err(BindingError::from)
    }

    fn poll_once(&self) -> napi::Result<Option<NativeInitializationSeal>> {
        let Some(mut future) = self.future.borrow_mut().take() else {
            return Err(napi::Error::from_reason(
                "native initialization seal is complete or cancelled",
            ));
        };
        let mut context = Context::from_waker(Waker::noop());
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => {
                if result.is_err() {
                    self.cancel();
                } else {
                    self.cleanup.borrow_mut().take();
                }
                result.map(Some)
            }
            Poll::Pending => {
                *self.future.borrow_mut() = Some(future);
                Ok(None)
            }
        }
    }

    #[napi]
    pub fn cancel(&self) {
        // Dropping the pending owner operation retires any undelivered reservation.
        self.future.borrow_mut().take();
        if let Some(cleanup) = self.cleanup.borrow_mut().take() {
            cleanup();
        }
    }
}

impl Drop for PendingNativeInitializationSeal {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl NapiDb {
    fn take_initialization_token(&self, token: &str) -> napi::Result<()> {
        self.streaming.ensure_admitted(self.view_id)?;
        if !self.initialization_seals.borrow_mut().remove(token) {
            return Err(napi::Error::from_reason(
                "initialization seal is unknown, consumed, or belongs to another runtime",
            ));
        }
        Ok(())
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
        self.streaming.ensure_admitted(self.view_id)?;
        jazz::db::validate_catalogue_capture_replacement(&previous, &next)
            .map_err(napi_error)
            .map_err(BindingError::from)
    }

    #[napi(js_name = "mergeCatalogueCaptures")]
    pub fn merge_catalogue_captures(
        &self,
        previous: Uint8Array,
        next: Uint8Array,
    ) -> js::Result<Uint8Array> {
        self.streaming.ensure_admitted(self.view_id)?;
        jazz::db::merge_catalogue_captures(&previous, &next)
            .map(Uint8Array::new)
            .map_err(napi_error)
            .map_err(BindingError::from)
    }

    #[napi(js_name = "hasAuthenticatedCatalogue")]
    pub fn has_authenticated_catalogue(
        &self,
    ) -> js::Result<Either<bool, PendingNativeCatalogueState>> {
        self.streaming.ensure_admitted(self.view_id)?;
        let inner = self.inner.borrow();
        let inner = inner
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        macro_rules! observe {
            ($db:expr) => {{
                let db = Rc::clone($db);
                Box::pin(async move {
                    let ready = db.has_authenticated_catalogue().map_err(napi_error)?;
                    Ok(NativeCatalogueState {
                        capture: None,
                        ready,
                    })
                }) as LocalBoxFuture<'static, napi::Result<NativeCatalogueState>>
            }};
        }
        let future = observe!(&inner.core);
        let pending = PendingNativeCatalogueState {
            future: RefCell::new(Some(future)),
            lifecycle: Rc::clone(&self.streaming),
            view_id: self.view_id,
        };
        match pending.poll_once()? {
            Some(state) => Ok(Either::A(state.ready)),
            None => Ok(Either::B(pending)),
        }
    }

    #[napi(js_name = "takeAuthenticatedCatalogueState")]
    pub fn take_authenticated_catalogue_state(
        &self,
    ) -> js::Result<Either<NativeCatalogueState, PendingNativeCatalogueState>> {
        self.streaming.ensure_admitted(self.view_id)?;
        if !self.owns_runtime {
            return Err(napi::Error::from_reason(
                "only the runtime owner can take authenticated catalogue state",
            )
            .into());
        }
        let inner = self.inner.borrow();
        let inner = inner
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        macro_rules! observe {
            ($db:expr) => {{
                let db = Rc::clone($db);
                Box::pin(async move {
                    let state = db
                        .take_authenticated_catalogue_state()
                        .map_err(napi_error)?;
                    Ok(NativeCatalogueState {
                        capture: state.capture.map(Uint8Array::new),
                        ready: state.ready,
                    })
                }) as LocalBoxFuture<'static, napi::Result<NativeCatalogueState>>
            }};
        }
        let future = observe!(&inner.core);
        let pending = PendingNativeCatalogueState {
            future: RefCell::new(Some(future)),
            lifecycle: Rc::clone(&self.streaming),
            view_id: self.view_id,
        };
        match pending.poll_once()? {
            Some(state) => Ok(Either::A(state)),
            None => Ok(Either::B(pending)),
        }
    }

    #[napi(js_name = "sealInitializationTransaction")]
    pub fn seal_initialization_transaction(
        &self,
        open_id: String,
    ) -> js::Result<Either<NativeInitializationSeal, PendingNativeInitializationSeal>> {
        self.streaming.ensure_admitted(self.view_id)?;
        let open_id = open_id
            .parse::<CoreOpenTransactionId>()
            .map_err(napi::Error::from_reason)?;
        let inner = self.inner.borrow();
        let inner = inner
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let db = Rc::clone(&inner.core);
        let owner = Rc::clone(&db);
        let non_durable = self.non_durable_client.get();
        let cleanup: Box<dyn FnOnce()> = Box::new(move || {
            // The foreground reservation future retires its request on drop.
            // Abandon any batch which had not yet frozen when cancellation won.
            let _ = owner.binding.rollback_transaction(&open_id.to_string());
        });
        let seals = Rc::clone(&self.initialization_seals);
        let streaming = Rc::clone(&self.streaming);
        let view_id = self.view_id;
        let pending = PendingNativeInitializationSeal {
            future: RefCell::new(Some(Box::pin(async move {
                let id = open_id.to_string();
                let seal = if non_durable {
                    db.binding
                        .seal_initialization_transaction_with_owner(&id)
                        .await
                } else {
                    db.binding.seal_initialization_transaction(&id)
                }
                .map_err(napi_error)?;
                if let Err(error) = streaming.ensure_admitted(view_id) {
                    let _ = db.binding.cancel_initialization_transaction(&seal.token);
                    return Err(error);
                }
                seals.borrow_mut().insert(seal.token.clone());
                Ok(NativeInitializationSeal {
                    token: seal.token,
                    reserved_tx_id: seal.reserved_tx_id,
                })
            }))),
            cleanup: RefCell::new(Some(cleanup)),
        };
        // Keep the persistent owner's cancellation boundary even though SQLite
        // commits synchronously. Returning the pending operation lets its
        // caller cancel or close before the first storage turn; dropping it
        // rolls back the unpublished transaction through the cleanup guard.
        if inner.core.is_persistent() {
            return Ok(Either::B(pending));
        }
        match pending.poll_once()? {
            Some(seal) => Ok(Either::A(seal)),
            None => Ok(Either::B(pending)),
        }
    }

    #[napi(js_name = "publishInitializationTransaction")]
    pub fn publish_initialization_transaction(&self, token: String) -> js::Result<Write> {
        self.take_initialization_token(&token)?;
        let inner = self.inner.borrow();
        let inner = inner
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let write = inner
            .core
            .binding
            .publish_initialization_transaction(&token)
            .map_err(napi_error)?;
        Ok(host_write_with_kind(
            Rc::clone(&inner.core),
            write,
            "serializable",
        ))
    }

    #[napi(js_name = "cancelInitializationTransaction")]
    pub fn cancel_initialization_transaction(&self, token: String) -> js::Result<()> {
        self.take_initialization_token(&token)?;
        let inner = self.inner.borrow();
        let inner = inner
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        inner
            .core
            .binding
            .cancel_initialization_transaction(&token)
            .map_err(napi_error)
            .map_err(BindingError::from)
    }

    #[napi(js_name = "recordInitializationInsertAbsence")]
    pub fn record_initialization_insert_absence(
        &self,
        open_id: String,
        table: String,
        row_id: Uint8Array,
    ) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
        self.streaming.ensure_admitted(self.view_id)?;
        let open_id = open_id
            .parse::<CoreOpenTransactionId>()
            .map_err(napi::Error::from_reason)?;
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let inner = self.inner.borrow();
        let inner = inner
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        inner
            .core
            .binding
            .record_initialization_insert_absence(&open_id.to_string(), &table, &row_id.to_bytes())
            .map_err(napi_error)?;
        Ok(Either::A(Uint8Array::new(Vec::new())))
    }

    #[napi(js_name = "initializationTransactionStatus")]
    pub fn initialization_transaction_status(
        &self,
        ids: Vec<String>,
    ) -> js::Result<Either<String, PendingNativePermissionAdvice>> {
        self.streaming.ensure_admitted(self.view_id)?;
        if ids.len() > 64 {
            return Err(napi::Error::from_reason(
                "initialization status accepts at most 64 identities",
            )
            .into());
        }
        let inner = self.inner.borrow();
        let inner = inner
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let status = inner
            .core
            .binding
            .initialization_transaction_status(&ids)
            .map_err(napi_error)?;
        Ok(Either::A(status))
    }
}

pub(super) fn open_cached_db(
    schema: JazzSchema,
    storage: NativeStorage,
    config: CoreOpenDbConfig,
    identity: CoreDbIdentity,
    cached: Option<&[u8]>,
) -> napi::Result<CoreDb> {
    open_cached_db_inner(schema, storage, config, identity, cached, None)
}

pub(super) fn open_cached_account_owner_db(
    schema: JazzSchema,
    storage: NativeStorage,
    config: CoreOpenDbConfig,
    identity: CoreDbIdentity,
    cached: Option<&[u8]>,
    admission: &account_owner::AdmittedAccountOwner,
) -> napi::Result<CoreDb> {
    open_cached_db_inner(schema, storage, config, identity, cached, Some(admission))
}

fn open_cached_db_inner(
    schema: JazzSchema,
    storage: NativeStorage,
    config: CoreOpenDbConfig,
    identity: CoreDbIdentity,
    cached: Option<&[u8]>,
    admission: Option<&account_owner::AdmittedAccountOwner>,
) -> napi::Result<CoreDb> {
    if cached.is_some() && config.history_complete {
        return Err(napi::Error::from_reason(
            "cached catalogue cannot establish history completeness",
        ));
    }
    let db = match admission {
        Some(admission) => CoreDb::open_account_owner(schema, storage, config, identity, admission),
        None => open_core_db(schema, storage, config, identity, false),
    }?;
    if let Some(cached) = cached {
        // SAFETY: the host owns and validates this app/environment cache.
        unsafe { db.install_cached_catalogue(cached) }.map_err(napi_error)?;
    }
    Ok(db)
}
