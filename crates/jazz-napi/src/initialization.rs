use super::*;
use jazz::db::{InitializationSeal, ReservedTxId};

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
        // The receiver owns the core seal until it is delivered to the caller.
        // Dropping it also drops an undelivered seal, retiring its reservation.
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
    fn take_initialization_seal(&self, token: &str) -> napi::Result<InitializationSeal> {
        self.streaming.ensure_admitted(self.view_id)?;
        self.initialization_seals
            .borrow_mut()
            .remove(token)
            .ok_or_else(|| {
                napi::Error::from_reason(
                    "initialization seal is unknown, consumed, or belongs to another runtime",
                )
            })
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
                        .await
                        .map_err(napi_error)?;
                    Ok(NativeCatalogueState {
                        capture: state.capture.map(Uint8Array::new),
                        ready: state.ready,
                    })
                }) as LocalBoxFuture<'static, napi::Result<NativeCatalogueState>>
            }};
        }
        let future = match inner {
            NapiDbInnerStorage::Memory(db) => observe!(db),
            NapiDbInnerStorage::Persistent(db) => observe!(db),
        };
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
        let now_ms = commit_timestamp_ms()?;
        macro_rules! seal {
            ($db:expr, $drive:expr) => {{
                let db = Rc::clone($db);
                let owner = Rc::clone(&db);
                let receiver = db.enqueue_transaction_read(open_id, async move {
                    owner
                        .seal_initialization_transaction_at_ms(open_id, now_ms)
                        .await
                });
                if $drive {
                    db.drive_queued_mutation_once();
                }
                let lifecycle = Rc::clone(&self.streaming);
                let view_id = self.view_id;
                let cleanup: Box<dyn FnOnce()> = Box::new(move || {
                    // Cancellation before execution has no core seal to drop.
                    // Queue abandonment after the already-admitted begin/staging.
                    if lifecycle.ensure_admitted(view_id).is_ok() {
                        db.enqueue_abandon_transaction_handle(open_id);
                    }
                });
                (receiver, cleanup)
            }};
        }
        let (receiver, cleanup) = match inner {
            NapiDbInnerStorage::Memory(db) => seal!(db, true),
            NapiDbInnerStorage::Persistent(db) => seal!(db, false),
        };
        let seals = Rc::clone(&self.initialization_seals);
        let streaming = Rc::clone(&self.streaming);
        let view_id = self.view_id;
        let pending = PendingNativeInitializationSeal {
            future: RefCell::new(Some(Box::pin(async move {
                let seal = receiver
                    .await
                    .map_err(|_| {
                        napi::Error::from_reason(
                            "initialization seal owner operation was cancelled",
                        )
                    })?
                    .map_err(napi_error)?;
                streaming.ensure_admitted(view_id)?;
                let reserved_tx_id = seal.reservation().encode();
                let token = CoreOpenTransactionId::new().to_string();
                seals.borrow_mut().insert(token.clone(), seal);
                Ok(NativeInitializationSeal {
                    token,
                    reserved_tx_id,
                })
            }))),
            cleanup: RefCell::new(Some(cleanup)),
        };
        match pending.poll_once()? {
            Some(seal) => Ok(Either::A(seal)),
            None => Ok(Either::B(pending)),
        }
    }

    #[napi(js_name = "publishInitializationTransaction")]
    pub fn publish_initialization_transaction(&self, token: String) -> js::Result<Write> {
        let seal = self.take_initialization_seal(&token)?;
        let inner = self.inner.borrow();
        let inner = inner
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match inner {
            NapiDbInnerStorage::Memory(db) => {
                let write = core_block_on(db.publish_initialization_transaction(seal))
                    .map_err(napi_error)?;
                db.drive_queued_mutation_once();
                core_write_memory(Rc::clone(db), write).map_err(BindingError::from)
            }
            NapiDbInnerStorage::Persistent(db) => {
                let write = core_block_on(db.publish_initialization_transaction(seal))
                    .map_err(napi_error)?;
                core_write_persistent(Rc::clone(db), write).map_err(BindingError::from)
            }
        }
    }

    #[napi(js_name = "cancelInitializationTransaction")]
    pub fn cancel_initialization_transaction(&self, token: String) -> js::Result<()> {
        let seal = self.take_initialization_seal(&token)?;
        let inner = self.inner.borrow();
        let inner = inner
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match inner {
            NapiDbInnerStorage::Memory(db) => {
                core_block_on(db.cancel_initialization_transaction(seal))
            }
            NapiDbInnerStorage::Persistent(db) => {
                core_block_on(db.cancel_initialization_transaction(seal))
            }
        }
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
        macro_rules! prepare {
            ($db:expr, $drive:expr) => {{
                let db = Rc::clone($db);
                let owner = Rc::clone(&db);
                let receiver = db.enqueue_transaction_read(open_id, async move {
                    owner
                        .prepare_initialization_insert(open_id, &table, row_id)
                        .await
                });
                if $drive {
                    db.drive_queued_mutation_once();
                }
                native_read_or_pending(Box::pin(async move {
                    receiver
                        .await
                        .map_err(|_| {
                            napi::Error::from_reason(
                                "initialization absence owner operation was cancelled",
                            )
                        })?
                        .map_err(napi_error)?;
                    Ok(Uint8Array::new(Vec::new()))
                }))
            }};
        }
        match inner {
            NapiDbInnerStorage::Memory(db) => prepare!(db, true),
            NapiDbInnerStorage::Persistent(db) => prepare!(db, false),
        }
        .map_err(BindingError::from)
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
        let ids = ids
            .iter()
            .map(|id| ReservedTxId::decode(id))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(napi_error)?;
        let inner = self.inner.borrow();
        let inner = inner
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        macro_rules! status {
            ($db:expr) => {{
                let db = Rc::clone($db);
                let lifecycle = Rc::clone(&self.streaming);
                let view_id = self.view_id;
                native_permission_advice_or_pending(Box::pin(async move {
                    let statuses = db
                        .initialization_transaction_status(&ids)
                        .await
                        .map_err(napi_error)?;
                    lifecycle.ensure_admitted(view_id)?;
                    jazz::binding_codec::encode_initialization_statuses(&ids, &statuses)
                        .map_err(napi_error)
                }))
            }};
        }
        match inner {
            NapiDbInnerStorage::Memory(db) => status!(db),
            NapiDbInnerStorage::Persistent(db) => status!(db),
        }
        .map_err(BindingError::from)
    }
}

pub(super) fn open_cached_db<S>(
    schema: JazzSchema,
    storage: S,
    config: CoreOpenDbConfig,
    identity: CoreDbIdentity,
    cached: Option<&[u8]>,
) -> napi::Result<CoreDb>
where
    S: CoreOrderedKvStorage + CoreReopenableStorage + 'static,
{
    let Some(cached) = cached else {
        return open_core_db(schema, storage, config, identity, false);
    };
    if config.history_complete {
        return Err(napi::Error::from_reason(
            "cached catalogue cannot establish history completeness",
        ));
    }
    let mut db_config = CoreDbConfig::new(schema, storage, identity);
    if let Some(seed) = config.row_id_seed {
        db_config = db_config.with_id_source(CoreSeededRowIdSource::new(seed));
    }
    // SAFETY: the private runtime host validates registry/application/environment
    // scope and capture provenance before passing these opaque cache bytes.
    let db = core_block_on(unsafe { CoreDb::open_with_cached_catalogue(db_config, Some(cached)) })
        .map_err(napi_error)?;
    configure_initial_sync_flush_cadence(&db, config.initial_sync_flush_every)
        .map_err(napi_error)?;
    Ok(db)
}
