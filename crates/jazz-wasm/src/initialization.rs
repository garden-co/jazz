use super::*;
use jazz::db::ReservedTxId;

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

    #[wasm_bindgen(js_name = takeAuthenticatedCatalogueState)]
    pub fn take_authenticated_catalogue_state(&self) -> Result<js_sys::Promise, JsValue> {
        let inner = self.open_inner()?;
        if !self.owns_runtime {
            return Err(JsValue::from_str(
                "only the runtime owner can take authenticated catalogue state",
            ));
        }
        let lifecycle = Rc::clone(&self.inner);
        Ok(future_to_promise(async move {
            let observation = async {
                match inner {
                    WasmDbInner::Memory(db) => db.take_authenticated_catalogue_state().await,
                    #[cfg(target_arch = "wasm32")]
                    WasmDbInner::Browser(db) => db.take_authenticated_catalogue_state().await,
                    WasmDbInner::Closed => unreachable!("open_inner rejects closed runtimes"),
                }
                .map_err(to_js_error)
            };
            let mut observation = std::pin::pin!(observation);
            let state = std::future::poll_fn(|context| {
                if lifecycle.borrow().is_none() {
                    return std::task::Poll::Ready(Err(JsValue::from_str("WasmDb is closed")));
                }
                observation.as_mut().poll(context)
            })
            .await?;
            let result = js_sys::Object::new();
            js_sys::Reflect::set(&result, &"ready".into(), &state.ready.into())?;
            if let Some(capture) = state.capture {
                let capture = js_sys::Uint8Array::from(capture.as_slice());
                js_sys::Reflect::set(&result, &"capture".into(), &capture)?;
            }
            Ok(result.into())
        }))
    }

    #[wasm_bindgen(js_name = sealInitializationTransaction)]
    pub fn seal_initialization_transaction(
        &self,
        open_id: String,
    ) -> Result<js_sys::Promise, JsValue> {
        let inner = self.open_inner()?;
        let lifecycle = Rc::clone(&self.inner);
        let seals = Rc::clone(&self.initialization_seals);
        let open_id = open_id
            .parse::<OpenTransactionId>()
            .map_err(|error| JsValue::from_str(&error))?;
        let now_ms = current_timestamp();
        Ok(future_to_promise(async move {
            let seal = match &inner {
                WasmDbInner::Memory(db) => {
                    db.seal_initialization_transaction_at_ms(open_id, now_ms)
                        .await
                }
                #[cfg(target_arch = "wasm32")]
                WasmDbInner::Browser(db) => {
                    db.seal_initialization_transaction_at_ms(open_id, now_ms)
                        .await
                }
                WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
            }
            .map_err(to_js_error)?;
            if lifecycle.borrow().is_none() {
                return Err(JsValue::from_str(
                    "WasmDb closed during initialization sealing",
                ));
            }
            let reserved = seal.reservation().encode();
            let token = OpenTransactionId::new().to_string();
            let result = js_sys::Object::new();
            js_sys::Reflect::set(&result, &"token".into(), &token.clone().into())?;
            js_sys::Reflect::set(&result, &"reservedTxId".into(), &reserved.into())?;
            seals.borrow_mut().insert(token, seal);
            Ok(result.into())
        }))
    }

    #[wasm_bindgen(js_name = publishInitializationTransaction)]
    pub fn publish_initialization_transaction(
        &self,
        token: String,
    ) -> Result<js_sys::Promise, JsValue> {
        let inner = self.open_inner()?;
        let seal = self
            .initialization_seals
            .borrow_mut()
            .remove(&token)
            .ok_or_else(|| {
                JsValue::from_str(
                    "initialization seal is unknown, consumed, or belongs to another runtime",
                )
            })?;
        Ok(future_to_promise(async move {
            let write = match inner {
                WasmDbInner::Memory(db) => {
                    let write = db
                        .publish_initialization_transaction(seal)
                        .await
                        .map_err(to_js_error)?;
                    db.drive_queued_mutation_once();
                    wasm_write_memory(db, write)?
                }
                #[cfg(target_arch = "wasm32")]
                WasmDbInner::Browser(db) => {
                    let write = db
                        .publish_initialization_transaction(seal)
                        .await
                        .map_err(to_js_error)?;
                    wasm_write_browser(db, write)?
                }
                WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
            };
            Ok(write.into())
        }))
    }

    #[wasm_bindgen(js_name = cancelInitializationTransaction)]
    pub fn cancel_initialization_transaction(
        &self,
        token: String,
    ) -> Result<js_sys::Promise, JsValue> {
        let inner = self.open_inner()?;
        let seal = self
            .initialization_seals
            .borrow_mut()
            .remove(&token)
            .ok_or_else(|| {
                JsValue::from_str(
                    "initialization seal is unknown, consumed, or belongs to another runtime",
                )
            })?;
        Ok(future_to_promise(async move {
            match inner {
                WasmDbInner::Memory(db) => db.cancel_initialization_transaction(seal).await,
                #[cfg(target_arch = "wasm32")]
                WasmDbInner::Browser(db) => db.cancel_initialization_transaction(seal).await,
                WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
            }
            .map_err(to_js_error)?;
            Ok(JsValue::UNDEFINED)
        }))
    }

    #[wasm_bindgen(js_name = recordInitializationInsertAbsence)]
    pub fn record_initialization_insert_absence(
        &self,
        open_id: String,
        table: String,
        row_id: Vec<u8>,
    ) -> Result<js_sys::Promise, JsValue> {
        let inner = self.open_inner()?;
        let open_id = open_id
            .parse::<OpenTransactionId>()
            .map_err(|error| JsValue::from_str(&error))?;
        let row_id = row_uuid_from_bytes(&row_id)?;
        Ok(future_to_promise(async move {
            match inner {
                WasmDbInner::Memory(db) => {
                    db.prepare_initialization_insert(open_id, &table, row_id)
                        .await
                }
                #[cfg(target_arch = "wasm32")]
                WasmDbInner::Browser(db) => {
                    db.prepare_initialization_insert(open_id, &table, row_id)
                        .await
                }
                WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
            }
            .map_err(to_js_error)?;
            Ok(JsValue::UNDEFINED)
        }))
    }

    #[wasm_bindgen(js_name = initializationTransactionStatus)]
    pub fn initialization_transaction_status(
        &self,
        ids: Vec<String>,
    ) -> Result<js_sys::Promise, JsValue> {
        let inner = self.open_inner()?;
        if ids.len() > 64 {
            return Err(JsValue::from_str(
                "initialization status accepts at most 64 identities",
            ));
        }
        let ids = ids
            .iter()
            .map(|id| ReservedTxId::decode(id))
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_js_error)?;
        Ok(future_to_promise(async move {
            let statuses = match inner {
                WasmDbInner::Memory(db) => db.initialization_transaction_status(&ids).await,
                #[cfg(target_arch = "wasm32")]
                WasmDbInner::Browser(db) => db.initialization_transaction_status(&ids).await,
                WasmDbInner::Closed => return Err(JsValue::from_str("WasmDb is closed")),
            }
            .map_err(to_js_error)?;
            Ok(
                jazz::binding_codec::encode_initialization_statuses(&ids, &statuses)
                    .map_err(to_js_error)?
                    .into(),
            )
        }))
    }
}

pub(super) async fn open_cached_db<S>(
    schema: JazzSchema,
    storage: S,
    config: WasmOpenDbConfig,
    cached: Option<&[u8]>,
) -> Result<Db, jazz::db::Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let Some(cached) = cached else {
        return open_db(schema, storage, config).await;
    };
    if config.history_complete {
        return Err(Error {
            code: ErrorCode::Protocol,
            message: "cached catalogue cannot establish history completeness".into(),
        });
    }
    let mut db_config = DbConfig::new(schema, storage, config.identity.into());
    if let Some(seed) = config.row_id_seed {
        db_config = db_config.with_id_source(SeededRowIdSource::new(seed));
    }
    // SAFETY: the private runtime host validates the app-scoped cache envelope.
    let db = unsafe { Db::open_with_cached_catalogue(db_config, Some(cached)).await? };
    configure_initial_sync_flush_cadence(&db, config.initial_sync_flush_every)?;
    Ok(db)
}
