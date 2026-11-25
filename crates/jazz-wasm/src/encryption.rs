//! The end-to-end encryption runtime in the browser's module: the same
//! runtime the Node binding runs (`jazz_e2ee_client::host`), over a private
//! store the page supplies from JavaScript. Each tab's database installs it
//! and decrypts and seals itself; the page's store is shared by the tabs.
//!
//! The runtime's futures are polled by the host after every call that can
//! make progress (a noop waker), as in the Node binding, so the store's
//! requests are taken and answered by the same JavaScript that serves Node's.

use std::cell::RefCell;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use jazz_e2ee_client::host::bridge::JsStore;
use jazz_e2ee_client::host::runtime::{self, Built, Encryption, JsonFuture, Options};
use wasm_bindgen::prelude::*;

use super::{WasmDb, WasmDbInner};
use jazz::binding_host::{self, Binding, OpenAs};
use jazz::db::block_on;

/// A promise waiting on a runtime call, settled with the JSON text of its
/// result or an error.
struct PendingJson {
    future: JsonFuture,
    resolve: js_sys::Function,
    reject: js_sys::Function,
}

thread_local! {
    static PENDING: RefCell<Vec<PendingJson>> = const { RefCell::new(Vec::new()) };
}

impl PendingJson {
    /// Whether the call finished (and its promise was settled).
    fn poll(&mut self) -> bool {
        let mut context = Context::from_waker(Waker::noop());
        let result = match self.future.as_mut().poll(&mut context) {
            Poll::Ready(result) => result,
            Poll::Pending => return false,
        };
        match result {
            Ok(value) => {
                let _ = self
                    .resolve
                    .call1(&JsValue::NULL, &JsValue::from_str(&value.to_string()));
            }
            Err(message) => {
                let _ = self
                    .reject
                    .call1(&JsValue::NULL, &js_sys::Error::new(&message));
            }
        }
        true
    }
}

fn json_promise(future: JsonFuture) -> js_sys::Promise {
    let mut settle = None;
    let promise = js_sys::Promise::new(&mut |resolve, reject| settle = Some((resolve, reject)));
    let (resolve, reject) = settle.expect("a promise runs its executor at once");
    let mut pending = PendingJson {
        future,
        resolve,
        reject,
    };
    if !pending.poll() {
        PENDING.with(|pending_calls| pending_calls.borrow_mut().push(pending));
    }
    promise
}

/// Poll every call that waits: the answer it needs may have arrived.
fn settle_promises() {
    let mut pending = PENDING.with(|calls| std::mem::take(&mut *calls.borrow_mut()));
    pending.retain_mut(|call| !call.poll());
    PENDING.with(|calls| {
        let mut calls = calls.borrow_mut();
        pending.append(&mut calls);
        *calls = pending;
    });
}

/// The runtime of an app, built over the private store the page supplies,
/// before the database that uses it opens. The store's requests are taken
/// with `takeStoreRequests` and answered with `replyStore`.
#[wasm_bindgen(js_name = EncryptionRuntime)]
pub struct WasmEncryptionRuntime {
    store: Rc<JsStore>,
    options: Rc<RefCell<Option<Options>>>,
    building: runtime::BuildSlot,
    account_id: String,
    app_id: String,
    built: Rc<RefCell<Option<Built>>>,
}

#[wasm_bindgen(js_class = EncryptionRuntime)]
impl WasmEncryptionRuntime {
    /// A runtime for the app whose generated bindings embed the compiled app
    /// `bindings` (JSON of `{ schema, relations }`), for the account
    /// `account_id` of the session that will open the database;
    /// `device_account_secret` (`jazz-auth-v1:…`) for a device
    /// account, whose secret is its recovery secret.
    #[wasm_bindgen(js_name = fromBindings)]
    pub fn from_bindings(
        bindings: String,
        app_id: String,
        environment: String,
        account_id: String,
        device_account_secret: Option<String>,
    ) -> Result<WasmEncryptionRuntime, JsValue> {
        let device_account_secret = device_account_secret
            .map(jazz::identity::DeviceAccountSecret::parse_owned)
            .transpose()
            .map_err(super::to_js_error)?;
        Ok(WasmEncryptionRuntime {
            store: Rc::default(),
            options: Rc::new(RefCell::new(Some(Options {
                app_file: bindings.into_bytes(),
                from_bindings: true,
                device_account_secret,
                app_id: app_id.clone(),
                environment,
            }))),
            building: runtime::BuildSlot::default(),
            account_id,
            app_id,
            built: Rc::default(),
        })
    }

    /// Builds the runtime: settles when its device is loaded or made, which
    /// takes the store (answer its requests while waiting).
    pub fn ready(&self) -> js_sys::Promise {
        let (build, held) = {
            let options = self.options.borrow();
            let Some(options) = options.as_ref() else {
                return js_sys::Promise::reject(&JsValue::from_str(runtime::ALREADY_BUILT));
            };
            let held = match self.building.take() {
                Ok(held) => held,
                Err(refusal) => return js_sys::Promise::reject(&JsValue::from_str(refusal)),
            };
            let build = runtime::build(options, self.account_id.clone(), Rc::clone(&self.store));
            (build, held)
        };
        let options = Rc::downgrade(&self.options);
        let slot = Rc::clone(&self.built);
        json_promise(Box::pin(async move {
            let result = build.await;
            drop(held);
            let built = result?;
            if let Some(options) = options.upgrade() {
                options.borrow_mut().take();
            }
            *slot.borrow_mut() = Some(built);
            Ok(serde_json::Value::Null)
        }))
    }

    /// The store requests queued since the last call, each a JSON text.
    #[wasm_bindgen(js_name = takeStoreRequests)]
    pub fn take_store_requests(&self) -> Vec<String> {
        let requests = self.store.take_requests();
        settle_promises();
        requests
    }

    /// The store's answer to a request; `false` when no such request is
    /// outstanding (the answer is dropped).
    #[wasm_bindgen(js_name = replyStore)]
    pub fn reply_store(&self, id: String, ok: bool, payload: String) -> bool {
        let accepted = id
            .parse()
            .is_ok_and(|id| self.store.reply(id, ok, &payload));
        settle_promises();
        accepted
    }
}

/// Bind the app's encryption declarations to this deployment.
#[cfg(target_arch = "wasm32")]
pub(crate) fn keyless_declarations(
    bindings: &str,
    app_id: &str,
    environment: &str,
) -> Result<jazz::e2ee::E2eeDeclarations, JsValue> {
    let app = jazz::tools::AppId::from_string(app_id)
        .unwrap_or_else(|_| jazz::tools::AppId::from_name(app_id));
    jazz::tools::deployment::artifact::bound_declarations(bindings.as_bytes(), app, environment)
        .map_err(|failure| error(failure.to_string()))?
        .ok_or_else(|| error("the app's bindings declare no encryption"))
}

fn error(message: impl AsRef<str>) -> JsValue {
    js_sys::Error::new(message.as_ref()).into()
}

fn proof_of(proof: &JsValue) -> Result<(Option<String>, Option<String>), JsValue> {
    if proof.is_undefined() || proof.is_null() {
        return Ok((None, None));
    }
    let field = |name: &str| {
        js_sys::Reflect::get(proof, &JsValue::from_str(name))
            .ok()
            .and_then(|value| value.as_string())
    };
    Ok((field("token"), field("claimedAuthor")))
}

impl WasmDb {
    #[cfg(all(target_arch = "wasm32", feature = "backend"))]
    fn open_keyless_as(
        schema: &[u8],
        config: &[u8],
        bindings: &str,
        app_id: &str,
        environment: &str,
        how: OpenAs<'_>,
    ) -> Result<Self, JsValue> {
        super::initialize_wasm_tracing();
        let (config, identity, trusted) = Self::prepare_open(schema, config, how)?;
        let hooks = jazz_e2ee_client::KeylessHooks::hooks(keyless_declarations(
            bindings,
            app_id,
            environment,
        )?);
        let db = block_on(binding_host::open_memory(config.with_e2ee(hooks)))
            .map_err(super::to_js_error)?;
        Ok(Self::runtime_owner(block_on(Binding::new(
            db, identity, trusted,
        ))))
    }

    /// The encryption runtime of this database, or why there is none.
    fn encryption(&self) -> Result<Rc<Encryption>, JsValue> {
        self.open_inner()?
            .host
            .encryption
            .borrow()
            .clone()
            .ok_or_else(|| error("this database has no encryption runtime"))
    }
}

#[wasm_bindgen]
impl WasmDb {
    /// Open the Worker's own database with keyless backend hooks.
    #[cfg(all(target_arch = "wasm32", feature = "backend"))]
    #[wasm_bindgen(js_name = openMemoryAsBackendKeyless)]
    pub fn open_memory_as_backend_keyless(
        schema: &[u8],
        config: &[u8],
        bindings: &str,
        app_id: &str,
        environment: &str,
    ) -> Result<WasmDb, JsValue> {
        Self::open_keyless_as(
            schema,
            config,
            bindings,
            app_id,
            environment,
            OpenAs::Backend,
        )
    }

    /// Serve a thin session with the same keyless hooks, as its own author.
    #[cfg(all(target_arch = "wasm32", feature = "backend"))]
    #[wasm_bindgen(js_name = openMemoryKeyless)]
    pub fn open_memory_keyless(
        schema: &[u8],
        config: &[u8],
        bindings: &str,
        app_id: &str,
        environment: &str,
        proof: JsValue,
    ) -> Result<WasmDb, JsValue> {
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Proof {
            token: String,
            app_id: String,
            claimed_author: String,
        }
        let proof: Option<Proof> =
            serde_wasm_bindgen::from_value(proof).map_err(super::to_js_error)?;
        let how = match &proof {
            Some(proof) => OpenAs::SelfSigned {
                token: &proof.token,
                app_id: &proof.app_id,
                claimed_author: &proof.claimed_author,
            },
            None => OpenAs::Untrusted,
        };
        Self::open_keyless_as(schema, config, bindings, app_id, environment, how)
    }

    /// Open an encrypted app's in-memory database with its runtime
    /// installed (see [`WasmEncryptionRuntime`]). `proof` is `{ token,
    /// claimedAuthor }` for a self-signed client identity.
    #[wasm_bindgen(js_name = openMemoryEncrypted)]
    pub fn open_memory_encrypted(
        schema: Vec<u8>,
        config: Vec<u8>,
        runtime: &WasmEncryptionRuntime,
        proof: JsValue,
    ) -> Result<WasmDb, JsValue> {
        super::initialize_wasm_tracing();
        let (token, author) = proof_of(&proof)?;
        let how = match (&token, &author) {
            (Some(token), Some(author)) => OpenAs::SelfSigned {
                token,
                app_id: &runtime.app_id,
                claimed_author: author,
            },
            _ => OpenAs::Untrusted,
        };
        let (db_config, identity, trusted) = Self::prepare_open(&schema, &config, how)?;
        let hooks = {
            let slot = runtime.built.borrow();
            let built = slot
                .as_ref()
                .ok_or_else(|| error("the encryption runtime is not ready"))?;
            if identity
                .author
                .account_id()
                .map(|account| account.0.to_string())
                != Some(runtime.account_id.clone())
            {
                return Err(error(
                    "the encryption runtime is for another account than this session's",
                ));
            }
            built.client.hooks()
        };
        let db_config = db_config.with_e2ee(hooks);
        let db = block_on(binding_host::open_memory(db_config)).map_err(super::to_js_error)?;
        let owner = Self::runtime_owner(block_on(Binding::new(db, identity, trusted)));
        let inner: WasmDbInner = owner.open_inner()?;
        let built = runtime
            .built
            .borrow_mut()
            .take()
            .expect("runtime checked before open");
        let host = Rc::downgrade(&inner.host);
        runtime.store.set_wake(Box::new(move || {
            let Some(host) = host.upgrade() else { return };
            let scheduler = host.tick_scheduler.borrow().clone();
            if let Some(scheduler) = scheduler {
                let _ = scheduler.call1(&JsValue::NULL, &JsValue::from_str("immediate"));
            }
        }));
        *inner.host.encryption.borrow_mut() = Some(Encryption::new(
            built,
            Rc::clone(&runtime.store),
            Rc::clone(&inner.binding.db),
        ));
        Ok(owner)
    }

    /// The private-store requests queued since the last call, each a JSON text.
    #[wasm_bindgen(js_name = takeStoreRequests)]
    pub fn take_store_requests(&self) -> Result<Vec<String>, JsValue> {
        let encryption = self.encryption()?;
        let requests = encryption.store.take_requests();
        // The host polls here, so a request the store never answers times out.
        encryption.drive();
        settle_promises();
        encryption.wake_if_owed();
        Ok(requests)
    }

    /// The store's answer to a request; `false` when no such request is
    /// outstanding (the answer is dropped).
    #[wasm_bindgen(js_name = replyStore)]
    pub fn reply_store(&self, id: String, ok: bool, payload: String) -> Result<bool, JsValue> {
        let encryption = self.encryption()?;
        let accepted = id
            .parse()
            .is_ok_and(|id| encryption.store.reply(id, ok, &payload));
        encryption.drive();
        settle_promises();
        encryption.wake_if_owed();
        Ok(accepted)
    }

    /// Run one end-to-end encryption operation: its name and arguments,
    /// answered with the JSON text of its result.
    #[wasm_bindgen(js_name = encryptionCall)]
    pub fn encryption_call(&self, op: String, args: JsValue) -> Result<js_sys::Promise, JsValue> {
        let encryption = self.encryption()?;
        let args = super::json_or_null(args)?;
        let future = encryption.call(op, args);
        let driver = Rc::clone(&encryption);
        let promise = json_promise(Box::pin(async move {
            let result = future.await;
            driver.drive();
            result
        }));
        encryption.drive();
        settle_promises();
        encryption.wake_if_owed();
        Ok(promise)
    }

    /// The runtime's reports since the last call, each a JSON text.
    #[wasm_bindgen(js_name = takeEncryptionReports)]
    pub fn take_encryption_reports(&self) -> Result<Vec<String>, JsValue> {
        Ok(self.encryption()?.take_reports())
    }

    /// The runtime's account status and last error, as JSON text.
    #[wasm_bindgen(js_name = encryptionState)]
    pub fn encryption_state(&self) -> Result<String, JsValue> {
        Ok(self.encryption()?.state().to_string())
    }

    /// An app subscription on an encrypted table opened (`open`) or closed.
    #[wasm_bindgen(js_name = encryptionUseSpace)]
    pub fn encryption_use_space(
        &self,
        table: String,
        id: String,
        open: bool,
    ) -> Result<(), JsValue> {
        self.encryption()?.use_space(open, table, id);
        Ok(())
    }

    /// Where each encrypted table keeps its space, as JSON text.
    #[wasm_bindgen(js_name = encryptionSpaces)]
    pub fn encryption_spaces(&self) -> Result<String, JsValue> {
        Ok(self.encryption()?.spaces.to_string())
    }

    /// The connection to the server went away (`false`) or came back (`true`).
    #[wasm_bindgen(js_name = encryptionSetOnline)]
    pub fn encryption_set_online(&self, online: bool) -> Result<(), JsValue> {
        self.encryption()?.set_online(online);
        Ok(())
    }
}

#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;
    use jazz::binding_host::{OpenConfig, OpenIdentity};
    use jazz::ids::{AuthorSubject, NodeUuid};
    use wasm_bindgen_futures::JsFuture;
    use wasm_bindgen_test::wasm_bindgen_test;

    /// Alice's malformed device secret is refused at browser runtime construction,
    /// before her application declarations or private store are read.
    #[wasm_bindgen_test]
    fn device_secret_is_checked_before_readiness() {
        let result = WasmEncryptionRuntime::from_bindings(
            "invalid app".into(),
            "test".into(),
            "test".into(),
            "alice".into(),
            Some("jazz-auth-v1:short".into()),
        );
        assert!(result.is_err_and(|error| {
            error.as_string().as_deref() == Some("invalid device account secret")
        }));
    }

    /// Alice can retry readiness after her private store refuses a request,
    /// and Bob's refused open leaves the built runtime available to Alice.
    #[wasm_bindgen_test]
    async fn encryption_startup_can_retry_after_store_failure_and_account_mismatch() {
        let app =
            include_bytes!("../../../packages/jazz-tools/tests/fixtures/encrypted/jazz.app.json");
        let definition = jazz::tools::deployment::artifact::load_app(app).unwrap();
        let schema = serde_json::to_vec(definition.current_schema()).unwrap();
        let alice = AuthorSubject::for_test_bytes([1; 16]);
        let bob = AuthorSubject::for_test_bytes([2; 16]);
        let runtime = WasmEncryptionRuntime {
            store: Rc::default(),
            options: Rc::new(RefCell::new(Some(Options {
                app_file: app.to_vec(),
                from_bindings: false,
                app_id: "test".into(),
                environment: "test".into(),
                device_account_secret: Some(jazz::identity::DeviceAccountSecret::from_bytes(
                    [1; 32],
                )),
            }))),
            building: runtime::BuildSlot::default(),
            account_id: alice.account_id().unwrap().0.to_string(),
            app_id: "test".into(),
            built: Rc::default(),
        };
        let id = |request: &str| {
            serde_json::from_str::<serde_json::Value>(request).unwrap()["id"]
                .as_str()
                .unwrap()
                .to_owned()
        };
        let failed = JsFuture::from(runtime.ready());
        assert!(
            JsFuture::from(runtime.ready()).await.is_err(),
            "build in progress"
        );
        let requests = runtime.take_store_requests();
        assert_eq!(requests.len(), 1);
        assert!(runtime.reply_store(id(&requests[0]), false, "store unavailable".into()));
        let failure = js_sys::Error::from(failed.await.unwrap_err());
        assert!(failure
            .message()
            .as_string()
            .unwrap()
            .contains("store unavailable"));
        assert!(
            runtime.options.borrow().as_ref().is_some_and(|options| {
                options
                    .device_account_secret
                    .as_ref()
                    .map(|secret| secret.as_bytes())
                    == Some(&[1; 32])
            }),
            "failed readiness preserves the recovery secret for a retry"
        );

        let store =
            jazz_e2ee_client::host::bridge::serve(Rc::new(jazz_e2ee_client::MemoryStore::new()));
        let retry = JsFuture::from(runtime.ready());
        loop {
            let requests = runtime.take_store_requests();
            if requests.is_empty() {
                break;
            }
            for request in requests {
                let request_id = id(&request);
                let (ok, payload) = store(request).await;
                assert!(ok, "{payload}");
                assert!(runtime.reply_store(request_id, ok, payload));
            }
        }
        retry.await.expect("readiness can be retried");
        assert!(
            JsFuture::from(runtime.ready()).await.is_err(),
            "build completed"
        );

        let open = |author| {
            let config = postcard::to_allocvec(&OpenConfig {
                identity: OpenIdentity {
                    node: NodeUuid::from_bytes([3; 16]),
                    author,
                },
                row_id_seed: None,
                history_complete: false,
                initial_sync_flush_every: None,
                migrations: None,
                system_node: None,
            })
            .unwrap();
            WasmDb::open_memory_encrypted(schema.clone(), config, &runtime, JsValue::UNDEFINED)
        };
        let failure =
            js_sys::Error::from(open(bob).err().expect("Bob cannot open Alice's runtime"));
        assert!(failure
            .message()
            .as_string()
            .unwrap()
            .contains("for another account"));
        let owner = open(alice).expect("Alice can still open her runtime");
        assert!(owner.encryption().is_ok());
        let failure = js_sys::Error::from(
            open(alice)
                .err()
                .expect("the runtime is attached only once"),
        );
        assert!(failure.message().as_string().unwrap().contains("not ready"));
    }

    /// Alice's schema view can close independently. Closing or dropping the
    /// owning database frees the runtime, database and host, even if JS keeps
    /// the store bridge. Weak references observe the native lifetimes.
    #[wasm_bindgen_test]
    async fn the_encrypted_owner_releases_its_runtime_on_close_and_drop() {
        let app =
            include_bytes!("../../../packages/jazz-tools/tests/fixtures/encrypted/jazz.app.json");
        let definition = jazz::tools::deployment::artifact::load_app(app).unwrap();
        let schema = serde_json::to_vec(definition.current_schema()).unwrap();
        let author = AuthorSubject::for_test_bytes([1; 16]);
        let config = postcard::to_allocvec(&OpenConfig {
            identity: OpenIdentity {
                node: NodeUuid::from_bytes([2; 16]),
                author,
            },
            row_id_seed: None,
            history_complete: false,
            initial_sync_flush_every: None,
            migrations: None,
            system_node: None,
        })
        .unwrap();
        for close in [true, false] {
            let store = Rc::new(JsStore::served_by(jazz_e2ee_client::host::bridge::serve(
                Rc::new(jazz_e2ee_client::MemoryStore::new()),
            )));
            let account_id = author.account_id().unwrap().0.to_string();
            let options = Options {
                app_file: app.to_vec(),
                from_bindings: false,
                app_id: "test".into(),
                environment: "test".into(),
                device_account_secret: None,
            };
            let built = runtime::build(&options, account_id.clone(), store.clone())
                .await
                .unwrap();
            let bridge = WasmEncryptionRuntime {
                store,
                options: Rc::default(),
                building: runtime::BuildSlot::default(),
                account_id,
                app_id: "test".into(),
                built: Rc::new(RefCell::new(Some(built))),
            };
            let owner = WasmDb::open_memory_encrypted(
                schema.clone(),
                config.clone(),
                &bridge,
                JsValue::UNDEFINED,
            )
            .unwrap();
            let runtime = Rc::downgrade(&owner.encryption().unwrap());
            let inner = owner.open_inner().unwrap();
            let db = Rc::downgrade(&inner.binding.db);
            let host = Rc::downgrade(&inner.host);
            drop(inner);
            let view = owner.register_schema(schema.clone(), None).unwrap();
            JsFuture::from(view.close()).await.unwrap();
            drop(view);
            assert!(runtime.upgrade().is_some());
            if close {
                JsFuture::from(owner.close()).await.unwrap();
            } else {
                drop(owner);
            }
            assert!(
                runtime.upgrade().is_none(),
                "runtime retained (explicit close: {close})"
            );
            assert!(db.upgrade().is_none(), "database retained");
            assert!(host.upgrade().is_none(), "store wake retained the host");
        }
    }
}
