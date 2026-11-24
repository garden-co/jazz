//! The end-to-end encryption runtime in the browser's module: the same
//! runtime the Node binding runs (`jazz_e2ee_client::host`), over a private
//! store the page supplies from JavaScript. Each tab's database installs it
//! and decrypts and seals itself; the page's store is shared by the tabs.
//!
//! The runtime's futures are polled by the host after every call that can
//! make progress (a noop waker), as in the Node binding, so the store's
//! requests are taken and answered by the same JavaScript that serves Node's.

use std::rc::Rc;

use jazz_e2ee_client::host::runtime::{Encryption, JsonFuture, RuntimeBuilder};
use wasm_bindgen::prelude::*;

use super::{WasmDb, WasmDbInner};
use jazz::binding_host::OpenAs;

thread_local! {
    static PENDING: jazz_e2ee_client::host::PromiseQueue = Default::default();
}

fn json_promise(future: JsonFuture) -> js_sys::Promise {
    let mut settle = None;
    let promise = js_sys::Promise::new(&mut |resolve, reject| settle = Some((resolve, reject)));
    let (resolve, reject) = settle.expect("a promise runs its executor at once");
    PENDING.with(|queue| {
        queue.submit(async move {
            match future.await {
                Ok(value) => {
                    let _ = resolve.call1(&JsValue::NULL, &JsValue::from_str(&value.to_string()));
                }
                Err(message) => {
                    let _ = reject.call1(&JsValue::NULL, &js_sys::Error::new(&message));
                }
            }
        })
    });
    promise
}

fn settle_promises() {
    PENDING.with(|queue| queue.settle());
}

/// The runtime of an app, built over the private store the page supplies,
/// before the database that uses it opens. The store's requests are taken
/// with `takeStoreRequests` and answered with `replyStore`.
#[wasm_bindgen(js_name = EncryptionRuntime)]
pub struct WasmEncryptionRuntime {
    builder: RuntimeBuilder,
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
        Ok(Self {
            builder: RuntimeBuilder::from_bindings(
                bindings,
                app_id,
                environment,
                account_id,
                device_account_secret,
            )
            .map_err(super::to_js_error)?,
        })
    }

    /// Builds the runtime: settles when its device is loaded or made, which
    /// takes the store (answer its requests while waiting).
    pub fn ready(&self) -> js_sys::Promise {
        match self.builder.build_future() {
            Some(future) => json_promise(future),
            None => js_sys::Promise::reject(&JsValue::from_str("the runtime is already built")),
        }
    }

    /// The store requests queued since the last call, each a JSON text.
    #[wasm_bindgen(js_name = takeStoreRequests)]
    pub fn take_store_requests(&self) -> Vec<String> {
        let requests = self.builder.take_requests();
        self.builder.drive(settle_promises);
        requests
    }

    /// The store's answer to a request; `false` when no such request is
    /// outstanding (the answer is dropped).
    #[wasm_bindgen(js_name = replyStore)]
    pub fn reply_store(&self, id: String, ok: bool, payload: String) -> bool {
        let accepted = self.builder.reply(&id, ok, &payload);
        self.builder.drive(settle_promises);
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
    jazz_e2ee_client::host::runtime::keyless_declarations(bindings, app_id, environment)
        .map_err(error)
}

fn error(message: impl AsRef<str>) -> JsValue {
    js_sys::Error::new(message.as_ref()).into()
}

pub(crate) struct SessionProof {
    token: Option<String>,
    author: Option<String>,
    #[cfg(all(target_arch = "wasm32", feature = "backend"))]
    app_id: Option<String>,
}

impl SessionProof {
    pub(crate) fn read(proof: &JsValue) -> Self {
        let field = |name: &str| {
            js_sys::Reflect::get(proof, &JsValue::from_str(name))
                .ok()
                .and_then(|value| value.as_string())
        };
        Self {
            token: field("token"),
            author: field("claimedAuthor"),
            #[cfg(all(target_arch = "wasm32", feature = "backend"))]
            app_id: field("appId"),
        }
    }

    pub(crate) fn for_app<'a>(&'a self, app_id: &'a str) -> OpenAs<'a> {
        match (&self.token, &self.author) {
            (Some(token), Some(claimed_author)) => OpenAs::SelfSigned {
                token,
                app_id,
                claimed_author,
            },
            _ => OpenAs::Untrusted,
        }
    }
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
        Self::open_memory_with(schema, config, how, |config, _| {
            let declarations = keyless_declarations(bindings, app_id, environment)?;
            Ok(config.with_e2ee(jazz_e2ee_client::KeylessHooks::hooks(declarations)))
        })
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
        let session = SessionProof::read(&proof);
        let how = if proof.is_null() || proof.is_undefined() {
            OpenAs::Untrusted
        } else {
            let app_id = session
                .app_id
                .as_deref()
                .ok_or_else(|| error("missing proof appId"))?;
            if session.token.is_none() || session.author.is_none() {
                return Err(error("missing session proof field"));
            }
            session.for_app(app_id)
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
        let proof = SessionProof::read(&proof);
        let owner = Self::open_memory_with(
            &schema,
            &config,
            proof.for_app(&runtime.builder.app_id),
            |config, identity| Ok(config.with_e2ee(runtime.builder.hooks(identity).map_err(error)?)),
        )?;
        let inner: WasmDbInner = owner.open_inner()?;
        let host = Rc::downgrade(&inner.host);
        *inner.host.encryption.borrow_mut() = Some(runtime.builder.attach(
            Rc::clone(&inner.binding.db),
            Box::new(move || {
                let Some(host) = host.upgrade() else { return };
                let scheduler = host.tick_scheduler.borrow().clone();
                if let Some(scheduler) = scheduler {
                    let _ = scheduler.call1(&JsValue::NULL, &JsValue::from_str("immediate"));
                }
            }),
        ));
        Ok(owner)
    }

    /// Run one end-to-end encryption operation: its name and arguments,
    /// answered with the JSON text of its result.
    #[wasm_bindgen(js_name = encryptionCall)]
    pub fn encryption_call(&self, op: String, args: JsValue) -> Result<js_sys::Promise, JsValue> {
        let encryption = self.encryption()?;
        let args = super::json_or_null(args)?;
        let promise = json_promise(encryption.call_driven(op, args));
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

    fn bindings(app: &jazz::tools::deployment::artifact::AppDefinition) -> String {
        serde_json::json!({
            "schema": {"current": app.current_hash(), "schemas": [{"hash": app.current_hash(), "schema": app.current_schema()}],
                "migrations": [], "encryption": app.encryption()},
            "relations": {}
        }).to_string()
    }

    async fn finish_build(runtime: &WasmEncryptionRuntime) {
        let store =
            jazz_e2ee_client::host::bridge::serve(Rc::new(jazz_e2ee_client::MemoryStore::new()));
        let ready = JsFuture::from(runtime.ready());
        loop {
            let requests = runtime.take_store_requests();
            if requests.is_empty() {
                break;
            }
            for request in requests {
                let request_id = serde_json::from_str::<serde_json::Value>(&request).unwrap()["id"]
                    .as_str()
                    .unwrap()
                    .to_owned();
                let (ok, payload) = store(request).await;
                assert!(ok, "{payload}");
                assert!(runtime.reply_store(request_id, ok, payload));
            }
        }
        ready.await.expect("readiness should succeed");
    }

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
        let runtime = WasmEncryptionRuntime::from_bindings(
            bindings(&definition),
            "test".into(),
            "test".into(),
            alice.account_id().unwrap().0.to_string(),
            Some(
                jazz::identity::DeviceAccountSecret::from_bytes([1; 32])
                    .format()
                    .to_string(),
            ),
        )
        .unwrap();
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
        finish_build(&runtime).await;

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
            let bridge = WasmEncryptionRuntime::from_bindings(
                bindings(&definition),
                "test".into(),
                "test".into(),
                author.account_id().unwrap().0.to_string(),
                None,
            )
            .unwrap();
            finish_build(&bridge).await;
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
