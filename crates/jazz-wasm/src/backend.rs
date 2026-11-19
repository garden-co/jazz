//! What a backend's `createJazz` needs from Jazz when it runs on this WASM
//! build instead of `jazz-napi` (a Cloudflare Worker, where native addons
//! cannot load): a database serving thin sessions' transactions, the router
//! splitting a thin connection between the core and the backend, admitting
//! a device account's session, waiting on a trusted session's writes, and
//! loading the app the backend publishes. Each mirrors the `jazz-napi`
//! export of the same name; where napi answers a value or a pending read,
//! these answer a `Promise`.

use std::cell::RefCell;
use std::rc::Rc;

use jazz::tx::DurabilityTier;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use crate::{durability_tier_from_str, host_error, WasmDb};

/// A thin session's transactions: idle between requests, taken while one
/// is served.
pub(crate) enum ThinState {
    Idle(jazz::thin_transactions::ThinTransactions),
    Serving,
}

/// The session's transactions while a request is served: back in their cell
/// when it ends, or when the caller drops it (a request it stopped waiting
/// for), so the next request finds them.
struct Serving {
    cell: Rc<RefCell<Option<ThinState>>>,
    transactions: Option<jazz::thin_transactions::ThinTransactions>,
}

impl Drop for Serving {
    fn drop(&mut self) {
        if let Some(transactions) = self.transactions.take() {
            *self.cell.borrow_mut() = Some(ThinState::Idle(transactions));
        }
    }
}

fn error(message: impl std::fmt::Display) -> JsValue {
    js_sys::Error::new(&message.to_string()).into()
}

#[wasm_bindgen]
impl WasmDb {
    /// Serve one of a thin session's transaction requests (SPEC 8.14), as
    /// the backend's client linked to the core as that session: `request`
    /// is the request's wire frame, the answer the response's. A commit
    /// answers once the core accepted it. One request at a time. (napi:
    /// `serveThinTransaction`.)
    #[wasm_bindgen(js_name = serveThinTransaction)]
    pub fn serve_thin_transaction(&self, request: Vec<u8>) -> js_sys::Promise {
        let inner = match self.open_inner() {
            Ok(inner) => inner,
            Err(error) => return js_sys::Promise::reject(&error),
        };
        let request = match jazz::wire::decode_postcard_frame(&request) {
            Ok(jazz::wire::WireFrame::ThinRequest(request)) => request,
            _ => {
                return js_sys::Promise::reject(&error(
                    "serveThinTransaction takes a thin request frame",
                ));
            }
        };
        let binding = inner.binding.clone();
        let thin = Rc::clone(&inner.host.thin);
        let state = thin.borrow_mut().replace(ThinState::Serving);
        let transactions = match state {
            Some(ThinState::Idle(transactions)) => transactions,
            None => {
                let made = if binding.author() == jazz::ids::AuthorSubject::SYSTEM {
                    // A trusted backend client's session, linked as the
                    // backend itself: its writes are the backend's, as the
                    // core ran them as SYSTEM.
                    Ok(jazz::thin_transactions::ThinTransactions::trusted())
                } else {
                    jazz::thin_transactions::ThinTransactions::new(binding.author())
                };
                match made {
                    Ok(transactions) => transactions,
                    Err(failure) => {
                        *thin.borrow_mut() = None;
                        return js_sys::Promise::reject(&host_error(
                            jazz::binding_host::HostError::Engine(failure),
                        ));
                    }
                }
            }
            Some(ThinState::Serving) => {
                *thin.borrow_mut() = Some(ThinState::Serving);
                return js_sys::Promise::reject(&error(
                    "serveThinTransaction serves one request at a time",
                ));
            }
        };
        let mut serving = Serving {
            cell: thin,
            transactions: Some(transactions),
        };
        future_to_promise(async move {
            let body = serving
                .transactions
                .as_mut()
                .expect("held until dropped")
                .serve(&binding.db, request.id, &request.op)
                .await;
            drop(serving);
            let frame = jazz::wire::encode_frame(&jazz::wire::WireFrame::ThinResponse(
                jazz::thin::ThinResponse {
                    id: request.id,
                    body,
                },
            ))
            .map_err(|failure| error(format!("encode thin answer: {failure}")))?;
            Ok(js_sys::Uint8Array::from(frame.as_slice()).into())
        })
    }

    /// End the thin session's open transactions and uploads: nothing of
    /// them reached the core. (napi: `releaseThinTransactions`.)
    #[wasm_bindgen(js_name = releaseThinTransactions)]
    pub fn release_thin_transactions(&self) -> js_sys::Promise {
        let inner = match self.open_inner() {
            Ok(inner) => inner,
            Err(error) => return js_sys::Promise::reject(&error),
        };
        let binding = inner.binding.clone();
        let state = inner.host.thin.borrow_mut().take();
        future_to_promise(async move {
            if let Some(ThinState::Idle(transactions)) = state {
                transactions.release(&binding.db).await;
            }
            Ok(JsValue::UNDEFINED)
        })
    }

    /// Admit a verified device account for this backend runtime only.
    /// (napi: `admitLocalFirstSession`, the name jazz-tools calls.)
    #[wasm_bindgen(js_name = admitLocalFirstSession)]
    pub fn admit_local_first_session(
        &self,
        token: String,
        app_id: String,
        claimed_author: String,
    ) -> Result<(), JsValue> {
        self.binding()?
            .admit_device_account_session(&token, &app_id, &claimed_author)
            .map_err(host_error)
    }

    /// TS `waitForSync`: resolve once `writer`'s writes so far (a trusted
    /// session's author bytes; none for this database's own) reached
    /// `tier`, failing with a rejection of theirs, including one that
    /// settled before and no wait reported. (napi: `waitForSync`.)
    #[wasm_bindgen(js_name = waitForSync)]
    pub fn wait_for_sync(&self, tier: String, writer: Option<Vec<u8>>) -> js_sys::Promise {
        let inner = match self.open_inner() {
            Ok(inner) => inner,
            Err(error) => return js_sys::Promise::reject(&error),
        };
        future_to_promise(async move {
            match durability_tier_from_str(&tier)? {
                DurabilityTier::None => {}
                DurabilityTier::Local => inner.flush().await?,
                DurabilityTier::Global => inner
                    .binding
                    .wait_for_sync_global(writer.as_deref())
                    .map_err(host_error)?
                    .await
                    .map_err(host_error)?,
            }
            Ok(JsValue::UNDEFINED)
        })
    }
}

/// One thin connection's frames split between the core and the session's
/// backend, in the client's order (SPEC 8.14;
/// `jazz::thin_transactions::ThinRouter`). Messages are WebSocket messages
/// (frame batches). (napi: `NapiThinRouter`.)
#[wasm_bindgen]
pub struct WasmThinRouter {
    router: RefCell<jazz::thin_transactions::ThinRouter>,
}

/// One step a [`WasmThinRouter`] releases: a message for the core, or a
/// request frame for the backend's client (`serveThinTransaction`).
#[wasm_bindgen(getter_with_clone)]
pub struct WasmThinStep {
    pub core: Option<js_sys::Uint8Array>,
    pub backend: Option<js_sys::Uint8Array>,
    /// The backend request's id.
    pub id: Option<f64>,
    /// Whether the backend request is a commit.
    pub commit: Option<bool>,
}

impl Default for WasmThinRouter {
    fn default() -> Self {
        Self::new()
    }
}

// The JS names are napi's (`fromClient`, `fromCore`): they read as "the
// message from the client", not conversions.
#[allow(clippy::wrong_self_convention)]
#[wasm_bindgen]
impl WasmThinRouter {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            router: RefCell::default(),
        }
    }

    /// Take one client message. Throws when it does not decode: the
    /// connection is broken.
    #[wasm_bindgen(js_name = fromClient)]
    pub fn from_client(&self, message: Vec<u8>) -> Result<(), JsValue> {
        let frames = jazz::wire::decode_websocket_frame_batch(&message)
            .map_err(|failure| error(format!("undecodable message: {failure}")))?;
        self.router.borrow_mut().from_client(frames).map_err(error)
    }

    /// Note one message the core sent the client: whether it answered a
    /// request sent to the core.
    #[wasm_bindgen(js_name = fromCore)]
    pub fn from_core(&self, message: Vec<u8>) -> bool {
        jazz::wire::decode_websocket_frame_batch(&message)
            .is_ok_and(|frames| self.router.borrow_mut().from_core(&frames))
    }

    /// Note the backend's answer (a response frame, as
    /// `serveThinTransaction` gives it), and return the messages that carry
    /// it to the client.
    pub fn answer(&self, answer: Vec<u8>) -> Result<Vec<js_sys::Uint8Array>, JsValue> {
        let Ok(jazz::wire::WireFrame::ThinResponse(answer)) =
            jazz::wire::decode_postcard_frame(&answer)
        else {
            return Err(error("answer takes a thin response frame"));
        };
        self.router.borrow_mut().answered(&answer);
        jazz::thin_transactions::answer_frames(answer)
            .into_iter()
            .map(|frame| {
                jazz::wire::encode_websocket_frame_batch(&[frame])
                    .map(|message| js_sys::Uint8Array::from(message.as_slice()))
                    .map_err(error)
            })
            .collect()
    }

    /// Answer backend request `id` with an error the backend gave in its
    /// place (`code`, `message`): a request it stopped waiting for, or one
    /// it could not serve. Returns the messages that carry it to the client.
    pub fn refuse(
        &self,
        id: f64,
        code: String,
        message: String,
    ) -> Result<Vec<js_sys::Uint8Array>, JsValue> {
        let frame = jazz::wire::encode_frame(&jazz::wire::WireFrame::ThinResponse(
            jazz::thin::ThinResponse {
                id: id as u64,
                body: jazz::thin::ThinBody::Error { code, message },
            },
        ))
        .map_err(error)?;
        self.answer(frame)
    }

    /// Whether a client's Hello (its first message after the prelude) asks
    /// for thin results: a thin client, whose transactions its backend
    /// serves.
    #[wasm_bindgen(js_name = asksThinResults)]
    pub fn asks_thin_results(message: Vec<u8>) -> bool {
        let Ok(frames) = jazz::wire::decode_websocket_frame_batch(&message) else {
            return false;
        };
        frames.first().is_some_and(|frame| {
            matches!(
                jazz::wire::decode_frame(frame),
                Ok(jazz::wire::WireFrame::Hello(hello))
                    if hello.features & jazz::wire::FEATURE_THIN_RESULTS != 0
            )
        })
    }

    /// The public schema JSON the client bound the connection to, once it
    /// did.
    pub fn schema(&self) -> Option<js_sys::Uint8Array> {
        self.router.borrow().schema().map(js_sys::Uint8Array::from)
    }

    /// Whether something routed waits on an answer that has not come.
    #[wasm_bindgen(js_name = isWaiting)]
    pub fn is_waiting(&self) -> bool {
        self.router.borrow().is_waiting()
    }

    /// What may go now, in order.
    pub fn ready(&self) -> Result<Vec<WasmThinStep>, JsValue> {
        let mut steps = Vec::new();
        for step in self.router.borrow_mut().ready() {
            match step {
                jazz::thin_transactions::Step::ToCore(frames) => {
                    for frame in frames {
                        let message =
                            jazz::wire::encode_websocket_frame_batch(&[frame]).map_err(error)?;
                        steps.push(WasmThinStep {
                            core: Some(js_sys::Uint8Array::from(message.as_slice())),
                            backend: None,
                            id: None,
                            commit: None,
                        });
                    }
                }
                jazz::thin_transactions::Step::ToBackend(request) => {
                    let id = request.id as f64;
                    let commit = matches!(request.op, jazz::thin::ThinOp::Commit { .. });
                    let frame =
                        jazz::wire::encode_frame(&jazz::wire::WireFrame::ThinRequest(request))
                            .map_err(error)?;
                    steps.push(WasmThinStep {
                        core: None,
                        backend: Some(js_sys::Uint8Array::from(frame.as_slice())),
                        id: Some(id),
                        commit: Some(commit),
                    });
                }
            }
        }
        Ok(steps)
    }
}

/// Read and check a backend's `jazz.permissions.json` (`permissions`)
/// against the compiled app its generated bindings embed (`schema`: JSON of
/// `{ schema, relations }`, the options they pass to `bindApp`): `{ current,
/// bundle, permissions, deployment, warnings }`, as napi's `loadBackendApp`
/// answers. An app this build cannot deploy fails with an error whose
/// `code` is the artifact's unsupported code.
#[wasm_bindgen(js_name = loadBackendApp)]
pub fn load_backend_app(schema: String, permissions: String) -> Result<JsValue, JsValue> {
    use jazz::tools::deployment::artifact;
    let loaded =
        artifact::load_backend(schema.as_bytes(), permissions.as_bytes()).map_err(|failure| {
            let thrown = js_sys::Error::new(&failure.to_string());
            if failure.is_unsupported() {
                let _ = js_sys::Reflect::set(
                    &thrown,
                    &JsValue::from_str("code"),
                    &JsValue::from_str(artifact::UNSUPPORTED_CODE),
                );
            }
            JsValue::from(thrown)
        })?;
    let value = serde_json::json!({
        "current": loaded.current_hash().to_string(),
        "bundle": loaded.bundle().to_hex(),
        "permissions": loaded.permissions(),
        "deployment": loaded.deployment_body(false),
        "warnings": loaded.warnings(),
    });
    js_sys::JSON::parse(&value.to_string())
}
