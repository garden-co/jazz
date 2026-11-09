//! WASM hosting for the shared Jazz authority.
//!
//! Startup supplies an opened database and catalogue through Rust APIs. The
//! JavaScript host owns event delivery; it must serialize ticks and admit
//! connections between ticks. Fixture publication and raw test routes live in dev/.
#[cfg(feature = "durable-object")]
mod durable_object;

use futures_channel::mpsc;
use futures_util::StreamExt;
use jazz::catalogue::{BundleId, CatalogueChange};
use jazz::db::Db;
use jazz::wire::{WireError, WireErrorCode, WireFrame, WireHello, WirePeerRole, WireRetry};
use jazz_server::runtime::websocket;
use jazz_server::runtime::{ServerCore, ServerSession, SessionInput};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Wake, Waker};
use tokio::sync::mpsc as output;
use wasm_bindgen::prelude::*;

fn error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

struct Schedule {
    sender: mpsc::UnboundedSender<()>,
    queued: Arc<AtomicBool>,
}
impl Wake for Schedule {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if !self.queued.swap(true, Ordering::AcqRel) {
            let _ = self.sender.unbounded_send(());
        }
    }
}

/// The channel waker contains no JS references. The receiver calls JavaScript
/// on a later microtask, after Rust has released its mutable borrows.
fn scheduler(callback: js_sys::Function) -> Waker {
    let (sender, mut receiver) = mpsc::unbounded();
    let queued = Arc::new(AtomicBool::new(false));
    let ready = Arc::clone(&queued);
    wasm_bindgen_futures::spawn_local(async move {
        while receiver.next().await.is_some() {
            ready.store(false, Ordering::Release);
            let _ = callback.call0(&JsValue::NULL);
        }
    });
    Waker::from(Arc::new(Schedule { sender, queued }))
}

#[wasm_bindgen]
pub struct WasmServerCore {
    core: Rc<RefCell<Option<ServerCore>>>,
    next_session: Cell<u64>,
    app: Option<String>,
    guests: RefCell<Vec<GuestSession>>,
    wake: Waker,
}

struct GuestSession {
    identity: jazz::ids::AuthorSubject,
    input: SessionInput,
    revoked: Rc<Cell<bool>>,
}

// A tick owns the core across await without holding a RefCell borrow. Dropping
// the future restores ownership as well, rather than leaving the host busy.
struct Running<'a> {
    slot: &'a RefCell<Option<ServerCore>>,
    core: Option<ServerCore>,
}
impl Drop for Running<'_> {
    fn drop(&mut self) {
        *self.slot.borrow_mut() = self.core.take();
    }
}

impl WasmServerCore {
    /// Host an opened authority. Catalogue selection and storage ownership are
    /// decided by startup before calling this constructor.
    pub fn from_db(
        db: Db,
        app: Option<String>,
        schedule: js_sys::Function,
    ) -> Result<Self, JsValue> {
        let wake = scheduler(schedule);
        let core = ServerCore::from_db(db, app.clone(), wake.clone()).map_err(error)?;
        core.accounts().initialize().map_err(error)?;
        Ok(Self {
            app,
            guests: RefCell::default(),
            core: Rc::new(RefCell::new(Some(core))),
            next_session: Cell::new(1),
            wake,
        })
    }

    pub fn is_idle(&self) -> bool {
        self.core.borrow().is_some()
    }

    /// Apply a validated catalogue change through the same authority path as
    /// native hosting. Ownership is restored if the future is cancelled.
    pub async fn apply_catalogue(&self, change: CatalogueChange) -> Result<(), JsValue> {
        let core = self
            .core
            .borrow_mut()
            .take()
            .ok_or_else(|| error("server core is already ticking"))?;
        let mut running = Running {
            slot: &self.core,
            core: Some(core),
        };
        let (_, result) = running
            .core
            .as_mut()
            .unwrap()
            .apply_catalogue(change, None)
            .await;
        result.map_err(error)
    }
}

#[wasm_bindgen]
impl WasmServerCore {
    /// Catalogue reads for an authenticated host adapter. These methods do not
    /// authenticate HTTP requests: the host must enforce its admin credential.
    #[wasm_bindgen(js_name = catalogueSnapshot)]
    pub fn catalogue_snapshot(&self) -> Result<String, JsValue> {
        let slot = self.core.borrow();
        let core = slot.as_ref().ok_or_else(|| error("server core is busy"))?;
        let snapshot = core.catalogue_snapshot().map_err(error)?;
        serde_json::to_string(&serde_json::json!({
            "schemas": snapshot.schemas(),
            "migrationGraph": snapshot.migration_graph(),
            "bundles": snapshot.bundles(),
        }))
        .map_err(error)
    }

    #[wasm_bindgen(js_name = catalogueSchema)]
    pub fn catalogue_schema(&self, hash: &str) -> Result<Option<String>, JsValue> {
        let hash = jazz::tools::public_schema::SchemaHash::from_hex(hash)
            .ok_or_else(|| error("invalid schema hash"))?;
        let slot = self.core.borrow();
        let core = slot.as_ref().ok_or_else(|| error("server core is busy"))?;
        core.catalogue_snapshot()
            .map_err(error)?
            .schema(&hash)
            .map(|schema| serde_json::to_string(&schema).map_err(error))
            .transpose()
    }

    /// Publish the same request body as the native admin bundles endpoint.
    /// Returns its status and JSON body; the host owns authentication, request
    /// limits, serialization with ticks, and keeping publication alive after a
    /// caller disconnects.
    #[wasm_bindgen(js_name = publishCatalogue)]
    pub async fn publish_catalogue(&self, request: &str) -> Result<String, JsValue> {
        let request = match serde_json::from_str::<jazz_server::runtime::deployment::DeployRequest>(request) {
            Ok(request) => request,
            Err(err) => return Ok(serde_json::json!({"status":400,"body":{"code":"bad_request","error":err.to_string()}}).to_string()),
        };
        let core = self
            .core
            .borrow_mut()
            .take()
            .ok_or_else(|| error("server core is busy"))?;
        let mut running = Running {
            slot: &self.core,
            core: Some(core),
        };
        let result = running
            .core
            .as_mut()
            .unwrap()
            .deploy(request, js_sys::Date::now() as u64)
            .await;
        let response = match result {
            Ok(body) => serde_json::json!({"status":200,"body":body}),
            Err(err) => serde_json::json!({"status":err.status.as_u16(),"body":err.body}),
        };
        Ok(response.to_string())
    }

    /// Authenticated, bounded HTTP input; the adapter supplies verified access.
    #[wasm_bindgen(js_name = accountRequest)]
    pub async fn account_request(
        &self,
        operation: &str,
        body: &str,
        access: &str,
    ) -> Result<String, JsValue> {
        use jazz_server::runtime::accounts;
        let core = self
            .core
            .borrow_mut()
            .take()
            .ok_or_else(|| error("server core is busy"))?;
        let running = Running {
            slot: &self.core,
            core: Some(core),
        };
        let registry = running.core.as_ref().unwrap().accounts();
        let app = self
            .app
            .as_deref()
            .and_then(|s| uuid::Uuid::parse_str(s).ok())
            .ok_or_else(|| error("account operations require an app id"))?;
        let result: Result<Option<accounts::AccountResponse>, accounts::Failure> = async {
            let admin = access == "admin";
            if !admin && access != "backend" {
                return Err((401, "backend credential required"));
            }
            if matches!(operation, "link" | "revoke") && !admin {
                return Err((401, "admin backend credential required"));
            }
            macro_rules! request {
                () => {
                    serde_json::from_str(body).map_err(|_| (400, "invalid_account_request"))?
                };
            }
            match operation {
                "resolve" => accounts::resolve(&registry, request!()).await.map(Some),
                "login-or-register" => accounts::register(&registry, request!()).await.map(Some),
                "found-local-first" => accounts::found(&registry, request!(), app, true)
                    .await
                    .map(Some),
                "link" => {
                    accounts::link(&registry, request!(), (js_sys::Date::now() / 1000.0) as u64)
                        .await
                        .map(Some)
                }
                "revoke" => accounts::revoke(&registry, request!()).await.map(|_| None),
                _ => Err((404, "not_found")),
            }
        }
        .await;
        // Recheck proof-authenticated sockets after mutations, including ambiguous
        // storage failures. Backend-stated identities retain native semantics.
        if (operation == "revoke" && result.is_ok()) || matches!(result, Err((503, _))) {
            self.guests.borrow_mut().retain(|guest| {
                let GuestSession {
                    identity,
                    input,
                    revoked,
                } = guest;
                if input.is_closed() {
                    return false;
                }
                let (issuer, subject) = identity.principal_parts();
                if !registry
                    .lookup(&jazz::account_registry::Principal { issuer, subject })
                    .is_ok_and(|a| Some(a.account) == identity.account_id())
                {
                    revoked.set(true);
                    input.close();
                    self.wake.wake_by_ref();
                    return false;
                }
                true
            });
        }
        Ok(match result {
            Ok(Some(body)) => serde_json::json!({"status":200,"body":body}),
            Ok(None) => serde_json::json!({"status":204}),
            Err((status, message)) => serde_json::json!({"status":status,"error":message}),
        }
        .to_string())
    }

    pub async fn tick(&self) -> Result<(), JsValue> {
        let core = self
            .core
            .borrow_mut()
            .take()
            .ok_or_else(|| error("server core is already ticking"))?;
        let mut running = Running {
            slot: &self.core,
            core: Some(core),
        };
        let core = running.core.as_mut().unwrap();
        core.advance().await;
        core.flush();
        let again = core.inbound_pending();
        drop(running);
        if again {
            self.wake.wake_by_ref();
        }
        Ok(())
    }
}

impl WasmServerCore {
    /// Open an already authenticated session. Raw framing is for embedding hosts.
    pub fn open_session(
        &self,
        author: jazz::ids::AuthorSubject,
        claims: BTreeMap<String, jazz::records::Value>,
        bundle: Option<BundleId>,
        batched: bool,
    ) -> Result<ServerTransport, String> {
        self.open_transport(author, claims, bundle, batched, false)
    }

    fn open_transport(
        &self,
        author: jazz::ids::AuthorSubject,
        claims: BTreeMap<String, jazz::records::Value>,
        bundle: Option<BundleId>,
        batched: bool,
        thin: bool,
    ) -> Result<ServerTransport, String> {
        let mut slot = self.core.borrow_mut();
        let core = slot
            .as_mut()
            .ok_or_else(|| "admit sessions between ticks".to_owned())?;
        let id = self.next_session.get();
        self.next_session.set(
            id.checked_add(1)
                .ok_or_else(|| "session ids exhausted".to_owned())?,
        );
        let session = ServerSession::from_host_id(id);
        let (sender, receiver) = output::unbounded_channel();
        let egress = Arc::new(AtomicUsize::new(0));
        if thin {
            core.open_thin_session(session, author, claims, bundle, sender, Arc::clone(&egress))
        } else {
            core.open_session(session, author, claims, bundle, sender, Arc::clone(&egress))
        }
        .map_err(|e| e.to_string())?;
        self.wake.wake_by_ref();
        Ok(ServerTransport {
            input: core.input(session),
            receiver,
            egress,
            wake: self.wake.clone(),
            closed: false,
            revoked: Rc::new(Cell::new(false)),
            batched,
        })
    }
}

type Outbound = (Result<Vec<Vec<u8>>, jazz::wire::WireError>, usize);
#[wasm_bindgen]
pub struct ServerTransport {
    input: Option<SessionInput>,
    receiver: output::UnboundedReceiver<Outbound>,
    egress: Arc<AtomicUsize>,
    wake: Waker,
    closed: bool,
    // Revocation suppresses already queued output before the next owner tick.
    revoked: Rc<Cell<bool>>,
    batched: bool,
}
#[wasm_bindgen]
impl ServerTransport {
    #[wasm_bindgen(js_name = sendWireFrame)]
    pub fn send_wire_frame(&self, frame: Vec<u8>) -> Result<(), JsValue> {
        if self.revoked.get() {
            return Err(error("account identity revoked"));
        }
        self.input
            .as_ref()
            .ok_or_else(|| error("session is closed"))?
            .receive(if self.batched {
                websocket::decode_frame_batch(&frame).map_err(error)?
            } else {
                vec![frame]
            })
            .map_err(error)?;
        self.wake.wake_by_ref();
        Ok(())
    }
    #[wasm_bindgen(js_name = recvWireFrames)]
    pub fn recv_wire_frames(&mut self) -> Result<js_sys::Array, JsValue> {
        let frames = js_sys::Array::new();
        if self.revoked.get() {
            if !self.closed {
                let refusal = jazz::wire::encode_frame(&WireFrame::Error(WireError::new(
                    WireErrorCode::AuthFailed,
                    WireRetry::Never,
                    "account identity revoked",
                )))
                .map_err(error)?;
                for batch in websocket::encode_frame_batches(&[refusal]).map_err(error)? {
                    frames.push(&js_sys::Uint8Array::from(batch.as_slice()));
                }
            }
            self.close();
            return Ok(frames);
        }
        while let Ok((result, bytes)) = self.receiver.try_recv() {
            self.egress.fetch_sub(bytes, Ordering::AcqRel);
            let encoded = match result {
                Ok(frames) => frames,
                Err(failure) if self.batched => {
                    vec![jazz::wire::encode_frame(&WireFrame::Error(failure)).map_err(error)?]
                }
                Err(failure) => {
                    return Err(error(format!("{:?}: {}", failure.code, failure.message)))
                }
            };
            let encoded = if self.batched {
                websocket::encode_frame_batches(&encoded).map_err(error)?
            } else {
                encoded
            };
            for frame in encoded {
                frames.push(&js_sys::Uint8Array::from(frame.as_slice()));
            }
            // Reading releases backpressure; held output may now be drainable.
            self.wake.wake_by_ref();
        }
        Ok(frames)
    }
    #[wasm_bindgen(js_name = isClosed)]
    pub fn is_closed(&self) -> bool {
        self.closed || self.revoked.get() || self.receiver.is_closed()
    }
    pub fn close(&mut self) {
        if let Some(input) = self.input.take() {
            input.close();
        }
        self.receiver.close();
        self.closed = true;
        self.wake.wake_by_ref();
    }
}
impl Drop for ServerTransport {
    fn drop(&mut self) {
        self.close();
    }
}

#[wasm_bindgen]
pub struct SocketAdmission {
    reply: Vec<u8>,
    transport: Option<ServerTransport>,
}
impl SocketAdmission {
    fn reply(frame: Vec<u8>, transport: Option<ServerTransport>) -> Result<Self, JsValue> {
        let mut batches = websocket::encode_frame_batches(&[frame]).map_err(error)?;
        Ok(Self {
            reply: batches.remove(0),
            transport,
        })
    }
}
#[wasm_bindgen]
impl SocketAdmission {
    pub fn response(&self) -> Vec<u8> {
        self.reply.clone()
    }
    #[wasm_bindgen(js_name = takeTransport)]
    pub fn take_transport(&mut self) -> Option<ServerTransport> {
        self.transport.take()
    }
}

#[wasm_bindgen]
impl WasmServerCore {
    /// Normal WebSocket carrier with a host-configured backend credential.
    /// Only backend-stated user sessions are currently supported.
    #[wasm_bindgen(js_name = acceptSocket)]
    pub fn accept_socket(
        &self,
        prelude: &[u8],
        hello: &[u8],
        backend_secret: Option<String>,
    ) -> Result<SocketAdmission, JsValue> {
        self.finish_socket(hello, || {
            websocket::admit_backend_session(
                websocket::Prelude::decode(prelude)?,
                backend_secret.as_deref(),
            )
        })
    }

    /// Upgrade credentials come from the host's request headers, not the prelude.
    #[wasm_bindgen(js_name = acceptRemoteSocket)]
    pub fn accept_remote_socket(
        &self,
        prelude: &[u8],
        hello: &[u8],
        credential: &str,
        access: &str,
        expected: &str,
    ) -> Result<SocketAdmission, JsValue> {
        let guest = RefCell::new(None);
        let result = self.finish_socket(hello, || {
            let prelude = websocket::Prelude::decode(prelude)?;
            if access.is_empty()
                || (access == "backend"
                    && prelude.auth.backend_session.is_none()
                    && prelude.auth.jwt_token.is_some())
            {
                let slot = self.core.borrow();
                let core = slot.as_ref().ok_or_else(|| {
                    WireError::new(
                        WireErrorCode::NotReady,
                        WireRetry::Later,
                        "server core is busy",
                    )
                })?;
                let admitted = websocket::admit_remote_guest(
                    prelude,
                    credential,
                    access,
                    expected,
                    self.app.as_deref().unwrap_or(""),
                    &core.accounts(),
                    (js_sys::Date::now() / 1000.0) as u64,
                )?;
                *guest.borrow_mut() = Some(admitted.identity);
                Ok(admitted)
            } else {
                websocket::admit_remote_session(prelude, credential, access, expected)
            }
        })?;
        if let (Some(identity), Some(transport)) = (guest.into_inner(), result.transport.as_ref()) {
            let mut guests = self.guests.borrow_mut();
            guests.retain(|guest| !guest.input.is_closed());
            if let Some(input) = &transport.input {
                guests.push(GuestSession {
                    identity,
                    input: input.clone(),
                    revoked: Rc::clone(&transport.revoked),
                });
            }
        }
        Ok(result)
    }
}

impl WasmServerCore {
    fn finish_socket(
        &self,
        hello: &[u8],
        admission: impl FnOnce() -> Result<websocket::BackendSession, WireError>,
    ) -> Result<SocketAdmission, JsValue> {
        if let Some(refusal) = websocket::foreign_version_refusal(hello) {
            return SocketAdmission::reply(refusal, None);
        }
        let admit = || -> Result<(ServerTransport, Vec<u8>), WireError> {
            let admitted = admission()?;
            let remote = websocket::decode_hello(hello)?;
            let negotiated = jazz::wire::negotiate_wire(
                &remote,
                jazz::wire::current_wire_features()
                    | jazz::wire::FEATURE_THIN_RESULTS
                    | jazz::wire::FEATURE_THIN_BACKEND_SCOPE_V1,
            )?;
            let thin = remote.features & jazz::wire::FEATURE_THIN_RESULTS != 0;
            let endpoint = jazz::wire::WireAuthorityEndpoint::fresh(
                jazz::ids::NodeUuid::from_bytes([0x5e; 16]),
            );
            let response = jazz::wire::encode_frame(&WireFrame::Hello(
                WireHello::current(WirePeerRole::Core, negotiated.features)
                    .with_authority(endpoint.node, endpoint.epoch),
            ))
            .map_err(|e| {
                WireError::new(WireErrorCode::Internal, WireRetry::Never, e.to_string())
            })?;
            let transport = self
                .open_transport(
                    admitted.identity,
                    admitted.claims,
                    admitted.bundle,
                    true,
                    thin,
                )
                .map_err(|e| WireError::new(WireErrorCode::AuthFailed, WireRetry::Never, e))?;
            Ok((transport, response))
        };
        match admit() {
            Ok((transport, response)) => SocketAdmission::reply(response, Some(transport)),
            Err(failure) => SocketAdmission::reply(
                jazz::wire::encode_frame(&WireFrame::Error(failure)).map_err(error)?,
                None,
            ),
        }
    }
}
