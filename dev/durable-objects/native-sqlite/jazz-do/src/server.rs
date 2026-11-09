//! Local workerd harness adapter. Identities and fixture schemas are supplied
//! by the test client; production authentication is deliberately outside it.
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Wake, Waker};

use futures_channel::mpsc;
use futures_util::StreamExt;
use jazz::binding_host::{self, OpenAs};
use jazz::catalogue::{BundleId, CatalogueChange, CataloguePut, bundle_id};
use jazz::db::Db;
use jazz::host::{HostTables, StoreFile};
use jazz_server::runtime::{ServerCore, ServerSession, SessionInput, catalogue_schema};
use jazz_wasm::{ObjectStorage, durable_object_storage};
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
    bundle: BundleId,
    wake: Waker,
    #[cfg(feature = "scope-tests")]
    checks: Db,
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

#[wasm_bindgen]
impl WasmServerCore {
    #[wasm_bindgen(js_name = openDurableObject)]
    pub async fn open(
        storage: ObjectStorage,
        schema: Vec<u8>,
        config: Vec<u8>,
        schedule: js_sys::Function,
    ) -> Result<WasmServerCore, JsValue> {
        let (schema, mut config) = binding_host::decode_open(&schema, &config).map_err(error)?;
        let (identity, _) =
            binding_host::open_identity(&mut config, OpenAs::Backend).map_err(error)?;
        let db_config = jazz::tools::deployment::published::with_client_migrations(
            binding_host::db_config(schema, &config, identity),
            config.migrations.as_deref(),
        )
        .map_err(error)?;
        let db = Db::open_storage(db_config, durable_object_storage(storage))
            .await
            .map_err(error)?;
        let mut source = db.current_schema().public_schema().clone();
        let permissions = source
            .iter()
            .map(|(name, table)| (*name, table.policies.clone()))
            .collect();
        for table in source.values_mut() {
            table.policies = Default::default();
        }
        let hash = jazz::tools::public_schema::SchemaHash::compute(&source);
        let bundle = bundle_id(&hash, &permissions).map_err(error)?;
        Self::host(
            db,
            bundle,
            schedule,
            Some(CatalogueChange {
                put: CataloguePut {
                    schemas: vec![(source, 0)],
                    bundles: vec![(hash, permissions, 0)],
                    ..Default::default()
                },
                ..Default::default()
            }),
        )
        .await
    }

    /// Reconstruct the fixture authority without accepting or publishing schema
    /// or permissions from the client. Production hosts select a bundle during
    /// authenticated admission; this harness requires exactly one stored bundle.
    #[wasm_bindgen(js_name = reopenDurableObject)]
    pub async fn reopen(
        storage: ObjectStorage,
        config: Vec<u8>,
        schedule: js_sys::Function,
    ) -> Result<WasmServerCore, JsValue> {
        let store = StoreFile::from_storage(durable_object_storage(storage)).map_err(error)?;
        let rows = store.catalogue().map_err(error)?;
        let schema = catalogue_schema(&rows)
            .map_err(error)?
            .ok_or_else(|| error("cannot reopen: no admitted catalogue schema"))?;
        let [bundle] = rows.bundles.as_slice() else {
            return Err(error(
                "fixture reopen requires exactly one stored permissions bundle",
            ));
        };
        let bundle = bundle.id;
        let schema = serde_json::to_vec(schema.public_schema()).map_err(error)?;
        let (schema, mut config) = binding_host::decode_open(&schema, &config).map_err(error)?;
        if config.migrations.is_some() {
            return Err(error(
                "fixture reopen loads migrations from the catalogue, not the client",
            ));
        }
        let (identity, _) =
            binding_host::open_identity(&mut config, OpenAs::Backend).map_err(error)?;
        let db = Db::open_file(binding_host::db_config(schema, &config, identity), store)
            .await
            .map_err(error)?;
        Self::host(db, bundle, schedule, None).await
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

    #[wasm_bindgen(js_name = acceptSubscriber)]
    pub fn accept_subscriber(&self, author: Vec<u8>) -> Result<ServerTransport, JsValue> {
        let author = jazz::binding_args::author_subject(&author).map_err(error)?;
        let mut slot = self.core.borrow_mut();
        let core = slot
            .as_mut()
            .ok_or_else(|| error("admit sessions between ticks"))?;
        let id = self.next_session.get();
        self.next_session.set(
            id.checked_add(1)
                .ok_or_else(|| error("session ids exhausted"))?,
        );
        let session = ServerSession::from_host_id(id);
        let (sender, receiver) = output::unbounded_channel();
        let egress = Arc::new(AtomicUsize::new(0));
        core.open_session(
            session,
            author,
            BTreeMap::new(),
            Some(self.bundle),
            sender,
            Arc::clone(&egress),
        )
        .map_err(error)?;
        self.wake.wake_by_ref();
        Ok(ServerTransport {
            input: core.input(session),
            receiver,
            egress,
            wake: self.wake.clone(),
            closed: false,
        })
    }

    #[cfg(feature = "scope-tests")]
    #[wasm_bindgen(js_name = checkRejectedRow)]
    pub fn check_rejected_row(&self, row: Vec<u8>, table: Option<String>) -> Result<(), JsValue> {
        if self.core.borrow().is_none() {
            return Err(error("checks require an idle core"));
        }
        let row =
            jazz::ids::RowUuid::from_bytes(row.try_into().map_err(|_| error("expected row UUID"))?);
        self.checks
            .check_rejected_row_for_test(table.as_deref().unwrap_or("tasks"), row)
            .map_err(error)
    }

    #[cfg(feature = "scope-tests")]
    #[wasm_bindgen(js_name = checkSqlScopes)]
    pub fn check_sql_scopes(&self) -> Result<(), JsValue> {
        if self.core.borrow().is_none() {
            return Err(error("checks require an idle core"));
        }
        jazz_wasm::check_durable_object_scopes(&self.checks)
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
}
#[wasm_bindgen]
impl ServerTransport {
    #[wasm_bindgen(js_name = sendWireFrame)]
    pub fn send_wire_frame(&self, frame: Vec<u8>) -> Result<(), JsValue> {
        self.input
            .as_ref()
            .ok_or_else(|| error("session is closed"))?
            .receive(vec![frame])
            .map_err(error)?;
        self.wake.wake_by_ref();
        Ok(())
    }
    #[wasm_bindgen(js_name = recvWireFrames)]
    pub fn recv_wire_frames(&mut self) -> Result<js_sys::Array, JsValue> {
        let frames = js_sys::Array::new();
        while let Ok((result, bytes)) = self.receiver.try_recv() {
            self.egress.fetch_sub(bytes, Ordering::AcqRel);
            for frame in result
                .map_err(|failure| error(format!("{:?}: {}", failure.code, failure.message)))?
            {
                frames.push(&js_sys::Uint8Array::from(frame.as_slice()));
            }
            // Reading releases backpressure; held output may now be drainable.
            self.wake.wake_by_ref();
        }
        Ok(frames)
    }
    #[wasm_bindgen(js_name = isClosed)]
    pub fn is_closed(&self) -> bool {
        self.closed || self.receiver.is_closed()
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

impl WasmServerCore {
    async fn host(
        db: Db,
        bundle: BundleId,
        schedule: js_sys::Function,
        publish: Option<CatalogueChange>,
    ) -> Result<Self, JsValue> {
        #[cfg(feature = "scope-tests")]
        let checks = db
            .governed_view(db.current_schema().version_id())
            .ok_or_else(|| error("missing fixture schema view"))?;
        let wake = scheduler(schedule);
        let mut core = ServerCore::from_db(db, None, wake.clone()).map_err(error)?;
        if let Some(change) = publish {
            let (_, published) = core.apply_catalogue(change, None).await;
            published.map_err(error)?;
        }
        Ok(Self {
            core: Rc::new(RefCell::new(Some(core))),
            next_session: Cell::new(1),
            bundle,
            wake,
            #[cfg(feature = "scope-tests")]
            checks,
        })
    }
}
