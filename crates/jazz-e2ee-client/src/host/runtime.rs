//! End-to-end encryption shared by the hosts: the runtime for a database whose
//! app declares encryption, its private store, and what it does between JS
//! calls (`E2EE_PROTOCOL.md` §4, §6; core hooks note H6, H9).
//!
//! The runtime's futures are polled by the host (a noop waker, after every
//! tick), like the host's own promises. One long-lived future, [`main_loop`],
//! verifies the account, holds the key watches open and runs the natural
//! moments of the delivery pass; calls from JS ([`Encryption::call`]) are
//! further futures settled as promises.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use crate::state::{RETRY_FIRST, RETRY_LAST};
use crate::{
    AccountStatus, ClientConfig, ClientError, DeviceStatus, E2eeClient, PendingApproval,
    PrivateStore, Report,
};
use futures::future::LocalBoxFuture;
use jazz::db::{Db, ErrorCode};
use jazz::e2ee::SpaceAddress;
use jazz::tools::public_schema::E2eeDeclarations;
use jazz_e2ee::OsRandom;
use jazz_e2ee::protocol::derive::RecoverySecret;

use super::bridge::{JsStore, StoreWake};
use serde_json::{Value, json};
use zeroize::Zeroizing;

/// A JavaScript host builds its client before opening the database. Once
/// attached, the store bridge drives the same runtime through a weak reference.
pub struct RuntimeBuilder {
    pub app_id: String,
    pub add_creator_as_member: Cell<bool>,
    account_id: String,
    environment: String,
    options: Rc<RefCell<Option<BuildOptions>>>,
    building: BuildSlot,
    built: Rc<RefCell<Option<Built>>>,
    store: Rc<JsStore>,
    attached: RefCell<std::rc::Weak<Encryption>>,
}

struct BuildOptions {
    bindings: String,
    secret: Option<RecoverySecret>,
}

impl RuntimeBuilder {
    pub fn from_bindings(
        bindings: String,
        app_id: String,
        environment: String,
        account_id: String,
        device_account_secret: Option<String>,
    ) -> Result<Self, String> {
        let secret = device_account_secret
            .map(jazz::identity::DeviceAccountSecret::parse_owned)
            .transpose()
            .map_err(|error| error.to_string())?;
        Ok(Self {
            app_id,
            add_creator_as_member: Cell::new(true),
            environment,
            account_id,
            options: Rc::new(RefCell::new(Some(BuildOptions { bindings, secret }))),
            building: BuildSlot::default(),
            built: Rc::default(),
            store: Rc::default(),
            attached: RefCell::default(),
        })
    }

    pub fn build_future(&self) -> Option<JsonFuture> {
        self.start_build().ok()
    }

    /// The build `ready()` drives, or why `ready()` is refused: the runtime
    /// is built ([`ALREADY_BUILT`]), or an earlier build has not settled
    /// ([`STILL_BUILDING`]).
    pub fn start_build(&self) -> Result<JsonFuture, &'static str> {
        let options = self.options.borrow();
        let source = options.as_ref().ok_or(ALREADY_BUILT)?;
        let held = self.building.take()?;
        let declarations = bound_declarations(
            source.bindings.as_bytes(),
            &self.app_id,
            &self.environment,
            false,
        )
        .and_then(|(_, declarations)| {
            declarations.ok_or_else(|| "the app's files declare no encryption".to_owned())
        });
        let recovery = source.secret.clone();
        let account = self.account_id.clone();
        let store = self.store.clone();
        let options = Rc::downgrade(&self.options);
        let slot = self.built.clone();
        Ok(Box::pin(async move {
            let result = match declarations {
                Ok(declarations) => {
                    build_client(declarations, account, store, recovery.as_ref()).await
                }
                Err(error) => Err(error),
            };
            drop(held);
            let built = result?;
            if let Some(options) = options.upgrade() {
                options.borrow_mut().take();
            }
            *slot.borrow_mut() = Some(built);
            Ok(Value::Null)
        }))
    }

    pub fn hooks(
        &self,
        identity: jazz::db::DbIdentity,
    ) -> Result<Rc<dyn jazz::e2ee::E2eeHooks>, &'static str> {
        let slot = self.built.borrow();
        let built = slot.as_ref().ok_or("the encryption runtime is not ready")?;
        if identity
            .author
            .account_id()
            .map(|id| id.0.to_string())
            .as_deref()
            != Some(&self.account_id)
        {
            return Err("the encryption runtime is for another account than this session's");
        }
        Ok(built.client.hooks())
    }

    /// Called after a successful open, so a failed open leaves the built client available.
    pub fn attach(&self, db: Rc<Db>, wake: Box<dyn Fn()>) -> Rc<Encryption> {
        let built = self
            .built
            .borrow_mut()
            .take()
            .expect("runtime checked before open");
        self.store.set_wake(wake);
        let runtime = Encryption::new(built, self.store.clone(), db);
        self.attached.replace(Rc::downgrade(&runtime));
        runtime
    }

    pub fn set_store_wake(&self, wake: Option<Box<dyn Fn(StoreWake)>>) {
        self.store.set_store_wake(wake);
    }

    pub fn take_requests(&self) -> Vec<String> {
        self.store.take_requests()
    }

    pub fn reply(&self, id: &str, ok: bool, payload: &str) -> bool {
        id.parse().is_ok_and(|id| self.store.reply(id, ok, payload))
    }

    pub fn drive(&self, settle: impl FnOnce()) {
        let announced = self.store.announced_deadline();
        self.store.check_clock();
        let runtime = self.attached.borrow().upgrade();
        if let Some(runtime) = &runtime {
            runtime.drive();
        }
        settle();
        self.store.refresh_deadline_after_poll(announced);
        if let Some(runtime) = runtime {
            runtime.wake_if_owed();
        }
    }
}

/// The built runtime and its reports.
pub struct Built {
    /// Where each encrypted table's rows keep their space: `{ table: { scope,
    /// column } }`, as JSON, for the host to watch the spaces in use.
    pub spaces: Value,
    pub client: Rc<E2eeClient>,
    pub reports: Rc<RefCell<ReportQueue>>,
}

/// The most reports kept for JS between two takes.
pub const MAX_REPORTS: usize = 1024;

/// The reports waiting for JS, as JSON texts: at most [`MAX_REPORTS`]. An
/// app that reads none (no listener) holds no more than that; past it the
/// oldest ordinary report goes first. If only challenges are waiting, a
/// new challenge replaces the oldest and an ordinary report is dropped.
/// The next take reports the number dropped.
#[derive(Debug, Default)]
pub struct ReportQueue {
    /// Each report, and whether it is a challenge.
    waiting: VecDeque<(bool, String)>,
    dropped: u64,
}

impl ReportQueue {
    pub fn push(&mut self, report: Value) {
        let challenge = report["type"] == "challenged";
        if self.waiting.len() >= MAX_REPORTS {
            self.dropped += 1;
            match self.waiting.iter().position(|(challenge, _)| !challenge) {
                Some(oldest) => {
                    self.waiting.remove(oldest);
                }
                None if challenge => {
                    self.waiting.pop_front();
                }
                None => return,
            }
        }
        self.waiting.push_back((challenge, report.to_string()));
    }

    /// The reports waiting, oldest first, and the count of those dropped.
    pub fn take(&mut self) -> Vec<String> {
        let dropped = std::mem::take(&mut self.dropped);
        let dropped = (dropped > 0)
            .then(|| json!({ "type": "reports-dropped", "count": dropped }).to_string());
        dropped
            .into_iter()
            .chain(self.waiting.drain(..).map(|(_, report)| report))
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.waiting.is_empty() && self.dropped == 0
    }
}

/// A native device runtime and its account-secret recovery configuration.
pub struct NativeBuilt {
    built: Built,
    recovery: Option<RecoverySecret>,
}

impl NativeBuilt {
    pub fn client(&self) -> &E2eeClient {
        &self.built.client
    }
}

struct NativeReadiness {
    recovery: RefCell<Option<RecoverySecret>>,
    result: RefCell<Verification>,
    observed_initial: Cell<bool>,
    requested: Cell<u64>,
    started: Cell<u64>,
    finished: Cell<u64>,
}

enum Verification {
    Pending,
    AwaitingRecovery,
    Retry(ClientError),
    Complete,
    Failed(ClientError),
}

/// Whether the host can repeat a refused write, and whether this is its final attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteRetry {
    Retry,
    Last,
    Stop,
}

/// The runtime and what the host keeps beside it.
pub struct Encryption {
    pub client: Rc<E2eeClient>,
    bridge_store: Option<Rc<JsStore>>,
    wake: Rc<dyn Fn()>,
    db: Rc<Db>,
    /// The reports of the runtime, as JSON, waiting for JS.
    reports: Rc<RefCell<ReportQueue>>,
    /// Approvals this device started, by the device they are for.
    approvals: RefCell<BTreeMap<String, PendingApproval>>,
    /// Space watches the app asked to open or close, applied by the loop:
    /// whether to open, the space table's name and the space's row.
    uses: RefCell<VecDeque<(bool, String, String)>>,
    /// Calls that need the loop's space watch, answered by the loop.
    commands: RefCell<VecDeque<Command>>,
    /// Where each encrypted table keeps its space (see [`Built::spaces`]).
    pub spaces: Value,
    /// Set when the connection came back: the loop verifies and runs the
    /// delivery pass again once caught up.
    reconnected: Cell<bool>,
    status: Cell<Option<AccountStatus>>,
    /// Whether the client knows the catalogue identities, so it can open
    /// the encrypted rows its store holds with the keys it holds (loaded
    /// before the loop starts), with or without the server.
    readable: Cell<bool>,
    native: Option<NativeReadiness>,
    last_error: RefCell<Option<String>>,
    /// What the loop is doing or waiting on, for a call that waits on it.
    stage: Cell<&'static str>,
    /// The send batch for which a wake was last requested.
    woken_batch: Cell<Option<u64>>,
    preparations: super::PromiseQueue,
    gate: Gate,
    /// The loop, polled by the host.
    main: RefCell<Option<LocalBoxFuture<'static, ()>>>,
    #[cfg(any(test, feature = "testing"))]
    approval_watch_fault: Cell<Option<ApprovalWatchFault>>,
}

#[cfg(any(test, feature = "testing"))]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ApprovalWatchFault {
    /// The next approval watch fails to open.
    Install,
    /// The approval watch's challenge stream closes once.
    Closed,
    /// The approval watch stays open but never reports a change, for as
    /// long as the fault is set.
    Stalled,
    /// The next authoritative challenge read that finds a challenge
    /// returns `false` instead, as a read that ran ahead of the challenge
    /// would.
    EarlyFalse,
}

/// One runtime call at a time: two calls that each hold a database
/// transaction open across awaits would lock one another out (the store is
/// busy), so the loop and the calls from JavaScript take turns.
struct Gate {
    held: Cell<bool>,
    waiting: Cell<bool>,
    wake: Rc<dyn Fn()>,
}

struct Turn<'a>(&'a Gate);

impl Drop for Turn<'_> {
    fn drop(&mut self) {
        self.0.held.set(false);
        if self.0.waiting.replace(false) {
            (self.0.wake)();
        }
    }
}

impl Gate {
    async fn enter(&self) -> Turn<'_> {
        std::future::poll_fn(|_| {
            if self.held.replace(true) {
                self.waiting.set(true);
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
        Turn(self)
    }
}

/// A call that runs inside the loop, which owns the space watch: the
/// delivery pass over every space this device holds keys of, answered here.
type Command = futures::channel::oneshot::Sender<Result<Value, String>>;

/// The refusal of `ready()` once the runtime is built.
pub const ALREADY_BUILT: &str = "the runtime is already built";
/// The refusal of `ready()` while an earlier build has not settled.
pub const STILL_BUILDING: &str = "the runtime is still being built: an earlier `ready()` is waiting for the private store to answer its requests";

/// The one build a host runs at a time. A build holds it until its future
/// settles or is dropped, so the slot frees on failure, on success and when
/// the build is abandoned; a later `ready()` is refused with
/// [`STILL_BUILDING`] only while a build is really running.
#[derive(Clone, Default)]
pub struct BuildSlot(Rc<Cell<bool>>);

impl BuildSlot {
    /// Takes the slot for one build, or refuses while another holds it.
    pub fn take(&self) -> Result<BuildHeld, &'static str> {
        if self.0.replace(true) {
            return Err(STILL_BUILDING);
        }
        Ok(BuildHeld(Rc::clone(&self.0)))
    }
}

/// A build's hold on its [`BuildSlot`]; dropping it frees the slot.
pub struct BuildHeld(Rc<Cell<bool>>);

impl Drop for BuildHeld {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

/// Build the native device from the declarations admitted for its database.
pub async fn build_native(
    declarations: E2eeDeclarations,
    account_id: String,
    store: Rc<dyn PrivateStore>,
    recovery: Option<RecoverySecret>,
) -> Result<NativeBuilt, String> {
    let built = build_client(declarations, account_id, store, recovery.as_ref()).await?;
    Ok(NativeBuilt { built, recovery })
}

/// Bind the compiled app to its deployment. Relay admission also verifies the
/// shipped schema against its hash before comparing it to the delivered one.
pub fn bound_declarations(
    bindings: &[u8],
    app_id: &str,
    environment: &str,
    verify_current_hash: bool,
) -> Result<
    (
        jazz::tools::public_schema::SchemaHash,
        Option<E2eeDeclarations>,
    ),
    String,
> {
    jazz::tools::deployment::artifact::bound_declarations(
        bindings,
        jazz::tools::AppId::from_text(app_id),
        environment,
        verify_current_hash,
    )
    .map_err(|error| error.to_string())
}

pub fn keyless_declarations(
    bindings: &str,
    app_id: &str,
    environment: &str,
) -> Result<E2eeDeclarations, String> {
    bound_declarations(bindings.as_bytes(), app_id, environment, false)?
        .1
        .ok_or_else(|| "the app's bindings declare no encryption".to_owned())
}

async fn build_client(
    declarations: E2eeDeclarations,
    account_id: String,
    store: Rc<dyn PrivateStore>,
    recovery: Option<&RecoverySecret>,
) -> Result<Built, String> {
    let spaces = Value::Object(
        declarations
            .tables
            .iter()
            .map(|(table, entry)| {
                (
                    table.clone(),
                    json!({ "scope": entry.scope, "column": entry.space }),
                )
            })
            .collect(),
    );
    let client = E2eeClient::open(ClientConfig {
        store,
        declarations,
        account_id,
        registry: jazz_e2ee::registry::Registry::builtin(),
        random: Box::new(OsRandom),
        device_account: recovery,
    })
    .await
    .map_err(|error| error.to_string())?;
    let client = Rc::new(client);
    let reports = Rc::new(RefCell::new(ReportQueue::default()));
    let sink = Rc::clone(&reports);
    let shared = Rc::downgrade(&client.shared);
    client.on_report(move |report| {
        sink.borrow_mut().push(report_json(report, |scope| {
            shared
                .upgrade()?
                .keys
                .borrow()
                .catalogue
                .as_ref()?
                .table_names
                .get(scope)
                .cloned()
        }));
    });
    Ok(Built {
        spaces,
        client,
        reports,
    })
}

impl Encryption {
    pub fn new(built: Built, store: Rc<JsStore>, db: Rc<Db>) -> Rc<Self> {
        let bridge = Rc::clone(&store);
        Self::start(
            built,
            Some(store),
            db,
            Rc::new(move || bridge.request_wake()),
            None,
        )
    }

    pub fn new_native(
        built: NativeBuilt,
        bridge_store: Option<Rc<JsStore>>,
        db: Rc<Db>,
        wake: Rc<dyn Fn()>,
    ) -> Rc<Self> {
        let native = NativeReadiness {
            recovery: RefCell::new(built.recovery),
            result: RefCell::new(Verification::Pending),
            observed_initial: Cell::new(false),
            requested: Cell::new(0),
            started: Cell::new(0),
            finished: Cell::new(0),
        };
        Self::start(built.built, bridge_store, db, wake, Some(native))
    }

    fn start(
        built: Built,
        bridge_store: Option<Rc<JsStore>>,
        db: Rc<Db>,
        wake: Rc<dyn Fn()>,
        native: Option<NativeReadiness>,
    ) -> Rc<Self> {
        let Built {
            spaces,
            client,
            reports,
        } = built;
        let encryption = Rc::new(Self {
            client,
            bridge_store,
            gate: Gate {
                held: Cell::new(false),
                waiting: Cell::new(false),
                wake: Rc::clone(&wake),
            },
            wake,
            db,
            reports,
            approvals: RefCell::default(),
            uses: RefCell::default(),
            commands: RefCell::default(),
            spaces,
            reconnected: Cell::new(false),
            status: Cell::new(None),
            readable: Cell::new(false),
            native,
            last_error: RefCell::default(),
            stage: Cell::new("starting"),
            woken_batch: Cell::new(None),
            preparations: Default::default(),
            main: RefCell::default(),
            #[cfg(any(test, feature = "testing"))]
            approval_watch_fault: Cell::default(),
        });
        let running = Rc::clone(&encryption);
        *encryption.main.borrow_mut() = Some(Box::pin(main_loop(running)));
        encryption
    }

    /// The request queue for hosts that supply their store over the bridge.
    pub fn bridge_store(&self) -> &JsStore {
        self.bridge_store
            .as_deref()
            .expect("host uses a store bridge")
    }

    /// Asks the host for a tick when a read the runtime just asked is waiting
    /// to go upstream: its next remote read is asked from a continuation,
    /// after the last one was answered, and nothing else would send it.
    ///
    /// It asks once for each batch that waits: a transport that is attached
    /// but cannot drain (its queue is full, its link is stuck) would
    /// otherwise make every drive ask again at once and spin the host. A new
    /// ask follows only once what waited has gone (a tick sent it).
    pub fn wake_if_owed(&self) {
        let batch = self.db.pending_upstream_send_batch();
        if self.woken_batch.replace(batch) != batch && batch.is_some() {
            (self.wake)();
        }
    }

    /// Lets the loop run as far as it can without waiting.
    pub fn drive(&self) {
        self.preparations.settle();
        let turn_was_held = self.gate.held.get();
        let mut main = self.main.borrow_mut();
        let Some(future) = main.as_mut() else {
            return;
        };
        let mut context = Context::from_waker(Waker::noop());
        if future.as_mut().poll(&mut context).is_ready() {
            *main = None;
        }
        drop(main);
        // A store or sync reply may have let the loop free its turn after
        // the preparations were polled. Give them that turn on this drive.
        if turn_was_held && !self.gate.held.get() {
            self.preparations.settle();
        }
        self.wake_if_owed();
    }

    /// Ends the loop (the host is closing): it holds this runtime, so it
    /// must be dropped for the runtime to be.
    ///
    /// Key preparations still running get one last poll, so one that can
    /// finish does; the rest end with the client, which is closing, and
    /// are dropped because they hold the runtime too.
    pub fn stop(&self) {
        *self.main.borrow_mut() = None;
        self.preparations.settle();
        // After the last poll, which may have asked for one.
        self.set_background_deadline(None);
        self.preparations.0.borrow_mut().clear();
        self.client.close();
        self.approvals.borrow_mut().clear();
        self.commands.borrow_mut().clear();
        if let Some(native) = &self.native {
            native.recovery.borrow_mut().take();
            *native.result.borrow_mut() = Verification::Failed(ClientError::Closed);
        }
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn pending_approval_count(&self) -> usize {
        self.approvals.borrow().len()
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn fail_approval_watch_once(&self, fault: ApprovalWatchFault) {
        self.approval_watch_fault.set(Some(fault));
    }

    /// The reports waiting, as JSON texts.
    pub fn take_reports(&self) -> Vec<String> {
        self.client.flush_reports();
        self.reports.borrow_mut().take()
    }

    /// An app subscription on an encrypted table opened (`true`) or closed
    /// (`false`); `space` is `table` identity and row of the space.
    pub fn use_space(&self, open: bool, table: String, id: String) {
        self.uses.borrow_mut().push_back((open, table, id));
        (self.wake)();
    }

    /// The connection to the server went away (`false`) or came back
    /// (`true`): offline, equality queries match every row of a space; back
    /// online, the loop verifies the account and runs the delivery pass.
    pub fn set_online(&self, online: bool) {
        self.client.set_online(online);
        if online {
            self.reconnected.set(true);
        }
        (self.wake)();
    }

    /// The address of the space `id` of the space table `table`.
    async fn space_address(&self, table: &str, id: &str) -> Result<SpaceAddress, String> {
        let cached = self
            .client
            .shared
            .keys
            .borrow()
            .catalogue
            .as_ref()
            .map(|catalogue| catalogue.table(table).ok().map(str::to_owned));
        let identity = match cached {
            Some(Some(identity)) => identity
                .parse::<uuid::Uuid>()
                .map_err(|error| error.to_string())?,
            Some(None) => return Err(format!("the schema has no table `{table}`")),
            None => {
                self.db
                    .table_identity(table)
                    .await
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| format!("the schema has no table `{table}`"))?
                    .0
            }
        };
        Ok(SpaceAddress {
            scope_id: identity.to_string(),
            identifier: id.to_owned(),
        })
    }

    fn notify_state(&self) {
        if let Some(store) = &self.bridge_store {
            store.notify_store(StoreWake::EncryptionState);
        }
    }

    fn set_status(&self, status: AccountStatus) {
        if self.status.replace(Some(status)) != Some(status) {
            self.notify_state();
        }
    }

    /// The loop's last account status and error, and whether the client
    /// can read encrypted rows yet (`readable`), as JSON.
    pub fn state(&self) -> Value {
        json!({
            "status": self.status.get().map(status_name),
            "readable": self.readable.get(),
            "error": self.last_error.borrow().clone(),
            "stage": self.stage.get(),
        })
    }

    /// Wait for the native loop to verify the account and, for a device
    /// account, register or restore recovery. The loop owns the recovery,
    /// so a caller timing out cannot interrupt a spent-nonce publication.
    pub async fn ready_native(&self, timeout: Duration) -> Result<&'static str, String> {
        let started = web_time::Instant::now();
        loop {
            if let Some(result) = self.native_ready_outcome(started.elapsed() >= timeout) {
                return result;
            }
            yield_now().await;
        }
    }

    /// What a native readiness waiter would receive if its deadline expired
    /// now. A host waiting on a different thread can retain this outcome
    /// while its owner drives recovery.
    pub fn native_ready_at_deadline(&self) -> Result<&'static str, String> {
        self.native_ready_outcome(true)
            .expect("an expired readiness call always has an outcome")
    }

    /// An explicit foreground waiter joins initial recovery, or asks the
    /// loop for a new verification once it has seen the initial outcome.
    /// Waiters for the same explicit pass share its work.
    pub fn request_foreground_ready(&self) -> u64 {
        let Some(native) = &self.native else { return 0 };
        // A terminal preparation error exits the loop before it can
        // take any further passes. Keep returning its recorded outcome.
        if !native.observed_initial.get() || self.main.borrow().is_none() {
            return 0;
        }
        if native.requested.get() <= native.finished.get() {
            native.requested.set(native.started.get() + 1);
        }
        native.requested.get()
    }

    /// Read only an outcome of the requested pass, even if an earlier pass
    /// succeeded. A caller can stop waiting without cancelling recovery.
    pub fn foreground_ready_outcome(
        &self,
        requested: u64,
        expired: bool,
    ) -> Option<Result<&'static str, String>> {
        if let Some(native) = &self.native {
            if native.finished.get() < requested && self.main.borrow().is_some() {
                return expired.then(|| Err(self.readiness_timeout()));
            }
            let result = self.native_ready_outcome(expired);
            if requested == 0 && self.native_ready_outcome(false).is_some() {
                native.observed_initial.set(true);
            }
            return result;
        }
        self.native_ready_outcome(expired)
    }

    fn readiness_timeout(&self) -> String {
        format!(
            "timeout: the account was not verified in time (waiting: {})",
            self.stage.get()
        )
    }

    fn native_ready_outcome(&self, expired: bool) -> Option<Result<&'static str, String>> {
        let Some(native) = &self.native else {
            return Some(Err("the runtime is not native".to_owned()));
        };
        match &*native.result.borrow() {
            Verification::Complete => Some(Ok(status_name(
                self.status.get().expect("the account was verified"),
            ))),
            Verification::Failed(error) => Some(Err(describe(error.clone()))),
            Verification::AwaitingRecovery if expired => Some(Ok("awaiting-approval")),
            Verification::Retry(error) if expired => Some(Err(describe(error.clone()))),
            _ if expired => Some(Err(self.readiness_timeout())),
            _ => None,
        }
    }

    fn set_background_deadline(&self, at: Option<web_time::Instant>) {
        if let Some(store) = &self.bridge_store {
            store.set_background_deadline(at);
        }
    }

    async fn within<F: Future + Unpin>(&self, future: F, limit: Duration) -> Option<F::Output> {
        let deadline = web_time::Instant::now() + limit;
        let _wake = self
            .bridge_store
            .as_ref()
            .map(|store| store.operation_deadline(deadline));
        within_at(future, deadline).await
    }

    /// The same window covers every key a single host write asks for.
    pub fn write_deadline(elapsed: Duration) -> web_time::Instant {
        web_time::Instant::now() + Duration::from_secs(10).saturating_sub(elapsed)
    }

    /// Decide when the host should repeat its refused write. Preparation can
    /// hold a transaction across a store call, so the runtime finishes it
    /// even if this particular write exhausts its deadline.
    pub async fn retry_write(
        self: &Rc<Self>,
        space: &SpaceAddress,
        deadline: web_time::Instant,
        repeated: bool,
    ) -> Result<WriteRetry, String> {
        self.retry_write_with(space, deadline, repeated, |preparation| {
            self.preparations.submit(preparation)
        })
        .await
    }

    /// Run each preparation on the caller's executor. Once started it must
    /// finish even when the write times out; only [`Self::stop`] ends it.
    pub async fn retry_write_with(
        self: &Rc<Self>,
        space: &SpaceAddress,
        deadline: web_time::Instant,
        repeated: bool,
        submit: impl Fn(LocalBoxFuture<'static, ()>),
    ) -> Result<WriteRetry, String> {
        self.retry_until(space, deadline, repeated, || {
            let runtime = self.clone();
            let address = space.clone();
            let (send, answer) = futures::channel::oneshot::channel();
            submit(Box::pin(async move {
                let result = runtime.prepare_host_write(&address, Some(deadline)).await;
                let _ = send.send(result);
            }));
            answer
        })
        .await
    }

    /// The loop of [`Self::retry_write_with`]: `prepare` starts a
    /// preparation and answers whether the write may be tried again (`Err`
    /// when the preparation was dropped unanswered). While it answers "not
    /// yet", ask again every 200 ms until the deadline.
    async fn retry_until<A>(
        &self,
        space: &SpaceAddress,
        deadline: web_time::Instant,
        mut repeated: bool,
        mut prepare: impl FnMut() -> A,
    ) -> Result<WriteRetry, String>
    where
        A: Future<Output = Result<Result<bool, String>, futures::channel::oneshot::Canceled>>
            + Unpin,
    {
        let remaining = || deadline.saturating_duration_since(self.client.shared.now());
        let fallback = |reason: &str| {
            self.client
                .retry_with_current_key(space, reason)
                .map_err(describe)
        };
        loop {
            if repeated {
                self.pause(remaining().min(Duration::from_millis(200)))
                    .await;
            }
            if remaining().is_zero() {
                break;
            }
            match self.within(prepare(), remaining()).await {
                Some(Ok(Ok(true))) => return Ok(WriteRetry::Retry),
                Some(Ok(Ok(false))) => repeated = true,
                Some(Ok(Err(reason))) if fallback(&reason)? => return Ok(WriteRetry::Last),
                Some(Ok(Err(reason))) => return Err(reason),
                Some(Err(_)) => return Err(describe(ClientError::Closed)),
                None => break,
            }
        }
        Ok(if fallback("rotation undecided at the write deadline")? {
            WriteRetry::Last
        } else {
            WriteRetry::Stop
        })
    }

    /// Waits `duration` before a write asks for its key again. A test that
    /// holds the runtime clock ([`E2eeClient::advance_settle_clock`]) has
    /// the wait stepped on that clock instead of slept.
    async fn pause(&self, duration: Duration) {
        #[cfg(any(test, feature = "testing"))]
        if self.client.shared.held_clock.get().is_some() {
            self.client.advance_settle_clock(duration);
            return;
        }
        // Through the runtime, so the host is asked to wake at the end.
        self.within(std::future::pending::<()>(), duration).await;
    }

    /// Prepares a native write for which the device is still missing a key.
    pub async fn prepare_host_write(
        &self,
        space: &SpaceAddress,
        deadline: Option<web_time::Instant>,
    ) -> Result<bool, String> {
        let _turn = self.gate.enter().await;
        let outcome = self
            .client
            .prepare_host_write(&self.db, space, deadline)
            .await;
        if self.native.is_none() {
            return outcome.map_err(describe);
        }
        match outcome {
            Err(ClientError::NoAccount) => match &self.native {
                Some(native) => match &*native.result.borrow() {
                    Verification::Pending
                    | Verification::AwaitingRecovery
                    | Verification::Retry(_) => Ok(false),
                    Verification::Failed(error) => Err(describe(error.clone())),
                    Verification::Complete => Err(describe(ClientError::NoAccount)),
                },
                None => Err(describe(ClientError::NoAccount)),
            },
            outcome => outcome.map_err(describe),
        }
    }

    /// Run one recovery step for the status the host just verified. With no
    /// recovery evidence yet, the host decides whether to wait or return.
    async fn recover_after_verification(
        &self,
        status: AccountStatus,
        secret: &RecoverySecret,
        verify_registration: bool,
    ) -> Result<Option<AccountStatus>, ClientError> {
        match status {
            AccountStatus::Active => {
                // Native startup verifies again around registration. JS returns the
                // status it already verified once the registration succeeds.
                if verify_registration {
                    self.client
                        .start_device_account(&self.db, secret)
                        .await
                        .map(Some)
                } else {
                    self.client.refuse_foreign_device_secret(secret)?;
                    self.client
                        .register_recovery_at_start(&self.db, secret)
                        .await?;
                    Ok(Some(status))
                }
            }
            AccountStatus::AwaitingApproval => {
                match self
                    .client
                    .recover_device_when_available(&self.db, secret)
                    .await
                {
                    Err(ClientError::ApprovalAnswered) => {
                        self.client.ensure_account(&self.db).await.map(Some)
                    }
                    outcome => outcome,
                }
            }
            AccountStatus::Reenrolled => Ok(Some(status)),
        }
    }

    fn record_native_failure(&self, native: &NativeReadiness, error: ClientError) {
        let text = describe(error.clone());
        let (code, message) = text.split_once(": ").expect("a host error has a code");
        self.reports
            .borrow_mut()
            .push(json!({ "type": "ready-failed", "code": code, "message": message }));
        native.recovery.borrow_mut().take();
        *native.result.borrow_mut() = Verification::Failed(error);
    }

    /// One call from JS: the operation's name and its arguments, and its
    /// result as JSON. `recoveryCreate` answers with the recovery phrase,
    /// the secret itself: the runtime zeroizes its own buffers, and the
    /// caller owns the one copy in the answer (a host moves it into a
    /// zeroizing buffer, or hands it on and drops it at once).
    pub fn call(
        self: &Rc<Self>,
        op: String,
        mut args: Value,
    ) -> LocalBoxFuture<'static, Result<Value, String>> {
        let this = Rc::clone(self);
        let secret = args.get_mut("secret").and_then(|value| match value {
            Value::String(text) => Some(Zeroizing::new(std::mem::take(text))),
            _ => None,
        });
        let phrase = match op.as_str() {
            "recoveryRegister" | "recoveryRestore" => take_recovery_phrase(&mut args),
            _ => None,
        };
        Box::pin(async move {
            this.run(
                &op,
                &args,
                phrase,
                secret.as_ref().map(|text| text.as_str()),
            )
            .await
        })
    }

    pub fn call_driven(self: &Rc<Self>, op: String, args: Value) -> JsonFuture {
        let future = self.call(op, args);
        let runtime = self.clone();
        Box::pin(async move {
            let result = future.await;
            runtime.drive();
            result
        })
    }

    async fn run(
        self: &Rc<Self>,
        op: &str,
        args: &Value,
        phrase: Option<Result<RecoverySecret, String>>,
        secret: Option<&str>,
    ) -> Result<Value, String> {
        if op == "shutdown" {
            // The loop can be holding the store lock while waiting for its
            // save to settle. Keep driving it until the client is closed.
            self.client.shutdown().await;
            self.stop();
            return Ok(Value::Null);
        }
        let text = |name: &str| -> Result<String, String> {
            args.get(name)
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| format!("`{name}` is missing"))
        };
        match op {
            "state" => return Ok(self.state()),
            "spaces" => return Ok(self.spaces.clone()),
            "useSpace" => {
                let open = args
                    .get("open")
                    .and_then(Value::as_bool)
                    .ok_or("`open` is missing")?;
                let (table, id) = (text("table")?, text("id")?);
                // Still answered after shutdown (a host's subscriptions
                // close after it), but nothing is queued for a loop that
                // no longer runs.
                if self.client.ensure_open().is_ok() {
                    self.use_space(open, table, id);
                }
                return Ok(Value::Null);
            }
            "setOnline" => {
                let online = args
                    .get("online")
                    .and_then(Value::as_bool)
                    .ok_or("`online` is missing")?;
                self.set_online(online);
                return Ok(Value::Null);
            }
            _ => {}
        }
        self.client.ensure_open().map_err(describe)?;
        if op == "reportsForListener" {
            // Flush and take the queue before sampling the held reports. The
            // reports produced after this call go in the next poll.
            let queued = self.take_reports();
            let keys = self.client.shared.keys.borrow();
            let active: Vec<_> = self
                .client
                .shared
                .held_reports()
                .iter()
                .map(|report| {
                    report_json(report, |scope| {
                        keys.catalogue.as_ref()?.table_names.get(scope).cloned()
                    })
                })
                .collect();
            return Ok(json!({ "active": active, "queued": queued }));
        }
        // The delivery pass belongs to the loop, which takes its turn itself.
        // A space's row is a local catalogue lookup: it never waits for the
        // loop's work.
        let _turn = match op {
            "keysDeliver" | "retryWrite" | "prepareSpace" | "retryWithCurrentKey" | "spaceRow" => {
                None
            }
            _ => Some(self.gate.enter().await),
        };
        let (client, db) = (&self.client, &*self.db);
        client.ensure_open().map_err(describe)?;
        Ok(match op {
            "deviceId" => json!(client.device().map_err(describe)?.id.clone()),
            "ready" => {
                let status = client.ensure_account(db).await.map_err(describe)?;
                self.set_status(status);
                json!(status_name(status))
            }
            "resetAccount" => {
                let status = client.reset_account(db).await.map_err(describe)?;
                self.set_status(status);
                json!(status_name(status))
            }
            "devices" => json!(
                client
                    .devices(db)
                    .await
                    .map_err(describe)?
                    .into_iter()
                    .map(|device| json!({
                        "id": device.id,
                        "removalRequested": device.removal_requested,
                        "status": match device.status {
                            DeviceStatus::Active => "active",
                            DeviceStatus::Waiting => "waiting",
                        },
                    }))
                    .collect::<Vec<_>>()
            ),
            "challenge" => {
                let device = text("deviceId")?;
                let pending = client
                    .challenge_device(db, &device)
                    .await
                    .map_err(describe)?;
                client.ensure_open().map_err(describe)?;
                self.approvals.borrow_mut().insert(device.clone(), pending);
                json!({ "deviceId": device })
            }
            "answer" => json!(client.answer_challenge(db).await.map_err(describe)?),
            "approvalCode" => {
                let device = text("deviceId")?;
                let pending = self.approvals.borrow_mut().remove(&device);
                let pending = pending.ok_or("no approval was started for this device")?;
                let code = client.approval_code_of(db, &pending).await;
                client.ensure_open().map_err(describe)?;
                self.approvals.borrow_mut().insert(device, pending);
                json!(code.map_err(describe)?)
            }
            "approve" => {
                let device = text("deviceId")?;
                let code = text("code")?;
                let pending = self.approvals.borrow_mut().remove(&device);
                let pending = pending.ok_or("no approval was started for this device")?;
                let done = client.approve_device(db, &pending, &code).await;
                if done.is_err() {
                    client.ensure_open().map_err(describe)?;
                    self.approvals.borrow_mut().insert(device, pending);
                }
                done.map_err(describe)?;
                Value::Null
            }
            "remove" => {
                client
                    .remove_device(db, &text("deviceId")?)
                    .await
                    .map_err(describe)?;
                Value::Null
            }
            "requestRemoval" => {
                client.request_removal(db).await.map_err(describe)?;
                Value::Null
            }
            "completeRemovals" => {
                let done = client
                    .complete_removal_requests(db)
                    .await
                    .map_err(describe)?;
                json!(
                    done.into_iter()
                        .map(|(device, outcome)| json!({
                            "deviceId": device,
                            "error": outcome.err().map(describe),
                        }))
                        .collect::<Vec<_>>()
                )
            }
            // A new recovery secret as its 24-word phrase, to keep; nothing is
            // registered.
            // The bytes and the phrase are zeroized here; the answer's one
            // copy belongs to the caller (see `Encryption::call`).
            "recoveryCreate" => {
                let mut bytes = jazz_e2ee::Zeroizing::new([0u8; 32]);
                jazz_e2ee::RandomSource::fill(&mut OsRandom, &mut *bytes)
                    .map_err(|error| error.to_string())?;
                let phrase = super::phrase::from_secret(&bytes);
                json!({ "phrase": phrase.as_str() })
            }
            // Older automatic calls selected registration or restoration by
            // name. A readiness caller now supplies the status it observed.
            "recoveryRegister" | "recoveryRestore"
                if args.get("automatic").and_then(Value::as_bool) == Some(true) =>
            {
                let status = match args.get("verifiedStatus").and_then(Value::as_str) {
                    Some("active") => AccountStatus::Active,
                    Some("awaiting-approval") => AccountStatus::AwaitingApproval,
                    Some(_) => return Err("unknown verified account status".to_owned()),
                    None if op == "recoveryRegister" => AccountStatus::Active,
                    None => AccountStatus::AwaitingApproval,
                };
                let secret = recovery_secret(args, phrase, secret)?;
                let restored = self
                    .recover_after_verification(status, &secret, false)
                    .await
                    .map_err(describe)?;
                if status == AccountStatus::AwaitingApproval
                    && let Some(restored) = restored
                {
                    self.set_status(restored);
                }
                if op == "recoveryRegister" {
                    Value::Null
                } else {
                    json!(restored.map(status_name))
                }
            }
            "recoveryRegister" => {
                let secret = recovery_secret(args, phrase, secret)?;
                client
                    .register_recovery(db, &secret)
                    .await
                    .map_err(describe)?;
                Value::Null
            }
            "recoveryRestore" => {
                let secret = recovery_secret(args, phrase, secret)?;
                let status = client.recover_device(db, &secret).await.map_err(describe)?;
                self.set_status(status);
                json!(status_name(status))
            }
            "recoveryRetire" => {
                client
                    .retire_recovery_root(db, &text("rootId")?)
                    .await
                    .map_err(describe)?;
                Value::Null
            }
            "recoveryList" => json!(
                client
                    .with_account(|account| {
                        account
                            .state
                            .recovery_roots
                            .iter()
                            .map(|root| json!({ "id": root.row.id, "device": root.row.signer_id }))
                            .collect::<Vec<_>>()
                    })
                    .ok_or_else(|| describe(ClientError::NoAccount))?
            ),
            "keysDeliver" => {
                let (reply, answer) = futures::channel::oneshot::channel();
                self.commands.borrow_mut().push_back(reply);
                (self.wake)();
                // The pass belongs to the loop; if it cannot run (its watch
                // keeps failing) the caller gets the loop's error, not a hang.
                match self.within(answer, DELIVER_WITHIN).await {
                    Some(done) => done.map_err(|_| describe(ClientError::Closed))??,
                    None => {
                        let why = self.last_error.borrow().clone();
                        return Err(why.unwrap_or_else(|| {
                            "database: the delivery pass did not run in time".to_owned()
                        }));
                    }
                }
            }
            "retryWrite" => {
                let space = SpaceAddress {
                    scope_id: text("scopeId")?,
                    identifier: text("identifier")?,
                };
                let started = args["startedAt"]
                    .as_f64()
                    .ok_or("expected write start in milliseconds")?;
                let now = web_time::SystemTime::now()
                    .duration_since(web_time::UNIX_EPOCH)
                    .unwrap_or_default();
                let elapsed = now.saturating_sub(Duration::from_millis(started as u64));
                let repeated = args["repeated"].as_bool().ok_or("expected repeated flag")?;
                json!(match self
                    .retry_write(&space, Self::write_deadline(elapsed), repeated)
                    .await?
                {
                    WriteRetry::Retry => "retry",
                    WriteRetry::Last => "last",
                    WriteRetry::Stop => "stop",
                })
            }
            "prepareSpace" | "retryWithCurrentKey" => {
                let space = SpaceAddress {
                    scope_id: text("scopeId")?,
                    identifier: text("identifier")?,
                };
                if op == "retryWithCurrentKey" {
                    json!(
                        client
                            .retry_with_current_key(&space, &text("failure")?)
                            .map_err(describe)?
                    )
                } else {
                    let deadline = if let Some(value) = args.get("deadline") {
                        // NAPI passes JavaScript numbers above u32::MAX as
                        // floats, including integer Unix timestamps in ms.
                        let millis = value.as_f64().ok_or("expected deadline in milliseconds")?;
                        let now = web_time::SystemTime::now()
                            .duration_since(web_time::UNIX_EPOCH)
                            .unwrap_or_default();
                        Some(
                            web_time::Instant::now()
                                + Duration::from_millis(millis as u64)
                                    .saturating_sub(now)
                                    .min(Duration::from_secs(10)),
                        )
                    } else {
                        None
                    };
                    json!(self.prepare_host_write(&space, deadline).await?)
                }
            }
            "spaceRow" => {
                let space = self.space_address(&text("table")?, &text("id")?).await?;
                json!({ "scopeId": space.scope_id, "identifier": space.identifier })
            }
            "keysRotate" | "keysRecoverRotation" => {
                let space = self.space_address(&text("table")?, &text("id")?).await?;
                let delivered = if op == "keysRecoverRotation" {
                    client.recover_rotation(db, &space).await
                } else {
                    client.rotate(db, &space).await
                }
                .map_err(|error| match error {
                    ClientError::NoAccount
                        if self.status.get() == Some(AccountStatus::AwaitingApproval) =>
                    {
                        "no-account: This device is waiting for approval".to_owned()
                    }
                    other => describe(other),
                })?;
                json!({ "delivered": delivered })
            }
            other => return Err(format!("unknown encryption operation `{other}`")),
        })
    }
}

/// The long-lived work of an encrypted app: verify the account, keep the
/// key watches, and run the delivery pass at its natural moments (start and
/// reconnect, and the app's call), and for the spaces in use whenever what the
/// watches see changes, completing removal requests when the account changes.
async fn main_loop(this: Rc<Encryption>) {
    let (client, db) = (Rc::clone(&this.client), Rc::clone(&this.db));
    // The runtime needs the catalogue identities, known after one sync.
    this.stage
        .set("waiting for the app's table identities from the server");
    loop {
        match Box::pin(client.prepare(&db)).await {
            Ok(true) => {
                this.readable.set(true);
                this.notify_state();
                this.set_background_deadline(None);
                break;
            }
            Ok(false) => {
                // The host drives the loop after sync applies the catalogue.
                this.set_background_deadline(None);
            }
            Err(error) => {
                if let Some(native) = &this.native {
                    if !native_may_retry(&error) {
                        *this.last_error.borrow_mut() = Some(describe(error.clone()));
                        this.record_native_failure(native, error);
                        return;
                    }
                    *native.result.borrow_mut() = Verification::Retry(error);
                }
                this.set_background_deadline(Some(
                    web_time::Instant::now() + Duration::from_millis(100),
                ));
            }
        }
        yield_now().await;
    }
    let mut account = None;
    let mut spaces = None;
    let mut verify = true;
    // A verification that failed for a reason that may pass (the server is
    // out of reach, the store failed) is tried again, later each time. A
    // protocol refusal is reported and waits for the account to change.
    let mut backoff = RETRY_FIRST;
    let mut retry_at: Option<web_time::Instant> = None;
    let mut start = true;
    let mut reconnect = false;
    let mut approval: Option<(String, _)> = None;
    let mut noticed = false;
    let mut retry_challenge_at = None;
    // The slow re-check of an open approval watch, and its backoff.
    let mut recheck_at: Option<web_time::Instant> = None;
    let mut recheck_backoff = APPROVAL_RECHECK_FIRST;
    loop {
        this.stage.set("waiting for its turn");
        let turn = this.gate.enter().await;
        this.stage.set("watching the account");
        if account.is_none() {
            match Box::pin(client.watch_account(&db)).await {
                Ok(watch) => account = Some(watch),
                Err(error) => *this.last_error.borrow_mut() = Some(describe(error)),
            }
        }
        if let Some(watch) = account.as_mut()
            && watch.changed()
        {
            verify = true;
        }
        if this.reconnected.replace(false) {
            // The retained subscriptions catch up through the normal sync path.
            verify = true;
            reconnect = true;
        }
        if retry_at.is_some_and(|at| web_time::Instant::now() >= at) {
            retry_at = None;
            verify = true;
        }
        if let Some(native) = &this.native {
            verify |= native.requested.get() > native.started.get();
        }
        let account_changed = verify;
        if verify {
            verify = false;
            if let Some(native) = &this.native {
                native.started.set(native.started.get() + 1);
            }
            this.stage.set("verifying the account");
            let recovering = this.native.as_ref().filter(|native| {
                !matches!(
                    *native.result.borrow(),
                    Verification::Complete | Verification::Failed(_)
                )
            });
            // A device already awaiting recovery keeps that evidence while
            // it looks again: a readiness deadline that falls inside this
            // pass still answers `awaiting-approval`, not a timeout. The
            // pass's own outcome replaces it below.
            if let Some(native) = recovering {
                let mut result = native.result.borrow_mut();
                if !matches!(*result, Verification::AwaitingRecovery) {
                    *result = Verification::Pending;
                }
            }
            let mut verified = Box::pin(client.ensure_account(&db)).await.map(Some);
            if let (Ok(Some(status)), Some(native)) = (&verified, recovering) {
                this.set_status(*status);
                this.stage.set("preparing account-secret recovery");
                let secret = native.recovery.borrow().clone();
                if let Some(secret) = secret {
                    verified =
                        Box::pin(this.recover_after_verification(*status, &secret, true)).await;
                }
            }
            if let Some(native) = &this.native {
                native.finished.set(native.started.get());
            }
            match verified {
                Ok(Some(status)) => {
                    backoff = RETRY_FIRST;
                    if this.native.is_some() {
                        retry_at = None;
                    }
                    this.set_status(status);
                    *this.last_error.borrow_mut() = None;
                    if let Some(native) = &this.native
                        && !matches!(*native.result.borrow(), Verification::Failed(_))
                    {
                        native.recovery.borrow_mut().take();
                        *native.result.borrow_mut() = Verification::Complete;
                    }
                    // The account watch was opened before verification; it
                    // will pick up any removal request from a later cut.
                    if status == AccountStatus::Active
                        && (account.is_none() || client.may_have_other_removal_requests())
                    {
                        match Box::pin(client.complete_removal_requests(&db)).await {
                            Ok(_) => {}
                            Err(error) => *this.last_error.borrow_mut() = Some(describe(error)),
                        }
                    }
                }
                Ok(None) => {
                    let native = this.native.as_ref().expect("only native recovery can wait");
                    *native.result.borrow_mut() = Verification::AwaitingRecovery;
                    retry_at = Some(web_time::Instant::now() + backoff);
                    backoff = (backoff * 2).min(RETRY_LAST);
                }
                Err(error) => {
                    let retry = if let Some(native) = &this.native {
                        if native_may_retry(&error) {
                            *native.result.borrow_mut() = Verification::Retry(error.clone());
                            true
                        } else {
                            this.record_native_failure(native, error.clone());
                            false
                        }
                    } else {
                        may_pass_later(&error)
                    };
                    if retry {
                        retry_at = Some(web_time::Instant::now() + backoff);
                        backoff = (backoff * 2).min(RETRY_LAST);
                    } else if this.native.is_some() {
                        retry_at = None;
                    }
                    *this.last_error.borrow_mut() = Some(if this.native.is_some() {
                        describe(error.clone())
                    } else {
                        describe(error)
                    });
                }
            }
        }
        if this.status.get() == Some(AccountStatus::AwaitingApproval) {
            if approval
                .as_ref()
                .is_some_and(|(id, _)| client.device().is_ok_and(|device| device.id != *id))
            {
                approval = None;
                noticed = false;
                retry_challenge_at = None;
                recheck_at = None;
            }
            if approval.is_none()
                && retry_challenge_at.is_none_or(|at| web_time::Instant::now() >= at)
            {
                retry_challenge_at = None;
                let opened = Box::pin(client.watch_approval(&db)).await;
                #[cfg(any(test, feature = "testing"))]
                let opened = if this.approval_watch_fault.get() == Some(ApprovalWatchFault::Install)
                {
                    this.approval_watch_fault.set(None);
                    Err(crate::StoreError::Failed("approval watch unavailable".into()).into())
                } else {
                    opened
                };
                match opened {
                    Ok(watch) => {
                        approval = Some(watch);
                        recheck_backoff = APPROVAL_RECHECK_FIRST;
                        recheck_at = Some(web_time::Instant::now() + recheck_backoff);
                    }
                    Err(_) => retry_challenge_at = Some(web_time::Instant::now() + APPROVAL_RETRY),
                }
            }
            if let Some((_, [challenges, proved])) = approval.as_mut() {
                let (changed, challenges_closed) = challenges.changes(|_| Some(()));
                let (proved_changed, proofs_closed) = proved.changes(|_| Some(()));
                #[cfg(any(test, feature = "testing"))]
                let (changed, proved_changed) =
                    if this.approval_watch_fault.get() == Some(ApprovalWatchFault::Stalled) {
                        challenges.rows.clear();
                        (Default::default(), Default::default())
                    } else {
                        (changed, proved_changed)
                    };
                #[cfg(any(test, feature = "testing"))]
                let challenges_closed = challenges_closed
                    || (this.approval_watch_fault.get() == Some(ApprovalWatchFault::Closed)
                        && this.approval_watch_fault.take().is_some());
                let now = web_time::Instant::now();
                let woke = account_changed || !changed.is_empty() || !proved_changed.is_empty();
                if woke {
                    // Evidence moved: look again soon after, in case the
                    // authoritative read below runs ahead of it.
                    recheck_backoff = APPROVAL_RECHECK_FIRST;
                    recheck_at = Some(now + recheck_backoff);
                }
                // A watch that stalls without closing, or a read that ran
                // ahead of the challenge, would leave the device waiting
                // forever: a slow re-check, backing off to
                // APPROVAL_RECHECK_LAST, bounds that wait.
                let recheck = recheck_at.is_some_and(|at| now >= at);
                if challenges.rows.is_empty() && !recheck {
                    if !changed.is_empty() {
                        noticed = false;
                    }
                    retry_challenge_at = None;
                } else if woke || recheck || retry_challenge_at.is_some_and(|at| now >= at) {
                    // A watch only tells us when looking could help. A
                    // challenge still needs the account's authoritative cut.
                    retry_challenge_at = None;
                    if recheck {
                        recheck_backoff = (recheck_backoff * 2).min(APPROVAL_RECHECK_LAST);
                        recheck_at = Some(now + recheck_backoff);
                    }
                    let challenged = Box::pin(client.challenged(&db)).await;
                    #[cfg(any(test, feature = "testing"))]
                    let challenged = if matches!(challenged, Ok(true))
                        && this.approval_watch_fault.get() == Some(ApprovalWatchFault::EarlyFalse)
                    {
                        this.approval_watch_fault.set(None);
                        Ok(false)
                    } else {
                        challenged
                    };
                    match challenged {
                        Ok(true) if !noticed => {
                            noticed = true;
                            this.reports
                                .borrow_mut()
                                .push(json!({ "type": "challenged" }));
                        }
                        Ok(false) => noticed = false,
                        Ok(true) => {}
                        Err(_) => {
                            retry_challenge_at = Some(web_time::Instant::now() + APPROVAL_RETRY);
                        }
                    }
                }
                if challenges_closed || proofs_closed {
                    approval = None;
                    recheck_at = None;
                    retry_challenge_at = Some(web_time::Instant::now() + APPROVAL_RETRY);
                }
            }
        } else {
            approval = None;
            noticed = false;
            retry_challenge_at = None;
            recheck_at = None;
        }
        if this.status.get() != Some(AccountStatus::Active) {
            // The pass needs an active device: answer what waits for it.
            while let Some(reply) = this.commands.borrow_mut().pop_front() {
                let _ = reply.send(Err(
                    "no-account: this device is not active in its account yet".to_owned(),
                ));
            }
        }
        // Reads that left rows out for want of a key: acquire the keys and
        // settle what they reported.
        if this.status.get() == Some(AccountStatus::Active)
            && let Err(error) = Box::pin(client.settle_reads(&db)).await
        {
            *this.last_error.borrow_mut() = Some(describe(error));
        }
        if this.status.get() == Some(AccountStatus::Active) {
            if spaces.is_none() {
                match Box::pin(client.watch_spaces(&db)).await {
                    Ok(watch) => spaces = Some(watch),
                    Err(error) => *this.last_error.borrow_mut() = Some(describe(error)),
                }
            }
            if spaces.is_none() {
                // No watch to run the pass on: answer what waits for it.
                let why =
                    this.last_error.borrow().clone().unwrap_or_else(|| {
                        "database: the key watch could not be opened".to_owned()
                    });
                while let Some(reply) = this.commands.borrow_mut().pop_front() {
                    let _ = reply.send(Err(why.clone()));
                }
            }
            if let Some(watch) = spaces.as_mut() {
                loop {
                    let next = this.uses.borrow_mut().pop_front();
                    let Some((open, table, id)) = next else { break };
                    let Ok(space) = this.space_address(&table, &id).await else {
                        continue;
                    };
                    if open {
                        watch.use_space(&space);
                    } else {
                        watch.release_space(&space);
                    }
                }
                let synced = Box::pin(client.sync_spaces(&db, watch)).await;
                let covered = synced.is_ok();
                if let Err(error) = synced {
                    *this.last_error.borrow_mut() = Some(describe(error));
                }
                loop {
                    let next = this.commands.borrow_mut().pop_front();
                    let Some(reply) = next else {
                        break;
                    };
                    let outcome = match Box::pin(client.deliver_keys(&db)).await {
                        Ok(results) => {
                            let failed = results.iter().filter(|(_, done)| done.is_err()).count();
                            let delivered: usize = results
                                .iter()
                                .filter_map(|(_, done)| done.as_ref().ok())
                                .sum();
                            Ok(json!({ "delivered": delivered, "failed": failed }))
                        }
                        Err(error) => Err(describe(error)),
                    };
                    let _ = reply.send(outcome);
                }
                if start || reconnect {
                    let retained = (!start && covered).then_some(&*watch);
                    start = false;
                    reconnect = false;
                    match client.deliver_keys_outside_watch(&db, retained).await {
                        Ok(results) => {
                            for (space, result) in results {
                                if let (Err(error), Ok(address)) =
                                    (result, crate::spaces::protocol_address(&space))
                                {
                                    client.tell_delivery_failed(&address, &error);
                                }
                            }
                        }
                        Err(error) => *this.last_error.borrow_mut() = Some(describe(error)),
                    }
                }
            }
        }
        this.stage.set("idle");
        // The approval watch wakes the loop when evidence changes. A retry
        // deadline covers a failed watch or authoritative challenge read,
        // and the slow re-check a watch that stalls or a read that ran early.
        // So does the retry wait of a rotation's remaining deliveries.
        this.set_background_deadline(
            retry_at
                .into_iter()
                .chain(retry_challenge_at)
                .chain(recheck_at)
                .chain(client.next_rotation_retry())
                .min(),
        );
        drop(turn);
        // A wake from queueing work may have been consumed while this
        // iteration was suspended after its drain point. Ask for the next
        // iteration only if it can take work left in the queues.
        if this.reconnected.get()
            || !this.commands.borrow().is_empty()
            || (this.status.get() == Some(AccountStatus::Active)
                && spaces.is_some()
                && !this.uses.borrow().is_empty())
        {
            (this.wake)();
        }
        yield_now().await;
    }
}

const APPROVAL_RETRY: Duration = Duration::from_secs(2);

/// The first re-check of an open approval watch, after it opens or after
/// it last reported a change. Each re-check that follows waits twice as
/// long, up to [`APPROVAL_RECHECK_LAST`].
const APPROVAL_RECHECK_FIRST: Duration = Duration::from_secs(2);

/// The longest wait between two re-checks of an open approval watch: one
/// authoritative challenge read at most this often while a device waits
/// for approval with nothing changing.
const APPROVAL_RECHECK_LAST: Duration = Duration::from_secs(30);

/// How long a call that waits for the loop's delivery pass may wait.
const DELIVER_WITHIN: std::time::Duration = std::time::Duration::from_secs(60);

/// `future`'s output, or `None` once `limit` has passed.
#[cfg(test)]
async fn within<F: Future + Unpin>(future: F, limit: Duration) -> Option<F::Output> {
    within_at(future, web_time::Instant::now() + limit).await
}

async fn within_at<F: Future + Unpin>(
    mut future: F,
    deadline: web_time::Instant,
) -> Option<F::Output> {
    std::future::poll_fn(|context| {
        if let Poll::Ready(value) = Pin::new(&mut future).poll(context) {
            return Poll::Ready(Some(value));
        }
        if web_time::Instant::now() >= deadline {
            return Poll::Ready(None);
        }
        context.waker().wake_by_ref();
        Poll::Pending
    })
    .await
}

/// Whether a failed verification may pass when tried again: a failure of the
/// database or the store (the server out of reach, a disk), not a refusal by
/// the protocol or a closed runtime.
fn may_pass_later(error: &ClientError) -> bool {
    matches!(error, ClientError::Database(_) | ClientError::Store(_))
}

/// Only unavailable observations, transport backpressure and serializable
/// conflicts can be retried by the native readiness loop. Store errors
/// and server refusals require the caller to see the cause.
fn native_may_retry(error: &ClientError) -> bool {
    matches!(error, ClientError::Database(error) if
        matches!(error.code, ErrorCode::Backpressure | ErrorCode::NotObserved)
            || error.is_conflict())
}

/// Pending once, so the host polls the loop again after its next tick.
async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(|context| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

fn status_name(status: AccountStatus) -> &'static str {
    match status {
        AccountStatus::Active => "active",
        AccountStatus::AwaitingApproval => "awaiting-approval",
        AccountStatus::Reenrolled => "reenrolled",
    }
}

/// A runtime error as JS sees it: a stable code, then the message.
fn describe(error: ClientError) -> String {
    if let ClientError::AccountResetPublication {
        record_ids,
        refused,
        source,
    } = &error
    {
        let code = match source.as_ref() {
            ClientError::Database(error) => error
                .reason
                .map_or(error.code.as_str(), |reason| reason.as_str()),
            ClientError::Protocol(error) => error.code().unwrap_or("protocol"),
            _ => "error",
        };
        return format!(
            "publication: {}",
            json!({
                "stage": "account-reset",
                "outcome": if *refused { "refused" } else { "unknown" },
                "code": code,
                "recordIds": record_ids.as_ref(),
                "message": source.to_string(),
            }),
        );
    }
    let code = match &error {
        ClientError::ApprovalAnswered => "approval-answered",
        ClientError::NoChallenge => "no-challenge",
        ClientError::ApprovalCodeMismatch => "approval-code-mismatch",
        ClientError::Store(_) => "store",
        ClientError::Closed => "closed",
        ClientError::NoAccount => "no-account",
        ClientError::Unsupported(_) => "unsupported",
        ClientError::Database(_) => "database",
        // The protocol's own code when it has one (`recovery-root-mismatch`,
        // `stale-state`, ...): what a caller tells expected refusals by.
        ClientError::Protocol(error) => error.code().unwrap_or("protocol"),
        _ => "error",
    };
    format!("{code}: {error}")
}

/// Move the JSON string's allocation into a zeroizing buffer and parse it
/// before creating the future, so only the parsed secret can wait in the gate.
fn take_recovery_phrase(args: &mut Value) -> Option<Result<RecoverySecret, String>> {
    let Value::String(text) = args.get_mut("phrase")? else {
        return None;
    };
    let phrase = Zeroizing::new(std::mem::take(text));
    Some(
        super::phrase::to_secret(&phrase)
            .map(|bytes| RecoverySecret::from_bytes(*bytes))
            .map_err(|error| format!("{}: the recovery phrase is not valid", error.code())),
    )
}

/// Use the supplied phrase, a formatted device account secret for automatic
/// recovery, or a hexadecimal backup secret for explicit recovery.
fn recovery_secret(
    args: &Value,
    phrase: Option<Result<RecoverySecret, String>>,
    text: Option<&str>,
) -> Result<RecoverySecret, String> {
    if let Some(phrase) = phrase {
        return phrase;
    }
    let text = text.ok_or("`phrase` is missing")?;
    if args.get("automatic").and_then(Value::as_bool) == Some(true) {
        return RecoverySecret::parse(text).map_err(str::to_owned);
    }
    secret(text)
}

/// A recovery secret from its hexadecimal text.
fn secret(hex: &str) -> Result<RecoverySecret, String> {
    if hex.len() != 64 || !hex.is_ascii() {
        return Err("a recovery secret is 64 hexadecimal characters".to_owned());
    }
    let mut bytes = jazz_e2ee::Zeroizing::new([0u8; 32]);
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[2 * index..2 * index + 2], 16)
            .map_err(|_| "a recovery secret is hexadecimal".to_owned())?;
    }
    Ok(RecoverySecret::from_bytes(*bytes))
}

/// A report as the JS events carry it.
fn report_json(report: &Report, table_name: impl Fn(&str) -> Option<String>) -> Value {
    let space = |at: &SpaceAddress| {
        let mut value = json!({ "scopeId": at.scope_id, "id": at.identifier });
        if let Some(table) = table_name(&at.scope_id) {
            value["table"] = Value::String(table);
        }
        value
    };
    match report {
        Report::Unreadable(unreadable) => json!({
            "type": "unreadable",
            "table": unreadable.table,
            "id": unreadable.id.map(|id| id.0.to_string()),
            "column": unreadable.column,
            "code": unreadable.code.as_str(),
            "space": unreadable.space.as_ref().map(space),
        }),
        Report::MissingKey {
            table,
            space: at,
            epoch_id,
        } => {
            json!({ "type": "missing-key", "table": table, "space": space(at), "epochId": epoch_id })
        }
        Report::SkippedRecord { document, reason } => {
            json!({ "type": "skipped-record", "document": document, "reason": reason })
        }
        Report::ConflictingAccountKey {
            record_id,
            signer_id,
            epoch_id,
        } => json!({
            "type": "conflicting-account-key", "recordId": record_id,
            "signerId": signer_id, "epochId": epoch_id,
        }),
        Report::UnsupportedAccountKey {
            record_id,
            signer_id,
            capability,
        } => json!({
            "type": "unsupported-account-key", "recordId": record_id,
            "signerId": signer_id, "capability": capability.code(),
        }),
        Report::InvalidDelivery {
            space: at,
            sender,
            recipient,
            key_epoch_id,
        } => json!({
            "type": "invalid-delivery", "space": space(at), "sender": sender,
            "recipient": recipient, "epochId": key_epoch_id,
        }),
        Report::BrokenHistory {
            space: at,
            key_epoch_id,
            author,
        } => json!({
            "type": "broken-history", "space": space(at), "epochId": key_epoch_id, "author": author,
        }),
        Report::AccountReset {
            account,
            space: at,
            generation,
        } => json!({
            "type": "account-reset", "account": account, "space": space(at), "generation": generation,
        }),
        Report::SpaceOverMemberLimit { space: at, members } => {
            json!({ "type": "space-over-member-limit", "space": space(at), "members": members })
        }
        Report::RotationCheckFailed { space: at, reason } => {
            json!({ "type": "rotation-check-failed", "space": space(at), "reason": reason })
        }
        Report::DeliveryFailed { space: at, reason } => {
            json!({ "type": "delivery-failed", "space": space(at), "reason": reason })
        }
        Report::RotationFailed { space: at, reason } => {
            json!({ "type": "rotation-failed", "space": space(at), "reason": reason })
        }
        Report::Withdrawn {
            table,
            id,
            column,
            space: at,
            code,
        } => json!({
            "type": "withdrawn", "table": table, "id": id.map(|id| id.0.to_string()),
            "column": column, "code": code.as_str(), "space": space(at),
        }),
    }
}

/// A future that settles as a JSON text, for a JS promise.
pub type JsonFuture = Pin<Box<dyn Future<Output = Result<Value, String>>>>;

#[cfg(test)]
#[path = "../../../jazz/layers/types/tests/support/zeroized_allocation.rs"]
mod zeroized_allocation;

#[cfg(test)]
mod tests {
    use super::*;

    fn run<T>(future: impl Future<Output = T>) -> T {
        jazz::db::block_on(future)
    }

    fn bindings_for(file: &[u8]) -> String {
        let app = jazz::tools::deployment::artifact::load_app(file).unwrap();
        json!({
            "schema": { "current": app.current_hash(),
                "schemas": [{ "hash": app.current_hash(), "schema": app.current_schema() }],
                "migrations": [], "encryption": app.encryption() },
            "relations": {}
        })
        .to_string()
    }

    /// Alice's host must get another wake when its timer polls before the
    /// deadline. Arm the builder's bridge directly to test an early poll
    /// without relying on real timer jitter.
    #[test]
    fn a_poll_before_the_deadline_rearms_the_host_timer() {
        let builder = RuntimeBuilder::from_bindings(
            String::new(),
            "test".into(),
            "test".into(),
            "alice".into(),
            None,
        )
        .unwrap();
        let deadlines = Rc::new(RefCell::new(Vec::new()));
        let announced = Rc::clone(&deadlines);
        builder.set_store_wake(Some(Box::new(move |wake| {
            if let StoreWake::Deadline(delay) = wake {
                announced.borrow_mut().push(delay);
            }
        })));

        builder
            .store
            .set_background_deadline(Some(web_time::Instant::now() + Duration::from_secs(30)));
        let first = deadlines.borrow_mut().pop().unwrap().unwrap();

        builder.drive(|| {});
        let second = deadlines.borrow_mut().pop().unwrap().unwrap();
        assert!(!second.is_zero() && second <= first);

        builder.drive(|| builder.store.set_background_deadline(None));
        builder.drive(|| {});
        assert_eq!(*deadlines.borrow(), [None]);
    }

    /// Alice can retry a failed store request with her original device-account
    /// secret. Inspect the pending builder because a successful local build
    /// alone cannot distinguish a device account from an app-login account.
    #[test]
    fn store_failure_preserves_the_device_account_secret_for_retry() {
        let bindings = bindings_for(include_bytes!(
            "../../../jazz-codegen/tests/apps/encrypted/jazz.app.json"
        ));
        let builder = RuntimeBuilder::from_bindings(
            bindings,
            "test".into(),
            "test".into(),
            uuid::Uuid::new_v4().to_string(),
            Some(RecoverySecret::from_bytes([1; 32]).format().to_string()),
        )
        .unwrap();
        let mut attempt = builder.build_future().unwrap();
        let mut context = Context::from_waker(Waker::noop());
        assert!(attempt.as_mut().poll(&mut context).is_pending());
        let requests = builder.take_requests();
        assert_eq!(requests.len(), 1);
        let request: Value = serde_json::from_str(&requests[0]).unwrap();
        assert!(builder.reply(request["id"].as_str().unwrap(), false, "unavailable"));
        assert!(matches!(
            attempt.as_mut().poll(&mut context),
            Poll::Ready(Err(_))
        ));
        assert!(
            builder
                .options
                .borrow()
                .as_ref()
                .and_then(|options| options.secret.as_ref())
                .is_some_and(|secret| secret.as_bytes() == &[1; 32])
        );
    }

    /// Contract: a build that fails, succeeds or is abandoned frees the slot,
    /// and a `ready()` while a build runs is told so, not that it is built.
    #[test]
    fn a_build_slot_frees_when_its_build_ends_or_is_dropped() {
        let slot = BuildSlot::default();
        let held = slot.take().unwrap();
        assert_eq!(slot.take().err(), Some(STILL_BUILDING));
        drop(held);

        // A build whose store never answers, then abandoned.
        let held = slot.take().unwrap();
        let mut never = Box::pin(async move {
            let _held = held;
            std::future::pending::<()>().await;
        });
        let waker = futures::task::noop_waker();
        assert!(
            never
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert_eq!(slot.take().err(), Some(STILL_BUILDING));
        drop(never);
        assert!(slot.take().is_ok());

        // A build that settles with an error.
        let held = slot.take().unwrap();
        let failed: Result<(), String> = run(async move {
            let _held = held;
            Err("store failed".to_owned())
        });
        assert!(failed.is_err());
        assert!(slot.take().is_ok());
    }

    /// Contract: a call that waits on the loop gets an answer or `None` at
    /// its limit, never a hang (white-box: the loop's watch cannot be made to
    /// fail from outside).
    #[test]
    fn waiting_on_the_loop_is_bounded() {
        let limit = std::time::Duration::from_millis(30);
        assert_eq!(run(within(std::future::ready(7), limit)), Some(7));
        let started = web_time::Instant::now();
        assert_eq!(run(within(std::future::pending::<u8>(), limit)), None);
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
    }

    #[test]
    fn a_recovery_secret_is_64_hexadecimal_characters() {
        assert!(secret(&"ab".repeat(32)).is_ok());
        assert!(secret("ab").is_err());
        assert!(secret(&"zz".repeat(32)).is_err());
    }

    /// Alice's owned mnemonic is wiped before its buffer is freed on success
    /// or parse failure. The argument boundary is tested directly so the
    /// assertion can identify the exact allocation handed to the parser.
    #[test]
    fn an_owned_recovery_phrase_is_wiped_on_success_and_failure() {
        use super::zeroized_allocation::assert_zeroized;

        let phrase = super::super::phrase::from_secret(&[5; 32]);
        let parse = |phrase| {
            let mut args = Value::Object(serde_json::Map::from_iter([(
                "phrase".to_owned(),
                Value::String(phrase),
            )]));
            take_recovery_phrase(&mut args).unwrap()
        };
        assert!(assert_zeroized(phrase.as_str().to_owned(), parse).is_ok());
        assert!(assert_zeroized("one word".to_owned(), parse).is_err());
    }

    /// Alice's mnemonic is consumed before either recovery call can wait
    /// or be cancelled. Test the argument boundary directly because callers
    /// cannot inspect the call's owned JSON after handing it over.
    #[test]
    fn recovery_phrase_is_removed_before_success_error_or_cancellation() {
        let bytes = [5; 32];
        let phrase = super::super::phrase::from_secret(&bytes);
        let mut args = json!({ "phrase": phrase.as_str() });
        let parsed = take_recovery_phrase(&mut args).unwrap().unwrap();
        assert_eq!(parsed.format(), RecoverySecret::from_bytes(bytes).format());
        assert_eq!(args["phrase"], "");

        let mut args = json!({ "phrase": "one word" });
        let parsed = take_recovery_phrase(&mut args).unwrap();
        assert!(matches!(parsed, Err(error) if error.starts_with("invalid-length: ")));
        assert_eq!(args["phrase"], "");

        let runtime = native_runtime_with_catalogue(true);
        let busy = run(runtime.gate.enter());
        let mut context = Context::from_waker(Waker::noop());
        for op in ["recoveryRegister", "recoveryRestore"] {
            let mut call = runtime.call(op.to_owned(), json!({ "phrase": phrase.as_str() }));
            assert!(call.as_mut().poll(&mut context).is_pending());
            drop(call);
        }
        drop(busy);
    }

    /// Contract: an encrypted app is installed from its own declarations: an
    /// app file without any encryption is refused, and one that declares it
    /// builds the runtime over the store the app supplied (here answered by
    /// the test as JavaScript would).
    #[test]
    fn the_runtime_is_built_from_the_apps_own_declarations() {
        let plain = br#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
            "tables": { "projects": { "columns": { "name": "string" } } } }"#;
        let account = uuid::Uuid::new_v4().to_string();
        let runtime = |file: &[u8]| {
            RuntimeBuilder::from_bindings(
                bindings_for(file),
                "napi-encryption".into(),
                "test".into(),
                account.clone(),
                None,
            )
            .unwrap()
        };
        assert!(
            run(runtime(plain).build_future().unwrap())
                .unwrap_err()
                .contains("declare no encryption")
        );
        let encrypted = br#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
            "tables": {
                "projects": { "columns": { "name": "string" } },
                "space_roots": { "columns": { "space": "uuid" } },
                "notes": {
                    "columns": { "projectId": "uuid", "title": "string", "body": "bytes" },
                    "relations": { "project": { "rel": "projects", "column": "projectId" } },
                    "encryption": { "space": "projectId", "scope": "projects",
                        "columns": { "body": "string" } }
                }
            } }"#;
        let answer = super::super::bridge::serve(Rc::new(crate::MemoryStore::new()));
        let builder = runtime(encrypted);
        let mut building = builder.build_future().unwrap();
        let mut context = std::task::Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(result) = building.as_mut().poll(&mut context) {
                result.unwrap();
                break;
            }
            // JavaScript's side: run each request against a store.
            for text in builder.take_requests() {
                let request: Value = serde_json::from_str(&text).unwrap();
                let id = request["id"].as_str().unwrap();
                let (ok, payload) = run(answer(text));
                assert!(builder.reply(id, ok, &payload));
            }
        }
        let slot = builder.built.borrow();
        let built = slot.as_ref().unwrap();
        assert_eq!(built.client.account_id(), account);
        assert!(built.reports.borrow().is_empty());
    }

    fn native_runtime_with_catalogue(include_notes: bool) -> Rc<Encryption> {
        native_runtime(include_notes, true, None)
    }

    fn native_runtime(
        include_notes: bool,
        observed: bool,
        bridge_store: Option<Rc<JsStore>>,
    ) -> Rc<Encryption> {
        use jazz::db::{DbConfig, DbIdentity};
        use jazz::ids::{AuthorSubject, NodeUuid};
        use jazz::schema::JazzSchema;
        use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

        let file = br#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
                "tables": {
                    "projects": { "columns": { "name": "string" } },
                    "notes": {
                        "columns": { "projectId": "uuid", "body": "bytes" },
                        "relations": { "project": { "rel": "projects", "column": "projectId" } },
                        "encryption": { "space": "projectId", "scope": "projects",
                            "columns": { "body": "string" } }
                    }
                } }"#;
        let declarations = jazz::tools::deployment::artifact::load_client_encrypted(
            file,
            jazz::tools::AppId::from_name("terminal-catalogue"),
            "test",
        )
        .unwrap()
        .2
        .unwrap();
        let built = run(build_native(
            declarations,
            uuid::Uuid::new_v4().to_string(),
            Rc::new(crate::MemoryStore::new()),
            None,
        ))
        .unwrap();

        let mut schema = SchemaBuilder::new()
            .table(TableSchemaBuilder::new("projects").column("name", ColumnType::Text));
        if include_notes {
            schema = schema.table(
                TableSchemaBuilder::new("notes")
                    .fk_column("projectId", "projects")
                    .column("body", ColumnType::Bytea),
            );
        }
        let db = Rc::new(
            run(Db::open(DbConfig::new(
                JazzSchema::new(&schema.build()).unwrap(),
                DbIdentity {
                    node: NodeUuid::from_bytes([1; 16]),
                    author: AuthorSubject::SYSTEM,
                },
            )))
            .unwrap(),
        );
        if observed {
            jazz::db::block_on(db.set_authority()).unwrap();
        }
        let store = bridge_store.clone();
        let wake = Rc::new(move || {
            if let Some(store) = &store {
                store.request_wake();
            }
        });
        Encryption::new_native(built, bridge_store, db, wake)
    }

    /// A space's row answers from the local catalogue, even while the loop's
    /// work holds the runtime (verification, a delivery pass).
    #[test]
    fn a_space_row_answers_while_the_loop_is_busy() {
        let runtime = native_runtime_with_catalogue(true);
        assert!(run(runtime.client.prepare(&runtime.db)).unwrap());
        let expected_scope = run(runtime.db.table_identity("projects"))
            .unwrap()
            .unwrap()
            .0
            .to_string();
        assert_ne!(expected_scope, uuid::Uuid::nil().to_string());
        let busy = run(runtime.gate.enter());
        let answer = run(within(
            runtime.call(
                "spaceRow".to_owned(),
                json!({ "table": "projects", "id": "room" }),
            ),
            std::time::Duration::from_secs(5),
        ));
        drop(busy);
        let row = answer.expect("the space row waited for the loop").unwrap();
        assert_eq!(row["identifier"], "room");
        assert_eq!(row["scopeId"], expected_scope);
    }

    #[test]
    fn a_space_address_preserves_not_observed_before_the_first_sync() {
        let runtime = native_runtime(true, false, None);
        let expected = run(runtime.db.table_identity("projects")).unwrap_err();
        assert_eq!(expected.code, ErrorCode::NotObserved);
        assert_eq!(
            run(runtime.space_address("projects", "room")).unwrap_err(),
            expected.to_string()
        );
    }

    /// After shutdown a host's subscriptions still close (`useSpace`
    /// answers), but nothing is queued for the loop, which no longer runs.
    #[test]
    fn a_space_use_after_shutdown_queues_nothing() {
        let runtime = native_runtime_with_catalogue(true);
        let use_space = || {
            run(runtime.call(
                "useSpace".into(),
                json!({ "open": false, "table": "projects", "id": "room" }),
            ))
        };
        assert_eq!(use_space(), Ok(Value::Null));
        assert_eq!(runtime.uses.borrow().len(), 1);
        runtime.uses.borrow_mut().clear();
        run(runtime.call("shutdown".into(), Value::Null)).unwrap();
        assert_eq!(use_space(), Ok(Value::Null));
        assert!(runtime.uses.borrow().is_empty());
    }

    /// Alice's write retry asks the host to wake both during the retry pause
    /// and while key preparation is pending. Poll directly to observe each
    /// timer request without running a host timer or waiting for it to fire.
    #[test]
    fn a_write_retry_arms_host_deadlines_for_pause_and_preparation() {
        let store = Rc::new(JsStore::default());
        let deadlines = Rc::new(RefCell::new(Vec::new()));
        let announced = Rc::clone(&deadlines);
        store.set_store_wake(Some(Box::new(move |wake| {
            if let StoreWake::Deadline(delay) = wake {
                announced.borrow_mut().push(delay);
            }
        })));
        let runtime = native_runtime(true, true, Some(store));
        let busy = run(runtime.gate.enter());
        let space = SpaceAddress {
            scope_id: uuid::Uuid::new_v4().to_string(),
            identifier: uuid::Uuid::new_v4().to_string(),
        };
        let mut context = Context::from_waker(Waker::noop());
        for (repeated, wait) in [(true, "retry pause"), (false, "key preparation")] {
            deadlines.borrow_mut().clear();
            let deadline = Encryption::write_deadline(Duration::ZERO);
            let mut retry = Box::pin(runtime.retry_write(&space, deadline, repeated));
            assert!(retry.as_mut().poll(&mut context).is_pending(), "{wait}");
            assert!(
                matches!(deadlines.borrow().as_slice(), [Some(_)]),
                "{wait} did not arm a host deadline: {:?}",
                deadlines.borrow()
            );
        }
        drop(busy);
        runtime.stop();
    }

    /// Alice's key preparation gets the turn as soon as the loop's store
    /// reply arrives. Replace the loop with a single store load so the test
    /// controls exactly which drive frees the turn.
    ///
    /// loop ──load──► store ──reply──► loop releases ──► Alice prepares
    #[test]
    fn a_store_reply_that_frees_the_turn_also_drives_a_waiting_preparation() {
        let store = Rc::new(JsStore::default());
        let wakes = Rc::new(Cell::new(0));
        let count = Rc::clone(&wakes);
        store.set_wake(Box::new(move || count.set(count.get() + 1)));
        let runtime = native_runtime(true, true, Some(Rc::clone(&store)));
        let running = Rc::clone(&runtime);
        let loading = Rc::clone(&store);
        *runtime.main.borrow_mut() = Some(Box::pin(async move {
            let _turn = running.gate.enter().await;
            assert!(loading.load("alice", "keys").await.unwrap().is_none());
        }));
        runtime.drive();
        let request: Value = serde_json::from_str(&store.take_requests()[0]).unwrap();

        let space = SpaceAddress {
            scope_id: uuid::Uuid::new_v4().to_string(),
            identifier: uuid::Uuid::new_v4().to_string(),
        };
        let deadline = Encryption::write_deadline(Duration::ZERO);
        let mut retry = Box::pin(runtime.retry_write(&space, deadline, false));
        assert!(
            retry
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(runtime.preparations.0.borrow().len(), 1);
        wakes.set(0);

        assert!(store.reply(
            request["id"].as_str().unwrap().parse().unwrap(),
            true,
            "null"
        ));
        runtime.drive();
        assert!(runtime.preparations.0.borrow().is_empty());
        assert_eq!(
            wakes.get(),
            1,
            "releasing the contended turn wakes the host"
        );
        runtime.drive();
        assert_eq!(wakes.get(), 1, "the freed gate schedules no idle wakes");
        runtime.stop();
    }

    /// Alice's subscription asks an idle host to drive the space watch.
    /// Observe the host callback directly, before any unrelated drive.
    #[test]
    fn a_space_subscription_wakes_an_idle_host() {
        let store = Rc::new(JsStore::default());
        let wakes = Rc::new(Cell::new(0));
        let count = Rc::clone(&wakes);
        store.set_wake(Box::new(move || count.set(count.get() + 1)));
        let runtime = native_runtime(true, true, Some(store));
        runtime.drive();
        wakes.set(0);
        runtime.use_space(true, "projects".into(), uuid::Uuid::new_v4().to_string());
        assert_eq!(wakes.get(), 1);
        runtime.stop();
    }

    /// Alice's connection changes request a drive of their own.
    /// Observe the runtime callback without sync traffic to drive it.
    #[test]
    fn a_connection_change_wakes_an_idle_host() {
        let store = Rc::new(JsStore::default());
        let wakes = Rc::new(Cell::new(0));
        let count = Rc::clone(&wakes);
        store.set_wake(Box::new(move || count.set(count.get() + 1)));
        let runtime = native_runtime(true, true, Some(store));
        runtime.drive();
        wakes.set(0);
        runtime.set_online(false);
        assert_eq!(wakes.get(), 1);
        runtime.set_online(true);
        assert_eq!(wakes.get(), 2);
        runtime.stop();
    }

    /// Alice's first catalogue wakes the host's readability waiters.
    /// Observe the bridge signal separately from account verification.
    #[test]
    fn a_readable_catalogue_notifies_the_host() {
        let store = Rc::new(JsStore::default());
        let notices = Rc::new(Cell::new(0));
        let count = Rc::clone(&notices);
        store.set_store_wake(Some(Box::new(move |wake| {
            if matches!(wake, StoreWake::EncryptionState) {
                count.set(count.get() + 1);
            }
        })));
        let runtime = native_runtime(true, true, Some(store));
        assert_eq!(runtime.state()["readable"], false);
        runtime.drive();
        assert_eq!(runtime.state()["readable"], true);
        assert_eq!(notices.get(), 1);
        runtime.drive();
        assert_eq!(notices.get(), 1);
        runtime.stop();
    }

    /// Alice's account status wakes host waiters once per change.
    /// Set the status directly to isolate its bridge signal from readability.
    #[test]
    fn an_account_status_change_notifies_the_host() {
        let store = Rc::new(JsStore::default());
        let notices = Rc::new(Cell::new(0));
        let count = Rc::clone(&notices);
        store.set_store_wake(Some(Box::new(move |wake| {
            if matches!(wake, StoreWake::EncryptionState) {
                count.set(count.get() + 1);
            }
        })));
        let runtime = native_runtime(true, true, Some(store));
        runtime.set_status(AccountStatus::AwaitingApproval);
        assert_eq!(notices.get(), 1);
        runtime.set_status(AccountStatus::AwaitingApproval);
        assert_eq!(notices.get(), 1);
        runtime.set_status(AccountStatus::Active);
        assert_eq!(notices.get(), 2);
        runtime.stop();
    }

    /// Alice's delivery call wakes an idle host so the loop can answer it.
    /// Poll the call directly because the Node wrapper drives the loop itself.
    #[test]
    fn a_delivery_command_wakes_an_idle_host() {
        let store = Rc::new(JsStore::default());
        let wakes = Rc::new(Cell::new(0));
        let count = Rc::clone(&wakes);
        store.set_wake(Box::new(move || count.set(count.get() + 1)));
        let runtime = native_runtime(true, true, Some(store));
        runtime.drive();
        wakes.set(0);
        let mut call = runtime.call("keysDeliver".to_owned(), Value::Null);
        let mut context = Context::from_waker(Waker::noop());
        assert!(call.as_mut().poll(&mut context).is_pending());
        assert_eq!(wakes.get(), 1);
        runtime.drive();
        assert_eq!(
            call.as_mut().poll(&mut context),
            Poll::Ready(Err(
                "no-account: this device is not active in its account yet".into()
            ))
        );
        runtime.stop();
    }

    /// Alice queues a space watch and a delivery after the loop has taken its
    /// snapshot of work. Inject both at the bridge's end-of-iteration signal
    /// to check that the loop asks for the turn that will actually serve them.
    #[test]
    fn space_uses_and_deliveries_queued_mid_iteration_wake_the_next_pass() {
        use jazz::db::{DbConfig, DbIdentity};
        use jazz::ids::{AuthorSubject, NodeUuid};
        use jazz::schema::JazzSchema;

        let (schema, _, declarations) = jazz::tools::deployment::artifact::load_client_encrypted(
            include_bytes!("../../../jazz-codegen/tests/apps/encrypted/jazz.app.json"),
            jazz::tools::AppId::from_name("queued-encryption-work"),
            "test",
        )
        .unwrap();
        let built = run(build_client(
            declarations.unwrap(),
            uuid::Uuid::new_v4().to_string(),
            Rc::new(crate::MemoryStore::new()),
            None,
        ))
        .unwrap();
        let db = Rc::new(
            run(Db::open(DbConfig::new(
                JazzSchema::new(&schema).unwrap(),
                DbIdentity {
                    node: NodeUuid::from_bytes([1; 16]),
                    author: AuthorSubject::SYSTEM,
                },
            )))
            .unwrap(),
        );
        run(db.set_authority()).unwrap();
        let store = Rc::new(JsStore::default());
        let wakes = Rc::new(Cell::new(0));
        let count = Rc::clone(&wakes);
        store.set_wake(Box::new(move || count.set(count.get() + 1)));
        let runtime = Encryption::new(built, Rc::clone(&store), db);
        // The local catalogue has the space-watch tables. No upstream is
        // needed to exercise the queues once the account is marked active.
        runtime.set_status(AccountStatus::Active);
        runtime.drive();

        let delivery = Rc::new(RefCell::new(None));
        let queued = Rc::clone(&delivery);
        let running = Rc::downgrade(&runtime);
        let consumed = Rc::clone(&wakes);
        let enqueue = Rc::new(Cell::new(false));
        let armed = Rc::clone(&enqueue);
        store.set_store_wake(Some(Box::new(move |wake| {
            if matches!(wake, StoreWake::Deadline(_)) && armed.replace(false) {
                let runtime = running.upgrade().unwrap();
                runtime.use_space(
                    true,
                    "projects".to_owned(),
                    uuid::Uuid::new_v4().to_string(),
                );
                let mut call = runtime.call("keysDeliver".to_owned(), Value::Null);
                assert!(
                    call.as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                *queued.borrow_mut() = Some(call);
                // An early drive already consumed the queueing wakes.
                consumed.set(0);
            }
        })));
        store.set_background_deadline(Some(web_time::Instant::now() + Duration::from_secs(30)));
        enqueue.set(true);
        runtime.drive();
        assert_eq!(runtime.uses.borrow().len(), 1);
        assert_eq!(runtime.commands.borrow().len(), 1);
        assert_eq!(wakes.get(), 1, "the queued work needs another host drive");

        runtime.drive();
        assert!(runtime.uses.borrow().is_empty());
        let result = delivery
            .borrow_mut()
            .take()
            .unwrap()
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()));
        assert_eq!(
            result,
            Poll::Ready(Ok(json!({ "delivered": 0, "failed": 0 })))
        );
        runtime.drive();
        assert_eq!(wakes.get(), 1, "the drained loop schedules no idle wakes");
        runtime.stop();
    }

    /// Alice's write must return its refusal at the deadline even if preparation
    /// still holds a transaction. Hold the runtime gate to keep it pending
    /// deterministically, then check that a late preparation can finish.
    #[test]
    fn a_write_deadline_bounds_a_stalled_key_preparation() {
        let runtime = native_runtime_with_catalogue(true);
        let busy = run(runtime.gate.enter());
        let space = SpaceAddress {
            scope_id: uuid::Uuid::new_v4().to_string(),
            identifier: uuid::Uuid::new_v4().to_string(),
        };
        let deadline = web_time::Instant::now() + Duration::from_millis(30);
        let result = run(within(
            Box::pin(runtime.retry_write(&space, deadline, false)),
            Duration::from_secs(1),
        ));
        assert_eq!(result, Some(Ok(WriteRetry::Stop)));
        assert!(web_time::Instant::now() >= deadline);
        assert_eq!(runtime.preparations.0.borrow().len(), 1);
        drop(busy);
        runtime.drive();
        assert!(runtime.preparations.0.borrow().is_empty());
        runtime.stop();
    }

    /// While preparation answers "not yet" (alice's device still waits for
    /// the key), her write asks again every 200 ms until its deadline, then
    /// stops. The runtime clock is held, so each wait is stepped on it, never
    /// slept. A unit test: the preparation's answer is the test's to give.
    ///
    /// ```text
    /// t=0 prepare: not yet ─200 ms─► t=200 not yet ─200 ms─► t=400 not yet
    ///     ─100 ms (what is left)─► t=500 deadline: stop
    /// ```
    #[test]
    fn a_write_not_yet_ready_asks_again_every_200_ms_until_its_deadline() {
        let runtime = native_runtime_with_catalogue(true);
        // An hour ahead of the real clock, so no real-time check expires.
        runtime
            .client
            .advance_settle_clock(Duration::from_secs(3600));
        let start = runtime.client.shared.now();
        let deadline = start + Duration::from_millis(500);
        let space = SpaceAddress {
            scope_id: uuid::Uuid::new_v4().to_string(),
            identifier: uuid::Uuid::new_v4().to_string(),
        };
        let asked = RefCell::new(Vec::new());
        let result = run(runtime.retry_until(&space, deadline, false, || {
            asked.borrow_mut().push(runtime.client.shared.now() - start);
            futures::future::ready(Ok(Ok(false)))
        }));
        assert_eq!(result, Ok(WriteRetry::Stop));
        assert_eq!(
            *asked.borrow(),
            [0, 200, 400].map(Duration::from_millis),
            "each attempt follows a 200 ms wait"
        );
        assert_eq!(
            runtime.client.shared.now(),
            deadline,
            "the last wait ends at the deadline"
        );
        runtime.stop();
    }

    /// A confirmed catalogue that lacks an encrypted table stops
    /// preparation. Later foreground readiness calls still report why.
    #[test]
    fn foreground_readiness_keeps_a_terminal_catalogue_error() {
        let runtime = native_runtime_with_catalogue(false);
        let first = runtime.request_foreground_ready();
        runtime.drive();
        let cause = runtime
            .foreground_ready_outcome(first, false)
            .unwrap()
            .unwrap_err();
        assert!(
            cause.starts_with("unsupported:") && cause.contains("`notes`"),
            "{cause}"
        );

        let second = runtime.request_foreground_ready();
        runtime.drive();
        assert_eq!(
            runtime.foreground_ready_outcome(second, false),
            Some(Err(cause.clone()))
        );
        assert_eq!(
            runtime.foreground_ready_outcome(second, true),
            Some(Err(cause))
        );
    }

    #[test]
    fn foreground_readiness_reports_closed_to_both_pending_waiters() {
        // The encryption catalogue is valid here. Missing ledger tables
        // fail account verification, which leaves the loop able to retry.
        let runtime = native_runtime_with_catalogue(true);
        let initial = runtime.request_foreground_ready();
        runtime.drive();
        assert!(matches!(
            runtime.foreground_ready_outcome(initial, false),
            Some(Err(message)) if message.starts_with("database:")
        ));

        let first = runtime.request_foreground_ready();
        let second = runtime.request_foreground_ready();
        assert_ne!(first, 0);
        assert_eq!(first, second);
        assert_eq!(runtime.foreground_ready_outcome(first, false), None);

        runtime.stop();
        let closed = describe(ClientError::Closed);
        assert_eq!(
            runtime.foreground_ready_outcome(first, false),
            Some(Err(closed.clone()))
        );
        assert_eq!(
            runtime.foreground_ready_outcome(second, true),
            Some(Err(closed))
        );
    }

    /// Alice's first listener gets all 10,000 active unreadables even when
    /// the host's event queue overflowed. Inject reports at the runtime to
    /// exercise both limits without 10,000 database reads.
    #[test]
    fn a_new_listener_gets_the_active_set_after_event_queue_overflow() {
        use jazz::e2ee::{Unreadable, UnreadableCode};
        use jazz::ids::RowUuid;

        let runtime = native_runtime_with_catalogue(true);
        for id in 1..=10_000 {
            runtime.client.shared.report(Report::Unreadable(Unreadable {
                table: "notes".into(),
                id: Some(RowUuid(uuid::Uuid::from_u128(id))),
                column: Some("body".into()),
                code: UnreadableCode::InvalidCiphertext,
                space: None,
            }));
        }
        let snapshot = run(runtime.call("reportsForListener".into(), Value::Null)).unwrap();
        let active = snapshot["active"].as_array().unwrap();
        assert_eq!(active.len(), 10_000);
        assert_eq!(active[0]["id"], uuid::Uuid::from_u128(1).to_string());
        let queued = snapshot["queued"].as_array().unwrap();
        assert_eq!(queued.len(), MAX_REPORTS + 1);
        let dropped: Value = serde_json::from_str(queued[0].as_str().unwrap()).unwrap();
        assert_eq!(
            dropped,
            json!({ "type": "reports-dropped", "count": 10_000 - MAX_REPORTS })
        );
        assert!(runtime.take_reports().is_empty());
        runtime.stop();
    }

    /// Alice's host delivers the client's report on its next drive.
    #[test]
    fn driving_the_host_delivers_the_clients_report() {
        let runtime = native_runtime_with_catalogue(true);
        runtime.client.shared.report(Report::SkippedRecord {
            document: "device".into(),
            reason: "invalid".into(),
        });
        runtime.drive();
        let reports = runtime.take_reports();
        assert!(
            reports.contains(
                &json!({
                    "type": "skipped-record", "document": "device", "reason": "invalid"
                })
                .to_string()
            )
        );
        assert!(runtime.take_reports().is_empty());
        runtime.stop();
    }

    /// Contract: the reports waiting for JS stay bounded when nothing takes
    /// them: past [`MAX_REPORTS`] the oldest go, a challenge is kept, and
    /// the next take says how many went before the rest, oldest first.
    #[test]
    fn the_report_queue_is_bounded_and_counts_what_it_dropped() {
        let mut queue = ReportQueue::default();
        queue.push(json!({ "type": "challenged" }));
        for index in 0..MAX_REPORTS + 10 {
            queue.push(json!({ "type": "unreadable", "index": index }));
        }
        let taken = queue.take();
        assert_eq!(taken.len(), MAX_REPORTS + 1);
        let first: Value = serde_json::from_str(&taken[0]).unwrap();
        assert_eq!(first, json!({ "type": "reports-dropped", "count": 11 }));
        assert_eq!(taken[1], json!({ "type": "challenged" }).to_string());
        let oldest_kept: Value = serde_json::from_str(&taken[2]).unwrap();
        assert_eq!(oldest_kept["index"], 11);
        let newest: Value = serde_json::from_str(taken.last().unwrap()).unwrap();
        assert_eq!(newest["index"], MAX_REPORTS + 9);
        assert!(queue.is_empty());
        assert!(queue.take().is_empty(), "the count is reported once");
    }

    /// Alice's full queue of challenges drops an ordinary arrival; a new
    /// challenge replaces the oldest. The bounded host queue owns that choice.
    #[test]
    fn a_full_queue_of_challenges_keeps_challenges_first() {
        let mut queue = ReportQueue::default();
        for index in 0..MAX_REPORTS {
            queue.push(json!({ "type": "challenged", "index": index }));
        }
        queue.push(json!({ "type": "unreadable" }));
        assert!(queue.waiting.iter().all(|(challenge, _)| *challenge));
        queue.push(json!({ "type": "challenged", "index": MAX_REPORTS }));
        let reports = queue.take();
        assert_eq!(reports.len(), MAX_REPORTS + 1);
        assert_eq!(
            reports[0],
            json!({ "type": "reports-dropped", "count": 2 }).to_string()
        );
        for (index, report) in reports.iter().enumerate().skip(1) {
            assert_eq!(
                *report,
                json!({ "type": "challenged", "index": index }).to_string()
            );
        }
    }

    /// Contract: a protocol failure that has a code of its own reaches JS
    /// with that code, so a caller can tell an expected refusal (nothing to
    /// recover from) from a failure; one without keeps `protocol`.
    #[test]
    fn protocol_failures_carry_their_own_code() {
        use jazz_e2ee::protocol::error::{ProtocolError, RecoveryFailure};
        let described = describe(ClientError::Protocol(ProtocolError::Recovery(
            RecoveryFailure::RootMismatch,
        )));
        assert!(
            described.starts_with("recovery-root-mismatch: "),
            "{described}"
        );
        let described = describe(ClientError::Protocol(ProtocolError::ProviderFailure));
        assert!(described.starts_with("protocol: "), "{described}");
    }

    /// Alice's host receives the skipped record, its signer and the capability
    /// needed to check it. The JSON serializer defines this binding contract.
    #[test]
    fn an_unsupported_account_key_names_its_signer_and_capability_in_json() {
        let report = Report::UnsupportedAccountKey {
            record_id: "record".into(),
            signer_id: "signer".into(),
            capability: jazz_e2ee::protocol::error::UnsupportedCapability::SigningVersion,
        };
        assert_eq!(
            report_json(&report, |_| None),
            json!({
                "type": "unsupported-account-key", "recordId": "record",
                "signerId": "signer", "capability": "signing-version"
            })
        );
    }

    /// A report on an obsolete scope still identifies the space after its table went away.
    #[test]
    fn a_report_preserves_an_unknown_scope() {
        let at = SpaceAddress {
            scope_id: uuid::Uuid::new_v4().to_string(),
            identifier: uuid::Uuid::new_v4().to_string(),
        };
        let report = Report::RotationFailed {
            space: at.clone(),
            reason: "offline".into(),
        };
        assert_eq!(
            report_json(&report, |_| None)["space"],
            json!({ "scopeId": at.scope_id, "id": at.identifier })
        );
    }

    /// Contract: reports reach JS as JSON with a stable `type`, and the
    /// unreadable ones carry the protocol's code names.
    #[test]
    fn reports_are_json_with_the_protocols_names() {
        let report = Report::Unreadable(jazz::e2ee::Unreadable {
            table: "notes".to_owned(),
            id: None,
            column: Some("body".to_owned()),
            code: jazz::e2ee::UnreadableCode::KeyPending,
            space: None,
        });
        let json = report_json(&report, |_| None);
        assert_eq!(json["type"], "unreadable");
        assert_eq!(json["code"], "key-pending");
        assert_eq!(json["column"], "body");
    }
}
