//! End-to-end encryption on Node: the runtime installed on a database whose
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
use jazz_e2ee::protocol::device_account::DeviceAccountIdentity;

use super::bridge::JsStore;
use serde_json::{Value, json};
use zeroize::Zeroizing;

/// What a host passes to open an encrypted app's database.
pub struct Options {
    /// The app's `jazz.app.json`, or its generated bindings' compiled app
    /// (`from_bindings`), from which the declarations are read.
    pub app_file: Vec<u8>,
    pub from_bindings: bool,
    /// The runtime derives the founding signing key from this secret. Native hosts
    /// also use the secret to register or restore recovery at startup.
    /// `None` for an account the app signs in itself.
    pub device_account_secret: Option<RecoverySecret>,
    pub app_id: String,
    pub environment: String,
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

/// The runtime and what the host keeps beside it.
pub struct Encryption {
    pub client: Rc<E2eeClient>,
    pub store: Rc<JsStore>,
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
    gate: Gate,
    /// The loop, polled by the host.
    main: RefCell<Option<LocalBoxFuture<'static, ()>>>,
}

/// One runtime call at a time: two calls that each hold a database
/// transaction open across awaits would lock one another out (the store is
/// busy), so the loop and the calls from JavaScript take turns.
#[derive(Default)]
struct Gate(Cell<bool>);

struct Turn<'a>(&'a Gate);

impl Drop for Turn<'_> {
    fn drop(&mut self) {
        self.0.0.set(false);
    }
}

impl Gate {
    async fn enter(&self) -> Turn<'_> {
        std::future::poll_fn(|context| {
            if self.0.replace(true) {
                context.waker().wake_by_ref();
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

/// The runtime for an encrypted app, built from the app's own declarations
/// over the private store the app supplied (a JavaScript store, see
/// `js_store`), for the account of its session.
/// The future owns the parsed secret so the host can wipe its options on drop.
pub fn build(
    options: &Options,
    account_id: String,
    store: Rc<JsStore>,
) -> impl Future<Output = Result<Built, String>> + use<> {
    let recovery = options.device_account_secret.clone();
    let declarations = declarations(options);
    async move { build_client(declarations?, account_id, store, recovery.as_ref()).await }
}

/// Build a native device runtime that handles account-secret recovery at
/// startup. When a keyless relay supplies its declarations, the device must
/// use those same declarations, including the app and environment.
pub async fn build_native(
    mut options: Options,
    account_id: String,
    store: Rc<JsStore>,
    relay_declarations: Option<&E2eeDeclarations>,
) -> Result<NativeBuilt, String> {
    let recovery = options.device_account_secret.take();
    let declarations = declarations(&options)?;
    if relay_declarations.is_some_and(|expected| *expected != declarations) {
        return Err("the device's encryption declarations do not match the relay".to_owned());
    }
    let built = build_client(declarations, account_id, store, recovery.as_ref()).await?;
    Ok(NativeBuilt { built, recovery })
}

fn declarations(options: &Options) -> Result<E2eeDeclarations, String> {
    let app_id = jazz::tools::AppId::from_string(&options.app_id)
        .unwrap_or_else(|_| jazz::tools::AppId::from_name(&options.app_id));
    let declarations = if options.from_bindings {
        jazz::tools::deployment::artifact::bound_declarations(
            &options.app_file,
            app_id,
            &options.environment,
        )
        .map_err(|error| error.to_string())?
    } else {
        jazz::tools::deployment::artifact::load_client_encrypted(
            &options.app_file,
            app_id,
            &options.environment,
        )
        .map_err(|error| error.to_string())?
        .2
    };
    declarations.ok_or_else(|| "the app's files declare no encryption".to_owned())
}

async fn build_client(
    declarations: E2eeDeclarations,
    account_id: String,
    store: Rc<JsStore>,
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
    let device_account = recovery
        .map(DeviceAccountIdentity::from_secret)
        .transpose()
        .map_err(|error| error.to_string())?;
    let store: Rc<dyn PrivateStore> = store;
    let client = E2eeClient::open(ClientConfig {
        store,
        declarations,
        account_id,
        registry: jazz_e2ee::registry::Registry::builtin(),
        random: Box::new(OsRandom),
        device_account,
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
        Self::start(built, store, db, None)
    }

    pub fn new_native(built: NativeBuilt, store: Rc<JsStore>, db: Rc<Db>) -> Rc<Self> {
        let native = NativeReadiness {
            recovery: RefCell::new(built.recovery),
            result: RefCell::new(Verification::Pending),
            observed_initial: Cell::new(false),
            requested: Cell::new(0),
            started: Cell::new(0),
            finished: Cell::new(0),
        };
        Self::start(built.built, store, db, Some(native))
    }

    fn start(
        built: Built,
        store: Rc<JsStore>,
        db: Rc<Db>,
        native: Option<NativeReadiness>,
    ) -> Rc<Self> {
        let Built {
            spaces,
            client,
            reports,
        } = built;
        let encryption = Rc::new(Self {
            client,
            store,
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
            gate: Gate::default(),
            main: RefCell::default(),
        });
        let running = Rc::clone(&encryption);
        *encryption.main.borrow_mut() = Some(Box::pin(main_loop(running)));
        encryption
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
            self.store.request_wake();
        }
    }

    /// Lets the loop run as far as it can without waiting.
    pub fn drive(&self) {
        // The host gets reports through its listener. Discard the raw polling
        // queue even when the app never asks for the listener's reports.
        self.client.shared.reports.borrow_mut().clear();
        let mut main = self.main.borrow_mut();
        let Some(future) = main.as_mut() else {
            return;
        };
        let mut context = Context::from_waker(Waker::noop());
        if future.as_mut().poll(&mut context).is_ready() {
            *main = None;
        }
        drop(main);
        self.wake_if_owed();
    }

    /// Ends the loop (the host is closing): it holds this runtime, so it
    /// must be dropped for the runtime to be.
    pub fn stop(&self) {
        *self.main.borrow_mut() = None;
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

    /// The reports waiting, as JSON texts.
    pub fn take_reports(&self) -> Vec<String> {
        self.client.flush_reports();
        self.reports.borrow_mut().take()
    }

    /// An app subscription on an encrypted table opened (`true`) or closed
    /// (`false`); `space` is `table` identity and row of the space.
    pub fn use_space(&self, open: bool, table: String, id: String) {
        self.uses.borrow_mut().push_back((open, table, id));
    }

    /// The connection to the server went away (`false`) or came back
    /// (`true`): offline, equality queries match every row of a space; back
    /// online, the loop verifies the account and runs the delivery pass.
    pub fn set_online(&self, online: bool) {
        self.client.set_online(online);
        if online {
            self.reconnected.set(true);
        }
    }

    /// The address of the space `id` of the space table `table`.
    async fn space_address(&self, table: &str, id: &str) -> Result<SpaceAddress, String> {
        let identity = self
            .db
            .table_identity(table)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("the schema has no table `{table}`"))?;
        Ok(SpaceAddress {
            scope_id: identity.0.to_string(),
            identifier: id.to_owned(),
        })
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
            Verification::Failed(error) => Some(Err(describe_native(error))),
            Verification::AwaitingRecovery if expired => Some(Ok("awaiting-approval")),
            Verification::Retry(error) if expired => Some(Err(describe_native(error))),
            _ if expired => Some(Err(self.readiness_timeout())),
            _ => None,
        }
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
                    Verification::Failed(error) => Err(describe_native(error)),
                    Verification::Complete => Err(describe(ClientError::NoAccount)),
                },
                None => Err(describe(ClientError::NoAccount)),
            },
            outcome => outcome.map_err(|error| describe_native(&error)),
        }
    }

    /// The startup recovery step for native hosts. `None` means the device
    /// is awaiting approval and should try automatic recovery again.
    async fn verify_native(
        &self,
        native: &NativeReadiness,
        status: AccountStatus,
    ) -> Result<Option<AccountStatus>, ClientError> {
        let Some(secret) = native.recovery.borrow().clone() else {
            return Ok(Some(status));
        };
        match status {
            // The start-up registration of the device account's own secret
            // (the runtime was opened with the identity that secret derives).
            AccountStatus::Active => self
                .client
                .start_device_account(&self.db, &secret)
                .await
                .map(Some),
            // Not what `start_device_account` does: a device awaiting
            // approval recovers with the secret when the root and its
            // delivery are there, and waits (`None`) while they are not.
            AccountStatus::AwaitingApproval => {
                match self
                    .client
                    .recover_device_when_available(&self.db, &secret)
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
        let text = describe_native(&error);
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
        Box::pin(async move {
            this.run(&op, &args, secret.as_ref().map(|text| text.as_str()))
                .await
        })
    }

    async fn run(&self, op: &str, args: &Value, secret: Option<&str>) -> Result<Value, String> {
        if op == "shutdown" {
            // The loop can be holding the store lock while waiting for its
            // save to settle. Keep driving it until the client is closed.
            self.client.shutdown().await;
            self.stop();
            return Ok(Value::Null);
        }
        self.client.ensure_open().map_err(describe)?;
        // The delivery pass belongs to the loop, which takes its turn itself.
        // A space's row is a local catalogue lookup: it never waits for the
        // loop's work.
        let _turn = match op {
            "keysDeliver" | "prepareSpace" | "retryWithCurrentKey" | "spaceRow" => None,
            _ => Some(self.gate.enter().await),
        };
        let (client, db) = (&self.client, &*self.db);
        client.ensure_open().map_err(describe)?;
        let text = |name: &str| -> Result<String, String> {
            args.get(name)
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| format!("`{name}` is missing"))
        };
        Ok(match op {
            "deviceId" => json!(client.device().map_err(describe)?.id.clone()),
            "ready" => {
                let status = client.ensure_account(db).await.map_err(describe)?;
                self.status.set(Some(status));
                json!(status_name(status))
            }
            "resetAccount" => {
                let status = client.reset_account(db).await.map_err(describe)?;
                self.status.set(Some(status));
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
            // `automatic`: the start-up registration of a device account's
            // own secret, for which a retired root is not an error. As in
            // `start_device_account`, a secret of another identity than the
            // device account the runtime was opened as is refused
            // (`recovery-root-mismatch`) and nothing is written. Otherwise a
            // retired root is refused (`recovery-root-mismatch`).
            "recoveryRegister" => {
                let secret = recovery_secret(args, secret)?;
                if args.get("automatic").and_then(Value::as_bool) == Some(true) {
                    match client.refuse_foreign_device_secret(&secret) {
                        Ok(()) => client.register_recovery_at_start(db, &secret).await,
                        Err(refused) => Err(refused),
                    }
                } else {
                    client.register_recovery(db, &secret).await
                }
                .map_err(describe)?;
                Value::Null
            }
            // `automatic`: the device's own recovery with its account secret,
            // as native readiness runs it. `null`: nothing to recover from
            // (yet), so the device stays awaiting approval.
            "recoveryRestore" => {
                let secret = recovery_secret(args, secret)?;
                let restored = if args.get("automatic").and_then(Value::as_bool) == Some(true) {
                    match client.recover_device_when_available(db, &secret).await {
                        Err(ClientError::ApprovalAnswered) => {
                            client.ensure_account(db).await.map(Some)
                        }
                        outcome => outcome,
                    }
                } else {
                    client.recover_device(db, &secret).await.map(Some)
                };
                match restored.map_err(describe)? {
                    Some(status) => {
                        self.status.set(Some(status));
                        json!(status_name(status))
                    }
                    None => Value::Null,
                }
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
                // The pass belongs to the loop; if it cannot run (its watch
                // keeps failing) the caller gets the loop's error, not a hang.
                match within(answer, DELIVER_WITHIN).await {
                    Some(done) => done.map_err(|_| describe(ClientError::Closed))??,
                    None => {
                        let why = self.last_error.borrow().clone();
                        return Err(why.unwrap_or_else(|| {
                            "database: the delivery pass did not run in time".to_owned()
                        }));
                    }
                }
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
                break;
            }
            Err(error) => {
                if let Some(native) = &this.native {
                    if !native_may_retry(&error) {
                        *this.last_error.borrow_mut() = Some(describe_native(&error));
                        this.record_native_failure(native, error);
                        return;
                    }
                    *native.result.borrow_mut() = Verification::Retry(error);
                }
            }
            _ => {}
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
    // A device awaiting approval looks for a challenge now and then: the
    // challenge rows are not among the account's watched tables.
    let mut noticed = false;
    let mut looked = web_time::Instant::now();
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
            // Back online: verify the account again and run the pass at the
            // natural moment once the subscriptions have caught up.
            verify = true;
            start = true;
        }
        if retry_at.is_some_and(|at| web_time::Instant::now() >= at) {
            retry_at = None;
            verify = true;
        }
        if let Some(native) = &this.native {
            verify |= native.requested.get() > native.started.get();
        }
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
                this.status.set(Some(*status));
                this.stage.set("preparing account-secret recovery");
                verified = Box::pin(this.verify_native(native, *status)).await;
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
                    this.status.set(Some(status));
                    *this.last_error.borrow_mut() = None;
                    if let Some(native) = &this.native
                        && !matches!(*native.result.borrow(), Verification::Failed(_))
                    {
                        native.recovery.borrow_mut().take();
                        *native.result.borrow_mut() = Verification::Complete;
                    }
                    if status == AccountStatus::Active {
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
                        describe_native(&error)
                    } else {
                        describe(error)
                    });
                }
            }
        }
        if this.status.get() == Some(AccountStatus::AwaitingApproval)
            && looked.elapsed() >= std::time::Duration::from_secs(2)
        {
            looked = web_time::Instant::now();
            match Box::pin(client.challenged(&db)).await {
                Ok(true) if !noticed => {
                    noticed = true;
                    this.reports
                        .borrow_mut()
                        .push(json!({ "type": "challenged" }));
                }
                Ok(false) => noticed = false,
                _ => {}
            }
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
                if let Err(error) = Box::pin(client.sync_spaces(&db, watch)).await {
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
                // At the natural moments only (start, reconnect, the app's
                // call): a change a watch sees was served by `sync_spaces`.
                if start {
                    start = false;
                    if let Err(error) = client.deliver_keys(&db).await {
                        *this.last_error.borrow_mut() = Some(describe(error));
                    }
                }
            }
        }
        this.stage.set("idle");
        drop(turn);
        yield_now().await;
    }
}

/// How long a call that waits for the loop's delivery pass may wait.
const DELIVER_WITHIN: std::time::Duration = std::time::Duration::from_secs(60);

/// `future`'s output, or `None` once `limit` has passed.
async fn within<F: Future + Unpin>(mut future: F, limit: std::time::Duration) -> Option<F::Output> {
    let deadline = web_time::Instant::now() + limit;
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
            || crate::account::conflict(error))
}

fn describe_native(error: &ClientError) -> String {
    if let ClientError::Protocol(error) = error
        && let Some(code) = error.code()
    {
        return format!("{code}: {error}");
    }
    describe(error.clone())
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

/// Read a recovery phrase, a formatted device account secret for automatic
/// recovery, or a hexadecimal backup secret for explicit recovery.
fn recovery_secret(args: &Value, text: Option<&str>) -> Result<RecoverySecret, String> {
    if let Some(phrase) = args.get("phrase").and_then(Value::as_str) {
        return super::phrase::to_secret(phrase)
            .map(|bytes| RecoverySecret::from_bytes(*bytes))
            .map_err(|error| format!("{}: the recovery phrase is not valid", error.code()));
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
mod tests {
    use super::*;

    fn run<T>(future: impl Future<Output = T>) -> T {
        jazz::db::block_on(future)
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

    /// Contract: an encrypted app is installed from its own declarations: an
    /// app file without any encryption is refused, and one that declares it
    /// builds the runtime over the store the app supplied (here answered by
    /// the test as JavaScript would).
    #[test]
    fn the_runtime_is_built_from_the_apps_own_declarations() {
        let plain = br#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
            "tables": { "projects": { "columns": { "name": "string" } } } }"#;
        let options = |file: &[u8]| Options {
            app_file: file.to_vec(),
            from_bindings: false,
            device_account_secret: None,
            app_id: "napi-encryption".to_owned(),
            environment: "test".to_owned(),
        };
        let account = uuid::Uuid::new_v4().to_string();
        let store = Rc::new(JsStore::default());
        assert!(run(build(&options(plain), account.clone(), store.clone())).is_err());
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
        let memory = crate::MemoryStore::new();
        let mut building = Box::pin(build(&options(encrypted), account.clone(), store.clone()));
        let mut context = std::task::Context::from_waker(Waker::noop());
        let built = loop {
            if let Poll::Ready(built) = building.as_mut().poll(&mut context) {
                break built.unwrap();
            }
            // JavaScript's side: run each request against a store.
            for text in store.take_requests() {
                let request: Value = serde_json::from_str(&text).unwrap();
                let (ok, payload) = answer(&memory, &request);
                assert!(store.reply(
                    request["id"].as_str().unwrap().parse().unwrap(),
                    ok,
                    &payload,
                ));
            }
        };
        assert_eq!(built.client.account_id(), account);
        assert!(built.reports.borrow().is_empty());
    }

    fn native_runtime_with_catalogue(include_notes: bool) -> Rc<Encryption> {
        use jazz::db::{DbConfig, DbIdentity};
        use jazz::ids::{AuthorSubject, NodeUuid};
        use jazz::schema::JazzSchema;
        use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

        let options = Options {
            app_file: br#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
                "tables": {
                    "projects": { "columns": { "name": "string" } },
                    "notes": {
                        "columns": { "projectId": "uuid", "body": "bytes" },
                        "relations": { "project": { "rel": "projects", "column": "projectId" } },
                        "encryption": { "space": "projectId", "scope": "projects",
                            "columns": { "body": "string" } }
                    }
                } }"#
                .to_vec(),
            from_bindings: false,
            device_account_secret: None,
            app_id: "terminal-catalogue".to_owned(),
            environment: "test".to_owned(),
        };
        let store = Rc::new(JsStore::served_by(super::super::bridge::serve(Rc::new(
            crate::MemoryStore::new(),
        ))));
        let built = run(build_native(
            options,
            uuid::Uuid::new_v4().to_string(),
            Rc::clone(&store),
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
        jazz::db::block_on(db.set_authority()).unwrap();
        Encryption::new_native(built, store, db)
    }

    /// A space's row answers from the local catalogue, even while the loop's
    /// work holds the runtime (verification, a delivery pass).
    #[test]
    fn a_space_row_answers_while_the_loop_is_busy() {
        let runtime = native_runtime_with_catalogue(true);
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
        assert!(
            row["scopeId"]
                .as_str()
                .is_some_and(|scope| !scope.is_empty())
        );
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
        let closed = describe_native(&ClientError::Closed);
        assert_eq!(
            runtime.foreground_ready_outcome(first, false),
            Some(Err(closed.clone()))
        );
        assert_eq!(
            runtime.foreground_ready_outcome(second, true),
            Some(Err(closed))
        );
    }

    /// What the app's store answers a request with, as the TypeScript side
    /// does it: here over an in-memory store.
    fn answer(store: &crate::MemoryStore, request: &Value) -> (bool, String) {
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD;
        let text = |name: &str| request[name].as_str().unwrap().to_owned();
        let number = |name: &str| request[name].as_str().unwrap().parse::<u64>().unwrap();
        let result: Result<Value, crate::StoreError> = match request["op"].as_str() {
            Some("openScope") => {
                run(store.open_scope(&text("scope"))).map(|n| json!(n.to_string()))
            }
            Some("load") => {
                run(store.load(&text("scope"), &text("doc"))).map(|found| match found {
                    None => Value::Null,
                    Some(found) => json!({
                        "bytes": STANDARD.encode(found.bytes),
                        "revision": found.revision.to_string(),
                    }),
                })
            }
            Some("save") => {
                let bytes = STANDARD.decode(text("bytes")).unwrap();
                let expected = request["expected"].as_str().map(|n| n.parse().unwrap());
                run(store.save(
                    &text("scope"),
                    number("incarnation"),
                    &text("doc"),
                    &bytes,
                    expected,
                ))
                .map(|n| json!(n.to_string()))
            }
            Some("list") => run(store.list(&text("scope"))).map(|names| json!(names)),
            Some("delete") => run(store.delete(&text("scope"))).map(|()| Value::Null),
            Some("protect") => {
                let secret = STANDARD.decode(text("secret")).unwrap();
                run(store.protect(&secret)).map(|blob| json!(STANDARD.encode(blob)))
            }
            Some("unprotect") => {
                let blob = STANDARD.decode(text("blob")).unwrap();
                run(store.unprotect(&blob)).map(|secret| json!(STANDARD.encode(&*secret)))
            }
            other => panic!("unknown store request {other:?}"),
        };
        match result {
            Ok(value) => (true, value.to_string()),
            Err(crate::StoreError::Conflict) => (false, "conflict".to_owned()),
            Err(crate::StoreError::Wiped) => (false, "wiped".to_owned()),
            Err(error) => (false, error.to_string()),
        }
    }

    /// Alice's host delivers each report once and drains the client's raw copy
    /// on its next drive. Only the client can expose whether that copy survived.
    #[test]
    fn driving_the_host_drains_the_clients_report_queue() {
        let runtime = native_runtime_with_catalogue(true);
        runtime.client.shared.report(Report::SkippedRecord {
            document: "device".into(),
            reason: "invalid".into(),
        });
        runtime.drive();
        assert!(runtime.client.take_reports().is_empty());
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
