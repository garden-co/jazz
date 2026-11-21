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

use crate::state::{RETRY_FIRST, RETRY_LAST};
use crate::{
    AccountStatus, ClientConfig, ClientError, DeviceStatus, E2eeClient, PendingApproval,
    PrivateStore, Report,
};
use futures::future::LocalBoxFuture;
use jazz::db::Db;
use jazz::e2ee::SpaceAddress;
use jazz_e2ee::OsRandom;
use jazz_e2ee::protocol::derive::RecoverySecret;
use jazz_e2ee::protocol::device_account::DeviceAccountIdentity;

use super::bridge::JsStore;
use serde_json::{Value, json};

/// What a host passes to open an encrypted app's database.
pub struct Options {
    /// The app's `jazz.app.json`, or its generated bindings' compiled app
    /// (`from_bindings`), from which the declarations are read.
    pub app_file: Vec<u8>,
    pub from_bindings: bool,
    /// The device account's secret as 64 hexadecimal characters: the
    /// runtime keeps only the signing key it derives, for founding the
    /// account. `None` for an account the app signs in itself.
    pub device_account_secret: Option<String>,
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
/// oldest report goes (never a challenge, which the device must answer)
/// and the next take starts with one `reports-dropped` report counting
/// those that went.
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
            let oldest = self
                .waiting
                .iter()
                .position(|(challenge, _)| !challenge)
                .unwrap_or(0);
            self.waiting.remove(oldest);
            self.dropped += 1;
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
    last_error: RefCell<Option<String>>,
    /// What the loop is doing or waiting on, for a call that waits on it.
    stage: Cell<&'static str>,
    /// A wake was asked for the batch that waits (see [`Encryption::wake_if_owed`]).
    woken: Cell<bool>,
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
pub async fn build(
    options: Options,
    account_id: String,
    store: Rc<JsStore>,
) -> Result<Built, String> {
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
    let declarations = declarations.ok_or("the app's files declare no encryption")?;
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
    let device_account = match &options.device_account_secret {
        Some(hex) => Some(
            DeviceAccountIdentity::from_secret(&secret(hex)?).map_err(|error| error.to_string())?,
        ),
        None => None,
    };
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
    client.on_report(move |report| sink.borrow_mut().push(report_json(report)));
    Ok(Built {
        spaces,
        client,
        reports,
    })
}

impl Encryption {
    pub fn new(built: Built, store: Rc<JsStore>, db: Rc<Db>) -> Rc<Self> {
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
            last_error: RefCell::default(),
            stage: Cell::new("starting"),
            woken: Cell::new(false),
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
        if !self.db.owes_requests_sent() {
            self.woken.set(false);
        } else if !self.woken.replace(true) {
            self.store.request_wake();
        }
    }

    /// Lets the loop run as far as it can without waiting.
    pub fn drive(&self) {
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
    fn space_address(&self, table: &str, id: &str) -> Result<SpaceAddress, String> {
        let identity = self
            .db
            .table_identity(table)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("the schema has no table `{table}`"))?;
        Ok(SpaceAddress {
            scope_id: identity.0.to_string(),
            identifier: id.to_owned(),
        })
    }

    /// The loop's last account status and error, as JSON.
    pub fn state(&self) -> Value {
        json!({
            "status": self.status.get().map(status_name),
            "error": self.last_error.borrow().clone(),
            "stage": self.stage.get(),
        })
    }

    /// One call from JS: the operation's name and its arguments, and its
    /// result as JSON.
    pub fn call(
        self: &Rc<Self>,
        op: String,
        args: Value,
    ) -> LocalBoxFuture<'static, Result<Value, String>> {
        let this = Rc::clone(self);
        Box::pin(async move { this.run(&op, &args).await })
    }

    async fn run(&self, op: &str, args: &Value) -> Result<Value, String> {
        // The delivery pass belongs to the loop, which takes its turn itself.
        let _turn = match op {
            "keysDeliver" => None,
            _ => Some(self.gate.enter().await),
        };
        let (client, db) = (&self.client, &*self.db);
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
            "devices" => json!(
                client
                    .devices(db)
                    .await
                    .map_err(describe)?
                    .into_iter()
                    .map(|device| json!({
                        "id": device.id,
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
                self.approvals.borrow_mut().insert(device.clone(), pending);
                json!({ "deviceId": device })
            }
            "answer" => json!(client.answer_challenge(db).await.map_err(describe)?),
            "approvalCode" => {
                let device = text("deviceId")?;
                let pending = self.approvals.borrow_mut().remove(&device);
                let pending = pending.ok_or("no approval was started for this device")?;
                let code = client.approval_code_of(db, &pending).await;
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
            // `atStart`: the registration a device account runs at each
            // start with its own secret. It writes nothing when the root is
            // registered already, and a retired root is not an error there
            // (the device still starts), where an explicit call refuses it.
            "recoveryRegister" => {
                let secret = secret(&text("secret")?)?;
                let registered = match args.get("atStart").and_then(Value::as_bool) {
                    Some(true) => client.register_recovery_at_start(db, &secret).await,
                    _ => client.register_recovery(db, &secret).await,
                };
                registered.map_err(describe)?;
                Value::Null
            }
            "recoveryRestore" => {
                let status = client
                    .recover_device(db, &secret(&text("secret")?)?)
                    .await
                    .map_err(describe)?;
                self.status.set(Some(status));
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
            "prepareSpace" => {
                let space = SpaceAddress {
                    scope_id: text("scopeId")?,
                    identifier: text("identifier")?,
                };
                json!(client.prepare_space(db, &space).await.map_err(describe)?)
            }
            "spaceRow" => {
                let space = self.space_address(&text("table")?, &text("id")?)?;
                json!({ "scopeId": space.scope_id, "identifier": space.identifier })
            }
            "keysRotate" => {
                let space = self.space_address(&text("table")?, &text("id")?)?;
                let delivered = client.rotate(db, &space).await.map_err(describe)?;
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
    while !client.prepare(&db).unwrap_or(false) {
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
            match client.watch_account(&db).await {
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
        if verify {
            verify = false;
            this.stage.set("verifying the account");
            match client.ensure_account(&db).await {
                Ok(status) => {
                    backoff = RETRY_FIRST;
                    this.status.set(Some(status));
                    *this.last_error.borrow_mut() = None;
                    if status == AccountStatus::Active {
                        match client.complete_removal_requests(&db).await {
                            Ok(_) => {}
                            Err(error) => *this.last_error.borrow_mut() = Some(describe(error)),
                        }
                    }
                }
                Err(error) => {
                    if may_pass_later(&error) {
                        retry_at = Some(web_time::Instant::now() + backoff);
                        backoff = (backoff * 2).min(RETRY_LAST);
                    }
                    *this.last_error.borrow_mut() = Some(describe(error));
                }
            }
        }
        if this.status.get() == Some(AccountStatus::AwaitingApproval)
            && looked.elapsed() >= std::time::Duration::from_secs(2)
        {
            looked = web_time::Instant::now();
            match client.challenged(&db).await {
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
            && let Err(error) = client.settle_reads(&db).await
        {
            *this.last_error.borrow_mut() = Some(describe(error));
        }
        if this.status.get() == Some(AccountStatus::Active) {
            if spaces.is_none() {
                match client.watch_spaces(&db).await {
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
                    let Ok(space) = this.space_address(&table, &id) else {
                        continue;
                    };
                    if open {
                        watch.use_space(&space);
                    } else {
                        watch.release_space(&space);
                    }
                }
                if let Err(error) = client.sync_spaces(&db, watch).await {
                    *this.last_error.borrow_mut() = Some(describe(error));
                }
                loop {
                    let next = this.commands.borrow_mut().pop_front();
                    let Some(reply) = next else {
                        break;
                    };
                    let outcome = match client.deliver_keys(&db).await {
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

/// A recovery secret from its hexadecimal text.
fn secret(hex: &str) -> Result<RecoverySecret, String> {
    if hex.len() != 64 || !hex.is_ascii() {
        return Err("a recovery secret is 64 hexadecimal characters".to_owned());
    }
    let mut bytes = [0u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[2 * index..2 * index + 2], 16)
            .map_err(|_| "a recovery secret is hexadecimal".to_owned())?;
    }
    Ok(RecoverySecret::from_bytes(bytes))
}

/// A report as the JS events carry it.
fn report_json(report: &Report) -> Value {
    let space = |space: &SpaceAddress| json!({ "table": space.scope_id, "id": space.identifier });
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
        assert!(run(build(options(plain), account.clone(), store.clone())).is_err());
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
        let mut building = Box::pin(build(options(encrypted), account.clone(), store.clone()));
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
        let json = report_json(&report);
        assert_eq!(json["type"], "unreadable");
        assert_eq!(json["code"], "key-pending");
        assert_eq!(json["column"], "body");
    }
}
