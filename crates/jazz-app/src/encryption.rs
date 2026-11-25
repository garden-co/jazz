//! End-to-end encryption for a Rust app: what a device app and a backend
//! open an app that declares encryption with (`E2EE_PROTOCOL.md` §1.4, §4;
//! core hooks note H9, H10).
//!
//! - A **device** (an end user's client, [`crate::Config::user`]) opens with
//!   the runtime's hooks, built from the app's own declarations over a
//!   private store the app supplies ([`EncryptionConfig`]), and runs the
//!   shared host loop between the client's ticks.
//! - A **backend** ([`crate::Config::backend`], or a backend client) is never
//!   an encryption participant: it opens with keyless hooks, no store, no
//!   account. Encrypted writes are refused, encrypted cells are unreadable,
//!   plain data is normal.
//!
//! The runtime's futures live on the client's thread. [`Encryption`] is the
//! `Send` handle that reaches them with calls (the host's own JSON
//! operations, the same ones the Node and browser bindings make).

use std::time::Duration;

use jazz::tools::deployment::artifact::encryption::is_equality_token_column;
use serde_json::Value;

use crate::error::{Error, Result};
use crate::handle::Jazz;

/// What a device app supplies for end-to-end encryption: its private store
/// (where this device keeps its keys, protected at rest) and the
/// environment its cells belong to.
///
/// The store is the app's own: the library ships none. The platform's key
/// store is the natural home (Keychain on Apple, Android Keystore, an OS
/// key store elsewhere), for the secret that protects the files the store
/// keeps.
#[cfg(feature = "e2ee-client")]
#[derive(Clone)]
pub struct EncryptionConfig {
    pub(crate) environment: String,
    pub(crate) store: StoreSource,
}

#[cfg(feature = "e2ee-client")]
#[derive(Clone)]
pub(crate) enum StoreSource {
    /// A Rust store, made on the client's thread.
    Native(
        std::sync::Arc<dyn Fn() -> std::rc::Rc<dyn jazz_e2ee_client::PrivateStore> + Send + Sync>,
    ),
    /// A foreign store: each request as JSON text (the bridge's own
    /// contract: `{ "op", ... }`, bytes as base64, numbers as decimal text)
    /// answered with `ok` and a JSON payload, or the error text (`conflict`,
    /// `wiped`, or a message). It runs on the client's thread and blocks it
    /// while it works: the store-call deadline cannot cut it short, so a
    /// callback that never returns blocks the client thread for good,
    /// closing included.
    Calls(std::sync::Arc<dyn Fn(String) -> (bool, String) + Send + Sync>),
}

#[cfg(feature = "e2ee-client")]
impl EncryptionConfig {
    /// A Rust private store. `make` runs on the client's thread, once.
    pub fn store(
        make: impl Fn() -> std::rc::Rc<dyn jazz_e2ee_client::PrivateStore> + Send + Sync + 'static,
    ) -> Self {
        Self {
            environment: "dev".to_owned(),
            store: StoreSource::Native(std::sync::Arc::new(make)),
        }
    }

    /// A private store of a foreign language, answered request by request.
    /// See the [store callback contract](jazz_e2ee_client::host::bridge).
    /// `call` runs synchronously on the client's thread: one that never
    /// returns blocks that thread, closing included, as no deadline can
    /// interrupt it.
    pub fn store_calls(call: impl Fn(String) -> (bool, String) + Send + Sync + 'static) -> Self {
        Self {
            environment: "dev".to_owned(),
            store: StoreSource::Calls(std::sync::Arc::new(call)),
        }
    }

    /// The environment every cell's domain names (default `dev`): the
    /// same for every client of one deployment.
    pub fn environment(mut self, environment: impl Into<String>) -> Self {
        self.environment = environment.into();
        self
    }
}

#[cfg(feature = "e2ee-client")]
impl std::fmt::Debug for EncryptionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionConfig")
            .field("environment", &self.environment)
            .finish_non_exhaustive()
    }
}

/// How a client takes part in end-to-end encryption.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Participation {
    /// The app declares none.
    No,
    /// A backend: relays and serves, holds no keys.
    #[cfg_attr(not(feature = "e2ee-client"), allow(dead_code))]
    Keyless,
    /// A device: holds keys.
    Device,
}

/// The message of a refusal to open an encrypted app without what it needs.
pub(crate) const NEEDS_DEVICE: &str = "this app declares end-to-end encryption: open it as an end user with \
     Config::encryption (a private store this app supplies, see EncryptionConfig), or as the \
     app's backend, which holds no keys";

pub(crate) use plan::Plan;
#[cfg(feature = "unstable-endpoint")]
pub(crate) use plan::keyless;

#[cfg(not(feature = "e2ee-client"))]
mod plan {
    use jazz::tools::public_schema::E2eeDeclarations;

    use super::*;

    /// With the `e2ee-client` feature off a client cannot open an
    /// encrypted app: it has no hooks.
    pub(crate) struct Plan;

    impl Plan {
        pub(crate) fn new(
            _declarations: Option<jazz::tools::public_schema::E2eeDeclarations>,
            _config: &crate::Config,
        ) -> Self {
            Plan
        }
        pub(crate) fn participation(&self, _backend: bool) -> Participation {
            Participation::No
        }
        pub(crate) fn declarations(&self) -> Option<std::sync::Arc<E2eeDeclarations>> {
            None
        }
        pub(crate) fn keyless(&self) -> Option<jazz::tools::ClientEncryption> {
            None
        }
        pub(crate) async fn device(
            &self,
            _account: crate::AccountId,
            _secret: Option<&crate::DeviceKey>,
        ) -> Result<Option<jazz::tools::ClientEncryption>> {
            Ok(None)
        }
    }

    /// With the feature off there are no keyless hooks.
    #[cfg_attr(not(feature = "unstable-endpoint"), allow(dead_code))]
    pub(crate) fn keyless(
        _declarations: Option<E2eeDeclarations>,
    ) -> Option<jazz::tools::ClientEncryption> {
        None
    }
}

#[cfg(feature = "e2ee-client")]
mod plan {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;

    use jazz::tools::public_schema::E2eeDeclarations;
    use jazz::tools::{ClientEncryption, EncryptionRuntime};
    use jazz_e2ee_client::PrivateStore;
    use jazz_e2ee_client::host::bridge::{DeadlineStore, JsStore};
    use jazz_e2ee_client::host::runtime as host;

    use super::*;

    thread_local! {
        /// The runtime of the client on this thread.
        static DEVICE: RefCell<Option<Rc<host::Encryption>>> = const { RefCell::new(None) };
        /// Told after each tick of the client on this thread: what calls
        /// waiting on the runtime wait for (see `polled`).
        static TICKED: Rc<tokio::sync::Notify> = Rc::new(tokio::sync::Notify::new());
    }

    /// What the client needs to open an app that declares encryption.
    pub(crate) struct Plan {
        declarations: Option<E2eeDeclarations>,
        config: Option<EncryptionConfig>,
    }

    impl Plan {
        pub(crate) fn new(declarations: Option<E2eeDeclarations>, config: &crate::Config) -> Self {
            Self {
                declarations,
                config: config.encryption.clone(),
            }
        }

        pub(crate) fn declarations(&self) -> Option<Arc<E2eeDeclarations>> {
            self.declarations.clone().map(Arc::new)
        }

        pub(crate) fn participation(&self, backend: bool) -> Participation {
            match (&self.declarations, backend) {
                (None, _) => Participation::No,
                (Some(_), true) => Participation::Keyless,
                (Some(_), false) => Participation::Device,
            }
        }

        /// A backend's keyless hooks.
        pub(crate) fn keyless(&self) -> Option<ClientEncryption> {
            keyless(self.declarations.clone())
        }

        /// A device's runtime for `account`, over the app's store.
        pub(crate) async fn device(
            &self,
            account: crate::AccountId,
            secret: Option<&crate::DeviceKey>,
        ) -> Result<Option<ClientEncryption>> {
            let Some(declarations) = &self.declarations else {
                return Ok(None);
            };
            let Some(config) = &self.config else {
                return Err(Error::App(NEEDS_DEVICE.to_owned()));
            };
            let store: Rc<dyn PrivateStore> = match &config.store {
                StoreSource::Native(make) => Rc::new(DeadlineStore::new(make())),
                StoreSource::Calls(call) => {
                    let call = Arc::clone(call);
                    Rc::new(JsStore::served_by(Rc::new(move |text| {
                        let answer = call(text);
                        Box::pin(async move { answer })
                    })))
                }
            };
            // The secret goes straight into a `RecoverySecret`, which
            // zeroizes it when dropped: no hexadecimal or other copy.
            let device_account_secret = secret
                .map(|key| jazz_e2ee_client::host::RecoverySecret::from_bytes(key.to_bytes()));
            let built = host::build_native(
                declarations.clone(),
                account.0.to_string(),
                store,
                device_account_secret,
            )
            .await
            .map_err(|message| Error::Encryption {
                code: "store".to_owned(),
                message,
            })?;
            let hooks = built.client().hooks();
            let start: jazz::tools::StartEncryption = Box::new(move |db, wake| {
                let runtime = host::Encryption::new_native(built, None, db, wake);
                DEVICE.with(|device| {
                    *device.borrow_mut() = Some(Rc::clone(&runtime));
                });
                Rc::new(Driver(runtime))
            });
            Ok(Some(ClientEncryption {
                hooks,
                start: Some(start),
            }))
        }
    }

    /// The keyless hooks a backend's client opens an app declaring
    /// `declarations` with; `None` for an app that declares none.
    pub(crate) fn keyless(declarations: Option<E2eeDeclarations>) -> Option<ClientEncryption> {
        Some(ClientEncryption {
            hooks: jazz_e2ee_client::KeylessHooks::hooks(declarations?),
            start: None,
        })
    }

    struct Driver(Rc<host::Encryption>);

    impl EncryptionRuntime for Driver {
        fn after_tick(&self) {
            self.0.drive();
            TICKED.with(|ticked| ticked.notify_waiters());
        }

        fn begin_streaming_upload<'a>(
            &self,
            begin: &'a mut dyn FnMut() -> futures_util::future::LocalBoxFuture<
                'a,
                jazz::tools::Result<jazz::db::StreamingValueUpload>,
            >,
            ended: futures_util::future::LocalBoxFuture<'a, ()>,
        ) -> futures_util::future::LocalBoxFuture<
            'a,
            jazz::tools::Result<jazz::db::StreamingValueUpload>,
        > {
            Box::pin(retry_after_key(
                async || begin().await,
                |error| jazz::tools::JazzError::Write(error.to_string()),
                ended,
                || jazz::tools::JazzError::ChannelClosed,
            ))
        }

        fn stop(&self) {
            self.0.stop();
            DEVICE.with(|device| device.borrow_mut().take());
        }
    }

    /// The loop holds the runtime, so a client that is dropped (an open that
    /// failed after the runtime started, say) must end it, or the database it
    /// reads stays open for good.
    impl Drop for Driver {
        fn drop(&mut self) {
            self.0.stop();
        }
    }

    /// An operation's failure as this crate's error: the runtime names it
    /// `code: message`.
    fn failure(text: String) -> Error {
        match text.split_once(": ") {
            Some((code, message)) => Error::Encryption {
                code: code.to_owned(),
                message: message.to_owned(),
            },
            None => Error::Encryption {
                code: "error".to_owned(),
                message: text,
            },
        }
    }

    fn device() -> Result<Rc<host::Encryption>> {
        DEVICE
            .with(|device| device.borrow().clone())
            .ok_or_else(|| Error::Encryption {
                code: "closed".to_owned(),
                message: "the encryption runtime is not running".to_owned(),
            })
    }

    /// The longest a waiting call goes without polling its future again
    /// when nothing on the client ticks: the runtime's own deadlines and
    /// retry timers are read when it is polled, so a wait with the server out
    /// of reach is a slow timer, not a busy loop.
    const IDLE_POLL_MAX: Duration = Duration::from_millis(100);
    const IDLE_POLL_FIRST: Duration = Duration::from_millis(2);

    /// Polls `future` until it is done, driving the runtime's loop between
    /// polls. The runtime's futures register no waker (the host polls them),
    /// so the wait is for the client's next tick, which is when they can make
    /// progress (a store answer and a sync message each tick the client),
    /// or, while nothing ticks, a timer that backs off to [`IDLE_POLL_MAX`].
    async fn polled<T>(
        runtime: &host::Encryption,
        future: impl std::future::Future<Output = T>,
    ) -> T {
        let mut future = std::pin::pin!(future);
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        let mut idle = IDLE_POLL_FIRST;
        loop {
            // Made before the poll, so a tick between the poll and the wait
            // still wakes it (`notify_waiters` counts a `Notified` from when
            // it is made).
            let ticks = TICKED.with(Rc::clone);
            let ticked = ticks.notified();
            if let std::task::Poll::Ready(value) = future.as_mut().poll(&mut context) {
                runtime.wake_if_owed();
                return value;
            }
            runtime.drive();
            tokio::select! {
                () = ticked => idle = IDLE_POLL_FIRST,
                () = tokio::time::sleep(idle) => idle = (idle * 2).min(IDLE_POLL_MAX),
            }
        }
    }

    /// One call of the runtime, on the client's thread.
    pub(super) async fn call(op: String, args: Value) -> Result<Value> {
        let runtime = device()?;
        polled(&runtime, runtime.call(op, args))
            .await
            .map_err(failure)
    }

    pub(super) fn state() -> Result<Value> {
        Ok(device()?.state())
    }

    pub(super) fn take_reports() -> Result<Vec<String>> {
        Ok(device()?.take_reports())
    }

    pub(super) async fn ready(timeout: Duration) -> Result<String> {
        let runtime = device()?;
        polled(&runtime, runtime.ready_native(timeout))
            .await
            .map(str::to_owned)
            .map_err(failure)
    }

    /// Drives the startup verification while it waits on the server. The
    /// host loop owns the work and reports any refusal.
    pub(crate) async fn start() {
        let _ = ready(Duration::from_secs(60)).await;
    }

    /// Repeats `operation` while it is refused for want of a key the runtime
    /// is preparing. Its waits for the key end with `ended`: then the
    /// original refusal's error is `ended_error`.
    pub(crate) async fn retry_after_key<T, E: From<jazz::tools::JazzError>>(
        mut operation: impl AsyncFnMut() -> std::result::Result<T, jazz::tools::JazzError>,
        preparation_error: impl Fn(Error) -> E,
        ended: impl std::future::Future<Output = ()>,
        ended_error: impl Fn() -> E,
    ) -> Result<T, E> {
        use host::WriteRetry;
        use jazz::e2ee::E2eeRefusal;
        use jazz::tools::JazzError;

        let mut error = match operation().await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        let mut ended = std::pin::pin!(ended);
        let deadline = host::Encryption::write_deadline(Duration::ZERO);
        let mut repeated = false;
        let mut last = false;
        loop {
            let JazzError::KeyRefusal { refusal, original } = error else {
                return Err(error.into());
            };
            let Some(runtime) = DEVICE
                .with(|device| device.borrow().clone())
                .filter(|_| !last)
            else {
                return Err((*original).into());
            };
            let space = match refusal {
                E2eeRefusal::NeedsKey { space } | E2eeRefusal::SpacePending { space } => space,
            };
            // Native preparation runs on the client thread and wakes its
            // waiter on completion, even between database ticks.
            let retry = runtime.retry_write_with(&space, deadline, repeated, |preparation| {
                let runtime = Rc::clone(&runtime);
                tokio::task::spawn_local(async move {
                    polled(&runtime, preparation).await;
                    TICKED.with(|ticked| ticked.notify_waiters());
                });
            });
            // The preparation goes on in its own task; only this wait ends.
            let decision = tokio::select! {
                biased;
                decision = polled(&runtime, retry) => decision,
                () = ended.as_mut() => return Err(ended_error()),
            }
            .map_err(|reason| preparation_error(failure(reason)))?;
            if decision == WriteRetry::Stop {
                return Err((*original).into());
            }
            last = decision == WriteRetry::Last;
            repeated = true;
            match operation().await {
                Ok(value) => return Ok(value),
                Err(next) => error = next,
            }
        }
    }
}

/// The handle to a device's encryption runtime ([`Jazz::encryption`]).
///
/// It is always there: for an app that declares no encryption, and for a
/// backend (which is never an encryption participant), every use is refused.
/// Calls are the runtime's operations by name, the same JSON ones the Node
/// and browser bindings make (`ready`, `devices`, `challenge`, `answer`,
/// `approvalCode`, `approve`, `remove`, `requestRemoval`, `completeRemovals`,
/// `recoveryRegister`, `recoveryRestore`, `recoveryRetire`, `recoveryList`,
/// `keysDeliver`, `keysRotate`, `deviceId`); [`Encryption::ready`] also
/// registers or uses the account secret's recovery.
#[derive(Clone)]
pub struct Encryption {
    pub(crate) jazz: Jazz,
}

macro_rules! with_runtime {
    ($encryption:expr, $arguments:expr, $job:expr) => {{
        if let Some(refusal) = $encryption.refusal() {
            return Err(refusal);
        }
        #[cfg(feature = "e2ee-client")]
        {
            $encryption.jazz.with_client($job).await
        }
        #[cfg(not(feature = "e2ee-client"))]
        {
            let _ = $arguments;
            unreachable!("no runtime without the feature")
        }
    }};
}

impl Encryption {
    fn refusal(&self) -> Option<Error> {
        match self.jazz.info.encryption {
            Participation::Device => None,
            Participation::Keyless => Some(Error::Encryption {
                code: "keyless".to_owned(),
                message: "a backend holds no encryption keys; encrypted columns are written by \
                          clients"
                    .to_owned(),
            }),
            Participation::No => Some(Error::Encryption {
                code: "not-declared".to_owned(),
                message: "this app declares no end-to-end encryption".to_owned(),
            }),
        }
    }

    /// One operation of the runtime by name, with its arguments as JSON.
    /// Its failure is [`Error::Encryption`] with the runtime's code
    /// (`approval-answered`, `no-challenge`, `approval-code-mismatch`,
    /// `device-no-longer-active`, `unsupported`, `store`, `closed`, ...).
    ///
    /// `recoveryCreate` answers `{ "phrase": ... }`, the recovery secret
    /// itself: take the string out of the answer (`Value::take`) into a
    /// zeroizing buffer, and keep no other copy.
    pub async fn call(&self, op: &str, args: Value) -> Result<Value> {
        with_runtime!(self, (op, args), {
            let op = op.to_owned();
            move |_| plan::call(op, args)
        })
    }

    /// Verifies the account, enrols this device or founds the account's
    /// first device, and, for a device account, registers recovery or
    /// recovers with the account secret. Waits up to `timeout` for the
    /// runtime's own loop (which retries while the server is out of reach).
    /// `active`, `awaiting-approval` or `reenrolled`. Closing the app (or
    /// this handle) ends the wait: [`Error::Stopped`].
    pub async fn ready(&self, timeout: Duration) -> Result<String> {
        with_runtime!(self, timeout, {
            let started = std::time::Instant::now();
            move |client: jazz::tools::JazzClient| async move {
                // The runtime's loop goes on verifying; only this wait ends.
                tokio::select! {
                    biased;
                    ready = plan::ready(timeout.saturating_sub(started.elapsed())) => ready,
                    () = client.waits_ended() => Err(Error::Stopped),
                }
            }
        })
    }

    /// What the runtime's loop is doing: `{ status, error, stage }`.
    pub async fn state(&self) -> Result<Value> {
        with_runtime!(self, (), |_| async { plan::state() })
    }

    /// The reports waiting, as JSON texts (`type`: `unreadable`,
    /// `missing-key`, `challenged`, `ready-failed`, ...).
    pub async fn take_reports(&self) -> Result<Vec<String>> {
        with_runtime!(self, (), |_| async { plan::take_reports() })
    }
}

impl Jazz {
    /// `stored`, the schema as the database holds it, as the app sees it when
    /// this client is a device: an encrypted column has the plaintext type
    /// its declaration names (a `title` the app writes as a string, though it
    /// is stored as sealed bytes), and the equality token columns are not
    /// there. For converting the values an app writes and reads and typing
    /// its queries; the schema sent to the database, and its hash, stay as
    /// stored. Any other client gets `stored` unchanged.
    pub fn plain_schema(&self, mut stored: jazz::tools::Schema) -> jazz::tools::Schema {
        let Some(declarations) = &self.info.declarations else {
            return stored;
        };
        if self.info.encryption != Participation::Device {
            return stored;
        }
        for (name, entry) in &declarations.tables {
            let Some(table) = stored.get_mut(&jazz::tools::TableName::new(name)) else {
                continue;
            };
            table
                .columns
                .columns
                .retain(|column| !is_equality_token_column(Some(entry), column.name_str()));
            for column in &mut table.columns.columns {
                if let Some(declared) = entry.columns.get(column.name.as_str()) {
                    column.column_type = declared.plain.column_type.clone();
                    column.nullable = declared.plain.nullable;
                }
            }
        }
        stored
    }

    /// The handle to this app's end-to-end encryption.
    pub fn encryption(&self) -> Encryption {
        Encryption { jazz: self.clone() }
    }
}

/// Starts the verification at open, for a device.
#[cfg(feature = "e2ee-client")]
pub(crate) fn spawn_start() {
    tokio::task::spawn_local(plan::start());
}

/// Retries a write that failed for want of a key when a device runtime is
/// running. The wait for the key ends with `client`'s waits (the app or the
/// handle closing): [`Error::Stopped`].
#[doc(hidden)]
pub async fn with_key_wait<T>(
    client: &jazz::tools::JazzClient,
    mut operation: impl FnMut() -> std::result::Result<T, jazz::tools::JazzError>,
) -> Result<T> {
    #[cfg(feature = "e2ee-client")]
    {
        plan::retry_after_key(
            async || operation(),
            std::convert::identity,
            client.waits_ended(),
            || Error::Stopped,
        )
        .await
    }
    #[cfg(not(feature = "e2ee-client"))]
    {
        let _ = client;
        operation().map_err(Error::from)
    }
}
