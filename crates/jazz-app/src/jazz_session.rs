//! A user-following session, as TypeScript's `JazzSession`
//! (`session/jazz-session.ts`, `createJazzClient` for a host without
//! cookies): who is signed in on this device, and the [`Jazz`] that reads
//! and writes as them. The app's backend decides who is signed in (its
//! `authenticate`, from the app's bearer); the session follows it, and
//! device accounts sign in without it.
//!
//! A [`Jazz`] is one user's: the session replaces it when someone signs in
//! or out, or the user changes. One kept from before fails with
//! [`Error::Replaced`] ([`Error::Stopped`] once the session closes), and
//! its subscriptions end.
//!
//! Every operation runs on the session's own runtime, whatever the
//! caller's executor (a foreign thread through jazz-ffi, `block_on`).
//! TypeScript's browser-only parts (focus and visibility checks, cross-tab
//! sign-out, cookie renewal) and the thin-client upgrade are not here.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use futures_core::Stream;
use futures_util::StreamExt as _;
use jazz::account_registry::AccountId;
use jazz::tools::AppId;
use serde::Serialize;
use tokio::sync::watch;

use crate::app::AppFiles;
use crate::auth::{Auth, Bearer, DeviceKey, TokenProvider, TokenReason};
use crate::config::{Config, Storage};
use crate::error::{Error, Result};
use crate::handle::Jazz;
use crate::session::{SessionState, User};

/// The file a device account's key is kept in, in the session's directory.
const DEVICE_KEY_FILE: &str = "device.key";

/// What a session is doing, as TypeScript's `JazzSessionSnapshot.status`
/// (and spelled as it: `signed-out`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SessionStatus {
    /// Opening; nothing to show yet.
    Starting,
    /// [`JazzSession::jazz`] is usable. `error` may still be set: an
    /// operation failed and left the client as it was.
    Ready,
    /// No one is signed in. `error` may say why.
    SignedOut,
    /// Signing in, out, or switching users.
    Transitioning,
    /// No usable client: opening, a sign-in or a reopen failed.
    /// [`JazzSession::retry`] tries again.
    Error,
    /// [`JazzSession::close`] was called.
    Closed,
}

impl std::fmt::Display for SessionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::SignedOut => "signed-out",
            Self::Transitioning => "transitioning",
            Self::Error => "error",
            Self::Closed => "closed",
        })
    }
}

/// What [`JazzSession::retry`] does about the snapshot's `error`, as
/// TypeScript's `recovery`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Recovery {
    /// Repeat the operation that failed; the client is as it was.
    Action,
    /// Reopen the signed-in account (or follow the backend again).
    Session,
}

/// A failed operation, as the snapshot reports it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionError {
    /// The [`Error`] variant, camelCase (`signedOut`, `server`, `refused`…).
    pub kind: String,
    pub message: String,
}

impl From<&Error> for SessionError {
    fn from(error: &Error) -> Self {
        let kind = match error {
            Error::App(_) => "app",
            Error::Config(_) => "config",
            Error::TextOffset(_) => "textOffset",
            Error::Server(_) => "server",
            Error::NotFound { .. } => "notFound",
            Error::Decode { .. } => "decode",
            Error::Rejected(_) => "rejected",
            Error::Refused(_) => "refused",
            Error::SignedOut => "signedOut",
            Error::NoAccount(_) => "noAccount",
            Error::NoUser { .. } => "noUser",
            Error::Replaced => "replaced",
            Error::Unsynced { .. } => "unsynced",
            Error::Unsupported(_) => "unsupported",
            Error::Upload(_) => "upload",
            Error::Panicked(_) => "panicked",
            Error::StoreHeld(_) => "storeHeld",
            Error::Stopped => "stopped",
            Error::Interrupted { .. } => "interrupted",
            Error::StillQueued { .. } => "stillQueued",
            Error::BundleMismatch { .. } => "bundleMismatch",
            Error::WriteRejected { .. } => "writeRejected",
            Error::Jazz(_) => "jazz",
        };
        Self {
            kind: kind.to_owned(),
            message: error.to_string(),
        }
    }
}

/// Who is signed in and how the session stands, as TypeScript's
/// `JazzSessionSnapshot`. Serializes camelCase. Not here: TS's `client`
/// (use [`JazzSession::jazz`], fetched again when `client_epoch` changes),
/// `localFirst` (a session's client is always local-first: there is no
/// thin client to upgrade) and `pending` (the operation under way).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionSnapshot {
    pub status: SessionStatus,
    /// Who the session is signed in as; `None` when no one is.
    pub user: Option<User>,
    /// Increases each time [`JazzSession::jazz`] would return another
    /// [`Jazz`]: fetch it again when it changes.
    pub client_epoch: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<SessionError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery: Option<Recovery>,
}

/// How a session opens.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Initial {
    /// Follow the app's backend: whoever the bearer names is signed in, and
    /// no one without a bearer. The default with a bearer.
    #[default]
    Backend,
    /// As the device account this device keeps: a guest who needs no
    /// sign-in. A new one only when none is kept (no key file): a key that
    /// can't be read or signed in with stays as it is, and the snapshot's
    /// `error` says why.
    Device,
    /// Signed out.
    SignedOut,
}

/// How to open a [`JazzSession`].
#[derive(Clone, Debug)]
pub struct SessionConfig {
    pub app_id: AppId,
    /// The app's Jazz endpoint (its backend's `createJazz` handler, e.g.
    /// `https://app.example/jazz`).
    pub server_url: Option<String>,
    /// [`Storage::Dir`] keeps each account's store in a directory of its
    /// own under it (`<dir>/<account id>/`), and the device account's key
    /// in `<dir>/device.key`. [`Storage::Memory`] keeps both for the life
    /// of the process. One client at a time has an account's store open: a
    /// second session, or client, of the same account and directory fails
    /// with [`Error::StoreHeld`]. [`Jazz::delete_client_storage`] on a
    /// session's client deletes that account's store only; the device key
    /// survives signing out.
    pub storage: Storage,
    /// The app's own auth: who its backend should sign in.
    pub bearer: Option<Bearer>,
    pub initial: Initial,
    /// See [`Config::allow_insecure_http`].
    pub allow_insecure_http: bool,
    /// When the backend names a user who has no account yet while a device
    /// guest is signed in, link the guest to them, so it keeps its data (TS's
    /// `linkGuest`, on by default). A link the backend refuses for good signs
    /// them in on an account of their own instead, once. Off: the guest
    /// stays, for the app to link ([`JazzSession::link_device_account`]).
    pub link_guest: bool,
}

impl SessionConfig {
    pub fn new(app_id: AppId) -> Self {
        Self {
            app_id,
            server_url: None,
            storage: Storage::Memory,
            bearer: None,
            initial: Initial::Backend,
            allow_insecure_http: false,
            link_guest: true,
        }
    }

    pub fn server(mut self, url: impl Into<String>) -> Self {
        self.server_url = Some(url.into());
        self
    }

    pub fn storage(mut self, storage: Storage) -> Self {
        self.storage = storage;
        self
    }

    /// The app's bearer, as [`Auth::bearer`] takes it. The session asks it
    /// while it runs an operation, one at a time: a provider that calls back
    /// into the same session (and awaits it) deadlocks.
    pub fn bearer(mut self, provider: impl TokenProvider + 'static) -> Self {
        let Auth::Bearer(bearer) = Auth::bearer(provider) else {
            unreachable!("Auth::bearer is a bearer")
        };
        self.bearer = Some(bearer);
        self
    }

    pub fn initial(mut self, initial: Initial) -> Self {
        self.initial = initial;
        self
    }

    pub fn allow_insecure_http(mut self) -> Self {
        self.allow_insecure_http = true;
        self
    }

    /// See [`SessionConfig::link_guest`].
    pub fn link_guest(mut self, link: bool) -> Self {
        self.link_guest = link;
        self
    }
}

/// The operation [`JazzSession::retry`] repeats. Not `Debug`: a restore
/// holds the raw key.
#[derive(Clone)]
enum Op {
    /// [`Initial::Device`]: the kept device account, or a new one when none
    /// is kept.
    StartDevice,
    Follow,
    /// Holds the new key, so a retry is for the same account.
    CreateDevice(DeviceKey),
    RestoreDevice(String),
    ReopenDevice,
    LinkDevice,
}

/// The signed-in user's client.
#[derive(Default)]
struct Current {
    /// Ready once the client being opened has let go of its store, for
    /// the close that interrupts it.
    opening: Option<tokio::sync::oneshot::Receiver<()>>,
    jazz: Option<Jazz>,
    /// The device account's key, while signed in as it.
    device: Option<DeviceKey>,
    /// The device key kept with [`Storage::Memory`].
    kept: Option<DeviceKey>,
    /// A guest's link the backend refused for good: that guest account and
    /// that user (issuer, subject), never linked again.
    refused_link: Option<(Option<AccountId>, String, String)>,
    last_failed: Option<Op>,
}

struct Inner {
    files: AppFiles,
    config: SessionConfig,
    /// Every operation runs here, one at a time.
    runtime: Option<tokio::runtime::Runtime>,
    handle: tokio::runtime::Handle,
    one_at_a_time: tokio::sync::Mutex<()>,
    /// Set as [`JazzSession::close`] begins, before it takes its turn: the
    /// operation under way stops waiting on the app and the network, and
    /// queued ones don't start.
    closing: watch::Sender<bool>,
    state: watch::Sender<SessionSnapshot>,
    current: Mutex<Current>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

/// A user-following session: `Send + Sync + Clone`. See the module docs.
///
/// ```no_run
/// # async fn open() -> jazz_app_support::Result<()> {
/// use jazz_app_support::{AppFiles, Initial, JazzSession, SessionConfig, Storage, TokenReason};
/// let session = JazzSession::open(
///     AppFiles::read(".")?,
///     SessionConfig::new(jazz_app_support::AppId::from_name("todos"))
///         .server("https://todos.example/jazz")
///         .storage(Storage::Dir("./jazz".into()))
///         .bearer(|_: TokenReason| async { Ok(Some("…".to_owned())) }),
/// )
/// .await?;
/// let jazz = session.jazz()?; // the signed-in user's
/// // … the app's own auth changes:
/// session.refresh().await?;
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct JazzSession {
    inner: Arc<Inner>,
    /// What else ends this view's operations' waits: its control
    /// ([`JazzSession::controlled`]) and the dropped call it runs for.
    call: CallEnds,
}

/// The per-call signals an operation stops waiting at, beside `closing`.
#[derive(Clone, Default)]
struct CallEnds {
    control: Option<crate::CallControl>,
    ends: Vec<watch::Receiver<bool>>,
}

impl std::fmt::Debug for JazzSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("JazzSession")
            .field(&*self.inner.state.borrow())
            .finish()
    }
}

impl JazzSession {
    /// Opens the session as `config.initial` says. A failure to sign in is
    /// the snapshot's `error`, not this call's: the session is still there
    /// to [`JazzSession::retry`].
    pub async fn open(files: AppFiles, config: SessionConfig) -> Result<Self> {
        if config.server_url.is_none() {
            return Err(Error::Config(
                "a session needs the app's Jazz endpoint (SessionConfig::server)".into(),
            ));
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("jazz-session")
            .enable_all()
            .build()
            .map_err(|error| Error::Config(format!("cannot start the session runtime: {error}")))?;
        let handle = runtime.handle().clone();
        let initial = config.initial;
        let has_bearer = config.bearer.is_some();
        let (state, _) = watch::channel(SessionSnapshot {
            status: SessionStatus::Starting,
            user: None,
            client_epoch: 0,
            error: None,
            recovery: None,
        });
        let session = Self {
            inner: Arc::new(Inner {
                files,
                config,
                runtime: Some(runtime),
                handle,
                one_at_a_time: tokio::sync::Mutex::new(()),
                closing: watch::Sender::new(false),
                state,
                current: Mutex::new(Current::default()),
            }),
            call: CallEnds::default(),
        };
        let start = match initial {
            Initial::Backend if has_bearer => Some(Op::Follow),
            Initial::Device => Some(Op::StartDevice),
            Initial::Backend | Initial::SignedOut => None,
        };
        match start {
            None => session.set(|snapshot| snapshot.status = SessionStatus::SignedOut),
            Some(op) => {
                // Opening never fails on a sign-in: the snapshot says why.
                let _ = session.run(op).await;
            }
        }
        Ok(session)
    }

    /// Who is signed in and how the session stands.
    pub fn snapshot(&self) -> SessionSnapshot {
        self.inner.state.borrow().clone()
    }

    /// The snapshot now, then each change. Ends after the `closed` one.
    pub fn changes(&self) -> impl Stream<Item = SessionSnapshot> + Send + Unpin + 'static {
        Box::pin(futures_util::stream::unfold(
            (self.inner.state.subscribe(), true, false),
            |(mut receiver, first, ended)| async move {
                if ended {
                    return None;
                }
                if !first && receiver.changed().await.is_err() {
                    return None;
                }
                let snapshot = receiver.borrow_and_update().clone();
                let closed = snapshot.status == SessionStatus::Closed;
                Some((snapshot, (receiver, false, closed)))
            },
        ))
    }

    /// The signed-in user's [`Jazz`]. [`Error::NoUser`] naming the status
    /// while there is no usable client (starting, signed out, switching
    /// users, failed or closed). Fetch it again when the snapshot's
    /// `client_epoch` changes: one kept from before fails with
    /// [`Error::Replaced`].
    pub fn jazz(&self) -> Result<Jazz> {
        let status = self.inner.state.borrow().status;
        let current = self.lock();
        match (&current.jazz, status) {
            (Some(jazz), SessionStatus::Ready) => Ok(jazz.clone()),
            _ => Err(Error::NoUser { status }),
        }
    }

    /// The app's auth changed (its own sign-in, sign-out or account
    /// switch): asks the bearer and the backend who is signed in now, and
    /// follows. The same user keeps the same [`Jazz`] while its client
    /// lives; another one gets theirs; no one signs out.
    pub async fn refresh(&self) -> Result<()> {
        self.run(Op::Follow).await
    }

    /// Signs in as a new device account: an account whose key lives on
    /// this device. It needs no sign-in and works offline. The key is kept
    /// (see [`SessionConfig::storage`]) for [`JazzSession::reopen_device_account`].
    ///
    /// It replaces a key kept before, readable or not, once the new account
    /// signed in: export that one first to keep its account. A failure
    /// leaves it, and [`JazzSession::retry`] is for the same new account.
    pub async fn create_device_account(&self) -> Result<()> {
        self.run(Op::CreateDevice(DeviceKey::generate()?)).await
    }

    /// Signs in as the device account a key from
    /// [`JazzSession::export_device_key`] names.
    pub async fn restore_device_account(&self, key: &str) -> Result<()> {
        self.run(Op::RestoreDevice(key.to_owned())).await
    }

    /// Signs in again as the device account this device keeps, with its
    /// data. [`Error::Config`] when it keeps none.
    pub async fn reopen_device_account(&self) -> Result<()> {
        self.run(Op::ReopenDevice).await
    }

    /// Keeps the signed-in device account and its data when its user signs
    /// up: the backend links it to the user the bearer names now, and the
    /// session continues as that user, with the same [`Jazz`].
    pub async fn link_device_account(&self) -> Result<()> {
        self.run(Op::LinkDevice).await
    }

    /// The signed-in device account's key (signed out: the one
    /// [`JazzSession::reopen_device_account`] would reopen), as
    /// `jazz-auth-v1:…`. Anyone holding it is that account.
    pub fn export_device_key(&self) -> Result<String> {
        let status = self.inner.state.borrow().status;
        let current = self.lock();
        if let Some(key) = &current.device {
            return Ok(key.to_string());
        }
        if status == SessionStatus::SignedOut
            && let Some(key) = self.kept_key(&current)?
        {
            return Ok(key.to_string());
        }
        Err(Error::Config(
            "no device account to export: signed in as someone else, or none kept".into(),
        ))
    }

    /// Signs out: the signed-in user's [`Jazz`] closes (calls on it fail
    /// with [`Error::Replaced`]) and the session reads `signedOut`. It stays
    /// usable: sign in again, or [`JazzSession::refresh`]. Each account's
    /// local store stays, and so does a device account's key.
    pub async fn sign_out(&self) -> Result<()> {
        self.operation(|this| async move {
            let _one = this.take_turn().await?;
            this.replace(None, None, SessionStatus::SignedOut).await;
            Ok(())
        })
        .await
    }

    /// Repeats what failed (the snapshot's `recovery`): the last failed
    /// operation, or following the backend.
    pub async fn retry(&self) -> Result<()> {
        let op = self.lock().last_failed.clone();
        match op {
            Some(op) => self.run(op).await,
            None if self.inner.config.bearer.is_some() => self.run(Op::Follow).await,
            None => Ok(()),
        }
    }

    /// Closes the session for good: the signed-in user's [`Jazz`] closes,
    /// the snapshot reads `closed` (the last [`JazzSession::changes`]
    /// yields), and every operation fails with [`Error::Stopped`].
    ///
    /// It doesn't wait for an operation under way to hear from the app's
    /// bearer or the backend: that operation fails with [`Error::Stopped`]
    /// too (what it sent may still apply there), and installs no client.
    /// A client being stopped is waited for, as [`Jazz::shutdown`] bounds it.
    /// So is one being opened, for up to 2 s, to let go of its store: past
    /// that (a token provider that doesn't return) deleting the store may
    /// fail with [`Error::StoreHeld`] until it has.
    pub async fn close(&self) -> Result<()> {
        // Before waiting for a turn, so whoever has it gives it up.
        self.inner.closing.send_replace(true);
        let this = self.clone();
        self.on_runtime(async move {
            let _one = this.inner.one_at_a_time.lock().await;
            if this.inner.state.borrow().status == SessionStatus::Closed {
                return Ok(());
            }
            // An opening it interrupted lets go of the store before close
            // returns, so the store can be reopened or deleted at once. Not
            // waited for longer: the app's token provider may be what holds
            // it up, and may be what called close.
            let opening = this.lock().opening.take();
            if let Some(opening) = opening {
                let _ = tokio::time::timeout(crate::handle::STREAM_SESSION_END, opening).await;
            }
            this.replace(None, None, SessionStatus::Closed).await;
            Ok(())
        })
        .await
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Current> {
        self.inner
            .current
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn set(&self, change: impl FnOnce(&mut SessionSnapshot)) {
        self.inner.state.send_modify(change);
    }

    fn ensure_open(&self) -> Result<()> {
        if *self.inner.closing.borrow() {
            Err(Error::Stopped)
        } else {
            Ok(())
        }
    }

    /// `wait`'s output, or [`Error::Stopped`] once the session closes;
    /// on a controlled view, [`Error::Interrupted`] once its control ends
    /// (what the operation sent may still apply). An operation whose caller
    /// dropped it stops waiting too.
    async fn unless_closing<T>(&self, wait: impl std::future::Future<Output = T>) -> Result<T> {
        let mut closing = self.inner.closing.subscribe();
        let mut ends: Vec<_> = self.call.ends.clone();
        ends.extend(self.call.control.as_ref().map(crate::CallControl::signal));
        let call_ended = async {
            let ends = ends.into_iter().map(|mut ends| {
                Box::pin(async move {
                    if ends.wait_for(|ended| *ended).await.is_err() {
                        std::future::pending::<()>().await;
                    }
                })
            });
            let ends: Vec<_> = ends.collect();
            if ends.is_empty() {
                std::future::pending::<()>().await;
            }
            futures_util::future::select_all(ends).await;
        };
        let timeout = self.call.control.clone().map(crate::CallControl::time_out);
        let timed_out = async {
            match timeout {
                Some(timeout) => timeout.await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            () = async { let _ = closing.wait_for(|closing| *closing).await; } => Err(Error::Stopped),
            output = wait => Ok(output),
            () = call_ended => Err(self.interrupted()),
            () = timed_out => Err(self.interrupted()),
        }
    }

    /// Why this view's operation was cut short by its call.
    fn interrupted(&self) -> Error {
        match self
            .call
            .control
            .as_ref()
            .and_then(crate::CallControl::interruption)
        {
            Some(reason) => Error::Interrupted { reason },
            // Dropped: nobody hears it.
            None => Error::Stopped,
        }
    }

    /// This session, with its operations (refresh, sign-in, link, sign-out,
    /// retry) cut short by `control`: they stop waiting on the app and the
    /// network once it is cancelled or timed out, and fail with
    /// [`Error::Interrupted`]; what they sent may still apply, and the
    /// snapshot reports the failure (retry repeats it). One made once the
    /// control has ended does nothing. See [`crate::CallControl`].
    pub fn controlled(&self, control: crate::CallControl) -> JazzSession {
        JazzSession {
            inner: self.inner.clone(),
            call: CallEnds {
                control: Some(control),
                ends: self.call.ends.clone(),
            },
        }
    }

    /// Runs `operation` (on a clone of this view that also stops waiting
    /// once this call is dropped) on the session's runtime: refused before
    /// it starts once the view's control has ended.
    async fn operation<T: Send + 'static, F>(
        &self,
        operation: impl FnOnce(JazzSession) -> F,
    ) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>> + Send + 'static,
    {
        if let Some(reason) = self
            .call
            .control
            .as_ref()
            .and_then(crate::CallControl::interruption)
        {
            return Err(Error::Interrupted { reason });
        }
        let (mut dropped, call_ends) = crate::control::EndOnDrop::new();
        let mut this = self.clone();
        this.call.ends.push(call_ends);
        let answer = self.on_runtime(operation(this)).await;
        dropped.disarm();
        answer
    }

    /// This operation's turn, unless the session closes first.
    async fn take_turn(&self) -> Result<tokio::sync::MutexGuard<'_, ()>> {
        let turn = self.unless_closing(self.inner.one_at_a_time.lock()).await?;
        self.ensure_open()?;
        Ok(turn)
    }

    /// Runs `future` on the session's runtime, whatever the caller's.
    async fn on_runtime<T: Send + 'static>(
        &self,
        future: impl std::future::Future<Output = Result<T>> + Send + 'static,
    ) -> Result<T> {
        self.inner
            .handle
            .spawn(future)
            .await
            .map_err(|_| Error::Stopped)?
    }

    /// Runs `op` on the session's runtime, one operation at a time,
    /// recording its failure in the snapshot.
    async fn run(&self, op: Op) -> Result<()> {
        self.operation(|this| async move {
            let _one = this.take_turn().await?;
            let result = this.perform(op.clone()).await;
            if result.is_err() && *this.inner.closing.borrow() {
                // Cut short by close, which has the snapshot's last word.
                return Err(Error::Stopped);
            }
            match &result {
                Ok(()) => {
                    this.lock().last_failed = None;
                    this.set(|snapshot| {
                        snapshot.error = None;
                        snapshot.recovery = None;
                    });
                }
                Err(error) => {
                    // A link that went through but whose reopen failed is
                    // retried by following the backend (the device account
                    // is the user's now), not by linking again.
                    let op = match op {
                        Op::LinkDevice if this.lock().device.is_none() => Op::Follow,
                        op => op,
                    };
                    this.lock().last_failed = Some(op);
                    let usable = this.lock().jazz.is_some();
                    let error = SessionError::from(error);
                    this.set(|snapshot| {
                        snapshot.error = Some(error);
                        if usable {
                            // The client is as it was.
                            snapshot.status = SessionStatus::Ready;
                            snapshot.recovery = Some(Recovery::Action);
                        } else if snapshot.status != SessionStatus::SignedOut {
                            snapshot.status = SessionStatus::Error;
                            snapshot.recovery = Some(Recovery::Session);
                        } else {
                            snapshot.recovery = Some(Recovery::Action);
                        }
                    });
                }
            }
            result
        })
        .await
    }

    async fn perform(&self, op: Op) -> Result<()> {
        match op {
            Op::Follow => self.follow().await,
            Op::StartDevice => {
                let kept = self.kept_key(&self.lock())?;
                let key = match kept {
                    Some(key) => key,
                    None => self.keep_first_key()?,
                };
                self.sign_in_device(key).await
            }
            Op::CreateDevice(key) => self.create_device(key).await,
            Op::RestoreDevice(text) => {
                let key: DeviceKey = text.parse()?;
                // Kept only once it signed in: a failed restore leaves the
                // device account kept before.
                self.sign_in_device(key.clone()).await?;
                self.keep_key(&key)
            }
            Op::ReopenDevice => {
                let kept = {
                    let current = self.lock();
                    self.kept_key(&current)?
                };
                let key = kept.ok_or_else(|| {
                    Error::Config("this device keeps no device account to reopen".into())
                })?;
                self.sign_in_device(key).await
            }
            Op::LinkDevice => self.link().await,
        }
    }

    /// Follows the backend: whoever the bearer names now, as TypeScript's
    /// backend watch (`session/backend-watch.ts`).
    ///
    /// - No one: a backend user signs out; a device guest is kept.
    /// - A user who has no account yet, while a device guest is signed in:
    ///   the guest is linked to them and keeps its data
    ///   ([`SessionConfig::link_guest`], on by default).
    /// - The user signed in now: the same client goes on, while it lives.
    /// - Anyone else: signed in (their account made if need be), with a
    ///   client of their own.
    async fn follow(&self) -> Result<()> {
        let Some(bearer) = self.inner.config.bearer.clone() else {
            return Err(Error::Config(
                "following the backend needs the app's bearer (SessionConfig::bearer)".into(),
            ));
        };
        let endpoint = self.endpoint();
        let app = self.inner.config.app_id;
        let guest = self.lock().device.is_some();
        let token = self
            .unless_closing(bearer.token(TokenReason::Refresh))
            .await??;
        let who = match &token {
            Some(token) => match self
                .unless_closing(crate::backend::backend_user(&endpoint, app, token))
                .await?
            {
                Ok(who) => who,
                // The backend can't be reached. A client already open goes
                // on; otherwise the bearer's user opens offline, if they
                // signed in on this device before (the open says if not).
                Err(crate::backend::Unanswered::Unreachable(error)) => {
                    if self.lock().jazz.is_some() {
                        self.set(|snapshot| snapshot.status = SessionStatus::Ready);
                        return Ok(());
                    }
                    if guest {
                        return Err(Error::Server(error));
                    }
                    self.transition();
                    let config = self.user_config().user(Auth::Bearer(bearer));
                    return self.switch_to(config, None).await;
                }
                Err(crate::backend::Unanswered::Failed(error)) => return Err(error),
            },
            None => None,
        };
        let (Some(token), Some(who)) = (token, who) else {
            if guest {
                // A guest needs no backend user.
                self.set(|snapshot| snapshot.status = SessionStatus::Ready);
            } else {
                self.replace(None, None, SessionStatus::SignedOut).await;
            }
            return Ok(());
        };
        if guest && who.account.is_none() {
            if !self.inner.config.link_guest {
                // The app decides what becomes of this guest
                // (`link_device_account`), as TS's `linkGuest: false`.
                self.set(|snapshot| snapshot.status = SessionStatus::Ready);
                return Ok(());
            }
            let guest_account = self
                .inner
                .state
                .borrow()
                .user
                .as_ref()
                .map(|user| user.account);
            let refusal = (
                guest_account,
                who.identity.issuer.clone(),
                who.identity.subject.clone(),
            );
            let refused_before = self.lock().refused_link.as_ref() == Some(&refusal);
            if !refused_before {
                match self.link_with(&bearer, &token).await {
                    Ok(()) => return Ok(()),
                    // Refused for good (a retired key, another app's guest,
                    // an account that appeared meanwhile): sign the user in
                    // on their own account instead, and never try this link
                    // again, as TS's `refusedLink`.
                    Err(Error::Refused(_)) => self.lock().refused_link = Some(refusal),
                    // Signed out while linking: no one to link to; the guest
                    // stays.
                    Err(Error::SignedOut) => {
                        self.set(|snapshot| snapshot.status = SessionStatus::Ready);
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        let same = self.inner.state.borrow().user.as_ref().is_some_and(|user| {
            user.issuer == who.identity.issuer
                && user.subject == who.identity.subject
                && who.account.is_none_or(|account| account == user.account)
        }) && self.lock().jazz.as_ref().is_some_and(Jazz::is_live);
        if same {
            // The same user keeps the same client.
            self.set(|snapshot| snapshot.status = SessionStatus::Ready);
            return Ok(());
        }
        let Some(_signed_in) = self
            .unless_closing(crate::backend::sign_in(&endpoint, app, &token))
            .await??
        else {
            self.replace(None, None, SessionStatus::SignedOut).await;
            return Ok(());
        };
        self.transition();
        let config = self.user_config().user(Auth::Bearer(bearer));
        self.switch_to(config, None).await
    }

    async fn sign_in_device(&self, key: DeviceKey) -> Result<()> {
        self.transition();
        let config = self.user_config().user(Auth::Device(key.clone()));
        self.switch_to(config, Some(key)).await
    }

    /// Signs in as `key`'s new account and keeps it. A key kept before is
    /// replaced only once that worked.
    async fn create_device(&self, key: DeviceKey) -> Result<()> {
        let Some(path) = self.key_path() else {
            if self.lock().kept.is_none() {
                self.keep_key(&key)?;
                return self.sign_in_device(key).await;
            }
            self.sign_in_device(key.clone()).await?;
            return self.keep_key(&key);
        };
        if is_absent(&path) {
            self.keep_key(&key)?;
            return self.sign_in_device(key).await;
        }
        // Written beside the kept one first: a key that can't be kept fails
        // before the session changes.
        let next = path.with_file_name(format!(".{DEVICE_KEY_FILE}.{}.next", key.user_id()));
        key.write_aside(&next)?;
        let created = self.sign_in_device(key).await.and_then(|()| {
            crate::auth::lock_to_replace(&path)
                .and_then(|_lock| std::fs::rename(&next, &path))
                .map_err(|error| key_file_error(&path, error))
        });
        if created.is_err() {
            let _ = std::fs::remove_file(&next);
        }
        created
    }

    /// The device account's backend link, as TypeScript's
    /// `linkDeviceAccount`: the session goes on as the linked user.
    async fn link(&self) -> Result<()> {
        let (jazz, device) = {
            let current = self.lock();
            (current.jazz.clone(), current.device.is_some())
        };
        let (Some(jazz), true) = (jazz, device) else {
            return Err(Error::Config(
                "link_device_account needs a signed-in device account".into(),
            ));
        };
        let Some(bearer) = self.inner.config.bearer.clone() else {
            return Err(Error::Config(
                "linking needs the app's bearer (SessionConfig::bearer)".into(),
            ));
        };
        let token = self
            .unless_closing(bearer.token(TokenReason::Refresh))
            .await??
            .ok_or(Error::SignedOut)?;
        drop(jazz);
        self.link_with(&bearer, &token).await
    }

    /// Links the signed-in device account to the user `token` names, then
    /// goes on as them: their account is the device's, and so is its store,
    /// reopened on the app's bearer (a backend may retire the device key on
    /// linking, `guestKeyAfterLink: "revoke"`).
    async fn link_with(&self, bearer: &Bearer, token: &str) -> Result<()> {
        let Some(jazz) = self.lock().jazz.clone() else {
            return Err(Error::Config(
                "link_device_account needs a signed-in device account".into(),
            ));
        };
        let session = jazz
            .session()
            .ok_or_else(|| Error::Config("the device account's client has no session".into()))?;
        self.unless_closing(session.link_account(token)).await??;
        drop(jazz);
        self.transition();
        let config = self.user_config().user(Auth::Bearer(bearer.clone()));
        self.switch_to(config, None).await
    }

    fn transition(&self) {
        self.set(|snapshot| snapshot.status = SessionStatus::Transitioning);
    }

    /// Closes the signed-in user's client and opens `config`'s, in one
    /// transition.
    async fn switch_to(&self, config: Config, device: Option<DeviceKey>) -> Result<()> {
        self.close_current().await;
        // Not finished when the session closes, the opening stops itself:
        // its thread ends once it has nobody to answer.
        let (ended, opening) = tokio::sync::oneshot::channel();
        self.lock().opening = Some(opening);
        let jazz = self
            .unless_closing(Jazz::open_tracked(self.inner.files.clone(), config, ended))
            .await?;
        self.lock().opening = None;
        match jazz {
            Ok(jazz) => {
                // Before any handle of it is given out.
                if let Some(session) = jazz.session() {
                    session.mark_owned();
                }
                let user = jazz.session().map(|session| match session.state() {
                    SessionState::SignedIn { user } | SessionState::Expired { user, .. } => {
                        Some(user)
                    }
                    SessionState::SignedOut => None,
                });
                let states = jazz.session().map(|session| session.changes());
                {
                    let mut current = self.lock();
                    current.jazz = Some(jazz);
                    current.device = device;
                }
                self.set(|snapshot| {
                    snapshot.user = user.flatten();
                    snapshot.status = SessionStatus::Ready;
                    snapshot.client_epoch += 1;
                });
                if let Some(states) = states {
                    self.follow_end(states);
                }
                Ok(())
            }
            Err(Error::SignedOut) => {
                self.set(|snapshot| {
                    snapshot.user = None;
                    snapshot.status = SessionStatus::SignedOut;
                });
                Ok(())
            }
            Err(error) => {
                self.set(|snapshot| {
                    snapshot.user = None;
                    snapshot.status = SessionStatus::Error;
                });
                Err(error)
            }
        }
    }

    /// Follows the current client ending by itself (its credential names no
    /// one any more): the session signs out with it, keeping a device key
    /// to reopen. A client the session replaced or closed meanwhile is not
    /// its current one, and changes nothing.
    fn follow_end(&self, mut states: impl Stream<Item = SessionState> + Send + Unpin + 'static) {
        let epoch = self.inner.state.borrow().client_epoch;
        // Holds neither the session nor the client open.
        let inner = Arc::downgrade(&self.inner);
        self.inner.handle.spawn(async move {
            // The stream ends when the client stops.
            while let Some(state) = states.next().await {
                if state == SessionState::SignedOut {
                    break;
                }
            }
            let Some(inner) = inner.upgrade() else {
                return;
            };
            let this = Self {
                inner,
                call: CallEnds::default(),
            };
            let _one = this.inner.one_at_a_time.lock().await;
            if this.ensure_open().is_err() || this.inner.state.borrow().client_epoch != epoch {
                return;
            }
            this.replace(None, None, SessionStatus::SignedOut).await;
            this.set(|snapshot| {
                snapshot.error = None;
                snapshot.recovery = None;
            });
        });
    }

    /// Ends the session's user and client, reading `status` after.
    async fn replace(&self, user: Option<User>, device: Option<DeviceKey>, status: SessionStatus) {
        self.close_current().await;
        self.lock().device = device;
        self.set(|snapshot| {
            snapshot.user = user;
            snapshot.status = status;
        });
    }

    /// Closes the signed-in user's client: calls on it fail from now on,
    /// with [`Error::Replaced`] while the session goes on, and with
    /// [`Error::Stopped`] once it closes.
    async fn close_current(&self) {
        let old = {
            let mut current = self.lock();
            current.device = None;
            current.jazz.take()
        };
        if let Some(old) = old {
            old.mark_replaced(self.inner.closing.subscribe());
            let _ = old.shutdown().await;
            self.set(|snapshot| snapshot.client_epoch += 1);
        }
    }

    fn endpoint(&self) -> String {
        self.inner
            .config
            .server_url
            .clone()
            .expect("checked on open")
    }

    /// A user's client config: their own store under the session's
    /// directory.
    fn user_config(&self) -> Config {
        let config = &self.inner.config;
        let storage = match &config.storage {
            Storage::Memory => Storage::Memory,
            // The client keeps the account's store in `<dir>/<account>/`.
            Storage::Dir(dir) => Storage::Dir(dir.clone()),
        };
        let mut user = Config::new(config.app_id)
            .server(self.endpoint())
            .storage(storage);
        user.allow_insecure_http = config.allow_insecure_http;
        user
    }

    fn key_path(&self) -> Option<PathBuf> {
        match &self.inner.config.storage {
            Storage::Dir(dir) => Some(dir.join(DEVICE_KEY_FILE)),
            Storage::Memory => None,
        }
    }

    /// The kept key, `None` only when none is kept: whatever else is at its
    /// path is an error, never a reason to replace it.
    fn kept_key(&self, current: &Current) -> Result<Option<DeviceKey>> {
        let Some(path) = self.key_path() else {
            return Ok(current.kept.clone());
        };
        let text = match std::fs::read_to_string(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if is_absent(&path) {
                    return Ok(None);
                }
                // There now: kept meanwhile (another first launch), or a
                // link to nowhere, which fails again.
                std::fs::read_to_string(&path)
            }
            read => read,
        };
        text.map_err(|error| key_file_error(&path, error))?
            .parse()
            .map(Some)
    }

    /// A new key, kept where none was: of first launches at once, the first
    /// to keep one wins and the others use it.
    fn keep_first_key(&self) -> Result<DeviceKey> {
        if let Some(path) = self.key_path() {
            match DeviceKey::load_or_create(&path) {
                Ok(key) => return Ok(key),
                // There now: another's, kept meanwhile, or what can't be read.
                Err(_) if !is_absent(&path) => return DeviceKey::load_or_create(&path),
                // On Android creating needs only the lock that writing it
                // as any other would wait for again, then replace blindly.
                #[cfg(any(target_os = "android", all(test, target_os = "linux")))]
                Err(error) => return Err(error),
                // Still none: where there are no hard links to create it
                // with, it is written as any other.
                #[cfg(not(any(target_os = "android", all(test, target_os = "linux"))))]
                Err(_) => {}
            }
        }
        let key = DeviceKey::generate()?;
        self.keep_key(&key)?;
        Ok(key)
    }

    /// Keeps `key` as the device account to reopen, replacing any kept
    /// before.
    fn keep_key(&self, key: &DeviceKey) -> Result<()> {
        let Some(path) = self.key_path() else {
            self.lock().kept = Some(key.clone());
            return Ok(());
        };
        key.write_to(&path)
    }
}

/// Whether nothing is at `path`. A link to a missing file is not nothing.
fn is_absent(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
}

fn key_file_error(path: &Path, error: std::io::Error) -> Error {
    Error::Config(format!("device key at {}: {error}", path.display()))
}

/// Android's device-key lock through sessions, run on Linux: the lib's own
/// tests keep keys as Android does (`auth`'s `cfg`s), as CI runs no Rust
/// tests on Android. White-box for that reason; offline, so they need no
/// server.
#[cfg(all(test, target_os = "linux"))]
mod android_key_lock_tests {
    use std::fs::File;
    use std::time::{Duration, Instant};

    use super::*;

    /// Longer than a creator's lock is waited for, shorter than twice that.
    const BOUND: Duration = Duration::from_secs(8);
    /// Set for the process that holds the lock in
    /// `a_killed_creator_lets_a_sessions_restore_through`.
    const HOLDER_DIR: &str = "JAZZ_DEVICE_KEY_HOLDER_DIR";

    #[derive(Clone, Copy, Debug)]
    enum Replace {
        Restore,
        Create,
        CreateOverKept,
    }

    async fn session(dir: &Path, initial: Initial) -> JazzSession {
        let files = include_bytes!("../tests/fixtures/todos/jazz.app.json");
        let config = SessionConfig::new(AppId::from_name("device-key-lock"))
            .server("http://127.0.0.1:9/jazz")
            .allow_insecure_http()
            .storage(Storage::Dir(dir.into()))
            .initial(initial);
        JazzSession::open(AppFiles::new(files.to_vec(), None), config)
            .await
            .unwrap()
    }

    fn creators_lock(dir: &Path) -> File {
        let file = File::create(dir.join(".device.key.lock")).unwrap();
        file.lock().unwrap();
        file
    }

    fn new_key() -> String {
        DeviceKey::generate().unwrap().to_string()
    }

    fn kept(dir: &Path) -> Option<String> {
        std::fs::read_to_string(dir.join(DEVICE_KEY_FILE)).ok()
    }

    /// alice's first launch has looked and not yet renamed its key in, as
    /// she restores or creates an account in another process:
    ///
    /// ```text
    /// first launch ──lock──look──────────────rename──unlock
    /// alice              └──restore/create──(waits)────────┴──rename──► kept
    /// ```
    ///
    /// Told it worked, she finds that account kept.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_first_launch_never_overwrites_a_restored_or_created_key() {
        for replace in [Replace::Restore, Replace::Create, Replace::CreateOverKept] {
            let dir = tempfile::tempdir().unwrap();
            if let Replace::CreateOverKept = replace {
                std::fs::write(dir.path().join(DEVICE_KEY_FILE), new_key()).unwrap();
            }
            let alice = session(dir.path(), Initial::SignedOut).await;
            let guests = dir.path().join("guest.tmp");
            std::fs::write(&guests, new_key()).unwrap();

            let first_launch = creators_lock(dir.path());
            let replacing = tokio::spawn({
                let alice = alice.clone();
                async move {
                    match replace {
                        Replace::Restore => alice.restore_device_account(&new_key()).await,
                        _ => alice.create_device_account().await,
                    }
                }
            });
            // Time to get as far as it can before the first launch is done.
            tokio::time::sleep(Duration::from_secs(1)).await;
            std::fs::rename(&guests, dir.path().join(DEVICE_KEY_FILE)).unwrap();
            drop(first_launch);

            replacing.await.unwrap().unwrap();
            let signed_in = alice.export_device_key().unwrap();
            assert_eq!(
                kept(dir.path()),
                Some(signed_in),
                "{replace:?}: the first launch's key replaced alice's"
            );
            alice.close().await.unwrap();
        }
    }

    /// A process of alice's is frozen holding the lock (Android freezes
    /// cached apps): her restore and create fail without touching the kept
    /// key, and `close` isn't stalled. Once it lets go, `retry` keeps the
    /// same account.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_frozen_creator_stalls_neither_replacing_nor_close() {
        for replace in [Replace::Restore, Replace::CreateOverKept] {
            let dir = tempfile::tempdir().unwrap();
            let before = new_key();
            std::fs::write(dir.path().join(DEVICE_KEY_FILE), &before).unwrap();
            let alice = session(dir.path(), Initial::SignedOut).await;
            let frozen = creators_lock(dir.path());

            let started = Instant::now();
            let failed = match replace {
                Replace::Restore => alice.restore_device_account(&new_key()).await,
                _ => alice.create_device_account().await,
            };
            assert!(failed.is_err(), "{replace:?} didn't wait for the lock");
            assert!(started.elapsed() < BOUND, "{replace:?}");
            assert_eq!(kept(dir.path()), Some(before), "{replace:?}");

            drop(frozen);
            alice.retry().await.unwrap();
            assert_eq!(
                kept(dir.path()),
                alice.export_device_key().ok(),
                "{replace:?}"
            );
            alice.close().await.unwrap();
        }

        let dir = tempfile::tempdir().unwrap();
        let alice = session(dir.path(), Initial::SignedOut).await;
        let _frozen = creators_lock(dir.path());
        let restoring = tokio::spawn({
            let alice = alice.clone();
            async move { alice.restore_device_account(&new_key()).await }
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        let closed = tokio::time::timeout(BOUND, alice.close()).await;
        closed.expect("close stalled").unwrap();
        assert!(
            restoring.await.unwrap().is_err(),
            "restore didn't wait for the lock"
        );
    }

    /// alice's first launch meets a frozen creator: it opens in time, as an
    /// error, and writes no key beside that creator's back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_first_launch_waits_for_a_frozen_creator_only_so_long() {
        let dir = tempfile::tempdir().unwrap();
        let _frozen = creators_lock(dir.path());
        let alice = tokio::time::timeout(BOUND, session(dir.path(), Initial::Device))
            .await
            .expect("open stalled");
        assert_eq!(alice.snapshot().status, SessionStatus::Error);
        assert_eq!(kept(dir.path()), None);
        alice.close().await.unwrap();
    }

    /// A creator of alice's is killed holding the lock: the restore waiting
    /// for it in another process goes through.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_killed_creator_lets_a_sessions_restore_through() {
        const NAME: &str = "jazz_session::android_key_lock_tests::a_killed_creator_lets_a_sessions_restore_through";
        if let Some(dir) = std::env::var_os(HOLDER_DIR) {
            let _held = creators_lock(Path::new(&dir));
            std::fs::write(Path::new(&dir).join("held"), "").unwrap();
            std::thread::sleep(Duration::from_secs(60));
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let alice = session(dir.path(), Initial::SignedOut).await;
        let mut creator = std::process::Command::new(std::env::current_exe().unwrap())
            .args([NAME, "--exact", "--test-threads=1"])
            .env(HOLDER_DIR, dir.path())
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while !dir.path().join("held").exists() {
            assert!(Instant::now() < deadline, "the creator never took the lock");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let key = new_key();
        let restoring = tokio::spawn({
            let (alice, key) = (alice.clone(), key.clone());
            async move { alice.restore_device_account(&key).await }
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        let waited = !restoring.is_finished();
        creator.kill().unwrap();
        creator.wait().unwrap();
        restoring.await.unwrap().unwrap();
        assert!(waited, "restore didn't wait for the lock");
        assert_eq!(kept(dir.path()), Some(key));
        alice.close().await.unwrap();
    }
}
