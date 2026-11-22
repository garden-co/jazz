//! A user-following session, as TypeScript's `JazzSession`
//! (`session/jazz-session.ts`, `createJazzClient` for a host without
//! cookies): who is signed in on this device, and the [`Jazz`] that reads
//! and writes as them. The app's backend decides who is signed in (its
//! `authenticate`, from the app's bearer); the session follows it, and
//! device accounts sign in without it.
//!
//! A [`Jazz`] is one user's: the session replaces it when someone signs in
//! or out, or the user changes. One kept from before fails with
//! [`Error::Replaced`], and its subscriptions end.
//!
//! Every operation runs on the session's own runtime, whatever the
//! caller's executor (a foreign thread through jazz-ffi, `block_on`).
//! TypeScript's browser-only parts (focus and visibility checks, cross-tab
//! sign-out, cookie renewal) and the thin-client upgrade are not here.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use futures_core::Stream;
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
    /// As the device account this device keeps (a new one if none): a
    /// guest who needs no sign-in.
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
    /// of the process. [`Jazz::delete_client_storage`] on a session's
    /// client deletes that account's store only (and is refused with
    /// [`Error::StoreHeld`] while another open client holds it); the device
    /// key survives signing out.
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
    Follow,
    CreateDevice,
    RestoreDevice(String),
    ReopenDevice,
    LinkDevice,
}

/// The signed-in user's client.
#[derive(Default)]
struct Current {
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
        };
        let start = match initial {
            Initial::Backend if has_bearer => Some(Op::Follow),
            Initial::Device => Some(Op::ReopenDevice),
            Initial::Backend | Initial::SignedOut => None,
        };
        match start {
            None => session.set(|snapshot| snapshot.status = SessionStatus::SignedOut),
            Some(op) => {
                // Opening never fails on a sign-in: the snapshot says why.
                let device_fallback = matches!(op, Op::ReopenDevice);
                let result = session.run(op).await;
                if device_fallback && matches!(result, Err(Error::Config(_))) {
                    // No device account kept: start as a new one.
                    let _ = session.run(Op::CreateDevice).await;
                }
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
    /// follows. The same user keeps the same [`Jazz`]; another one gets
    /// theirs; no one signs out.
    pub async fn refresh(&self) -> Result<()> {
        self.run(Op::Follow).await
    }

    /// Signs in as a new device account: an account whose key lives on
    /// this device. It needs no sign-in and works offline. The key is kept
    /// (see [`SessionConfig::storage`]) for [`JazzSession::reopen_device_account`].
    pub async fn create_device_account(&self) -> Result<()> {
        self.run(Op::CreateDevice).await
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
        let this = self.clone();
        self.on_runtime(async move {
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
    pub async fn close(&self) -> Result<()> {
        // Before waiting for a turn, so whoever has it gives it up.
        self.inner.closing.send_replace(true);
        let this = self.clone();
        self.on_runtime(async move {
            let _one = this.inner.one_at_a_time.lock().await;
            if this.inner.state.borrow().status == SessionStatus::Closed {
                return Ok(());
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

    /// `wait`'s output, or [`Error::Stopped`] once the session closes.
    async fn unless_closing<T>(&self, wait: impl std::future::Future<Output = T>) -> Result<T> {
        let mut closing = self.inner.closing.subscribe();
        tokio::select! {
            biased;
            () = async { let _ = closing.wait_for(|closing| *closing).await; } => Err(Error::Stopped),
            output = wait => Ok(output),
        }
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
        let this = self.clone();
        self.on_runtime(async move {
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
            Op::CreateDevice => {
                let key = DeviceKey::generate()?;
                self.keep_key(&key)?;
                self.sign_in_device(key).await
            }
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
    /// - The user signed in now: the same client goes on.
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
            Some(token) => {
                self.unless_closing(crate::backend::backend_user(&endpoint, app, token))
                    .await??
            }
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
        }) && self.lock().jazz.is_some();
        if same {
            // The same user keeps the same client.
            self.set(|snapshot| snapshot.status = SessionStatus::Ready);
            return Ok(());
        }
        let Some(signed_in) = self
            .unless_closing(crate::backend::sign_in(&endpoint, app, &token))
            .await??
        else {
            self.replace(None, None, SessionStatus::SignedOut).await;
            return Ok(());
        };
        self.transition();
        let config = self
            .user_config(signed_in.account)
            .user(Auth::Bearer(bearer));
        self.switch_to(config, None).await
    }

    async fn sign_in_device(&self, key: DeviceKey) -> Result<()> {
        self.transition();
        let config = self
            .user_config(key.account(self.inner.config.app_id))
            .user(Auth::Device(key.clone()));
        self.switch_to(config, Some(key)).await
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
        let user = self.unless_closing(session.link(token)).await??;
        drop(jazz);
        self.transition();
        let config = self
            .user_config(user.account)
            .user(Auth::Bearer(bearer.clone()));
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
        let jazz = self
            .unless_closing(Jazz::open(self.inner.files.clone(), config))
            .await?;
        match jazz {
            Ok(jazz) => {
                let user = jazz.session().map(|session| match session.state() {
                    SessionState::SignedIn { user } | SessionState::Expired { user, .. } => {
                        Some(user)
                    }
                    SessionState::SignedOut => None,
                });
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

    /// Ends the session's user and client, reading `status` after.
    async fn replace(&self, user: Option<User>, device: Option<DeviceKey>, status: SessionStatus) {
        self.close_current().await;
        self.lock().device = device;
        self.set(|snapshot| {
            snapshot.user = user;
            snapshot.status = status;
        });
    }

    /// Closes the signed-in user's client: calls on it fail with
    /// [`Error::Replaced`] from now on.
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
    fn user_config(&self, account: AccountId) -> Config {
        let config = &self.inner.config;
        let storage = match &config.storage {
            Storage::Memory => Storage::Memory,
            Storage::Dir(dir) => Storage::Dir(dir.join(account.0.to_string())),
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

    fn kept_key(&self, current: &Current) -> Result<Option<DeviceKey>> {
        let Some(path) = self.key_path() else {
            return Ok(current.kept.clone());
        };
        match std::fs::read_to_string(&path) {
            Ok(text) => text.parse().map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(Error::Config(format!(
                "device key at {}: {error}",
                path.display()
            ))),
        }
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
