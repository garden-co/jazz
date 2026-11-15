//! An end user's session: who is signed in, whether their credential still
//! holds, and the upstream link's status. Moved here from the TypeScript
//! client (`runtime/auth-state.ts`, the renewal in `runtime/db.ts` and
//! `accounts/context.ts`), so every binding drives the same machine.
//!
//! The machine runs on the client's thread, next to the [`JazzClient`] it
//! renews credentials for. [`Session`] is its `Send + Clone` face: state and
//! status as values and streams, and commands sent to the machine.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_core::Stream;
use jazz::account_registry::AccountId;
use jazz::link::{AuthFailureReason, LinkPhase, LinkStatus};
use jazz::tools::{AppId, JazzClient};
use tokio::sync::{mpsc, oneshot, watch};

use crate::auth::{Bearer, DEVICE_TOKEN_TTL_SECS, DeviceKey, TokenReason, jwt_expiry};
use crate::error::{Error, Result};

/// Why the server refused a credential, as TypeScript's `AuthFailureReason`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub enum AuthFailure {
    Expired,
    Missing,
    Invalid,
    Disabled,
    Revoked,
}

impl AuthFailure {
    /// Whether a new credential can fix it (a revoked or disabled account
    /// cannot).
    pub fn is_renewable(self) -> bool {
        AuthFailureReason::from(self).is_renewable()
    }
}

impl From<AuthFailureReason> for AuthFailure {
    fn from(reason: AuthFailureReason) -> Self {
        match reason {
            AuthFailureReason::Expired => Self::Expired,
            AuthFailureReason::Missing => Self::Missing,
            AuthFailureReason::Invalid => Self::Invalid,
            AuthFailureReason::Disabled => Self::Disabled,
            AuthFailureReason::Revoked => Self::Revoked,
        }
    }
}

impl From<AuthFailure> for AuthFailureReason {
    fn from(reason: AuthFailure) -> Self {
        match reason {
            AuthFailure::Expired => Self::Expired,
            AuthFailure::Missing => Self::Missing,
            AuthFailure::Invalid => Self::Invalid,
            AuthFailure::Disabled => Self::Disabled,
            AuthFailure::Revoked => Self::Revoked,
        }
    }
}

/// Who is signed in, and whether their credential still holds. Serializes
/// as `{"status": "signedIn", "user": {..}}`, `{"status": "expired", "user":
/// {..}, "reason": "expired"}` or `{"status": "signedOut"}`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum SessionState {
    /// Signed in; the client renews the credential before it expires.
    SignedIn { user: User },
    /// The server refused the credential, and a fresh one did not help (or
    /// cannot: a revoked or disabled account). The data stays readable
    /// locally; [`Session::refresh`] tries again.
    Expired { user: User, reason: AuthFailure },
    /// Signed out: by [`crate::Jazz::sign_out`], or because the app's token
    /// provider names no one any more.
    SignedOut,
}

impl SessionState {
    pub fn user(&self) -> Option<&User> {
        match self {
            Self::SignedIn { user } | Self::Expired { user, .. } => Some(user),
            Self::SignedOut => None,
        }
    }
}

/// A signed-in user of the app, as TypeScript's `JazzUser`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct User {
    pub account: AccountId,
    /// `urn:jazz:local-first` for a device account, `jazz:backend` for a
    /// user the app's backend signed in.
    pub issuer: String,
    pub subject: String,
    /// The claims the app's backend gave the user (`{}` for a device).
    pub claims: serde_json::Value,
    pub kind: UserKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub enum UserKind {
    /// A local-first device account ([`crate::Auth::Device`]).
    Device,
    /// A user the app's backend signed in ([`crate::Auth::Bearer`]).
    Backend,
}

/// How the client syncs, as TypeScript's `SyncStatus` (serialized the same
/// way, camelCase).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SyncStatus {
    /// The upstream connection.
    pub link: Link,
    /// Transactions this client committed that the server has neither
    /// accepted nor rejected yet, including ones recovered from local
    /// storage after a restart.
    pub pending_uploads: usize,
    /// When the oldest of them was committed, in Unix ms.
    pub oldest_pending_at: Option<u64>,
    /// This client's transactions the server rejected since it opened.
    pub rejected: u64,
    /// The latest thing that went wrong with sync since the client opened:
    /// a rejected transaction, or the link failing. It stays after the link
    /// recovers (`link` says how things are now).
    pub last_error: Option<SyncError>,
    /// Nothing waits for upload, and the link is connected (or there is no
    /// server).
    pub synced: bool,
}

/// The upstream link, as TypeScript's `SyncStatus.link`: `{"state":
/// "connected"}`, `{"state": "offline", "reason": "network", "retrying":
/// true}` and so on. A dropped link that is retrying reads as `Connecting`
/// until the client publishes an outage, so a short blip never shows as
/// offline.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "state", rename_all = "camelCase")]
pub enum Link {
    /// No server: the app runs locally.
    LocalOnly,
    Connecting,
    Connected,
    /// Not syncing; `retrying` when the client reconnects on its own.
    Offline {
        reason: Offline,
        retrying: bool,
        /// Why the server refused the credential, with `reason: auth`.
        #[serde(skip_serializing_if = "Option::is_none")]
        auth: Option<AuthFailure>,
    },
}

/// Why the link is offline, as TypeScript's `OfflineReason`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Offline {
    /// The network failed or the server is unreachable.
    Network,
    /// The server refused the credential.
    Auth,
    /// A fatal failure; nothing happens until the client is reopened.
    Stopped,
    /// The application took the client offline.
    Disconnected,
}

/// What went wrong with sync, as TypeScript's `SyncError`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum SyncError {
    /// The server rejected a transaction this client committed.
    Rejected {
        /// The public error code, such as `permission_denied`.
        code: String,
        message: String,
        /// When, in Unix ms.
        at: u64,
    },
    /// The link failed this way.
    Network {
        message: String,
        at: u64,
    },
    Auth {
        message: String,
        at: u64,
    },
    Stopped {
        message: String,
        at: u64,
    },
}

impl SyncError {
    fn at(&self) -> u64 {
        match self {
            Self::Rejected { at, .. }
            | Self::Network { at, .. }
            | Self::Auth { at, .. }
            | Self::Stopped { at, .. } => *at,
        }
    }
}

/// The app-facing link, as TypeScript's `linkStatusFromUpstream`.
fn link_of(status: Option<&LinkStatus>) -> Link {
    let offline = |reason, auth| Link::Offline {
        reason,
        retrying: reason == Offline::Network,
        auth,
    };
    let Some(status) = status else {
        return Link::LocalOnly;
    };
    match &status.phase {
        LinkPhase::NoServer => Link::LocalOnly,
        LinkPhase::Offline => offline(Offline::Disconnected, None),
        LinkPhase::Live => Link::Connected,
        LinkPhase::Connecting | LinkPhase::Backoff if status.outage_published => {
            offline(Offline::Network, None)
        }
        LinkPhase::Connecting | LinkPhase::Backoff => Link::Connecting,
        LinkPhase::AwaitingCredential(reason) => offline(Offline::Auth, Some((*reason).into())),
        LinkPhase::Stopped => offline(Offline::Stopped, None),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as u64)
}

/// Assembles the status from what the client reports, remembering the
/// latest link failure (the client keeps only its rejections).
#[derive(Default)]
struct SyncTracker {
    link_failure: Option<SyncError>,
}

impl SyncTracker {
    fn status(&mut self, client: &JazzClient) -> SyncStatus {
        let upstream = client.upstream_status();
        let link = link_of(upstream.as_ref());
        if let Link::Offline { reason, .. } = &link {
            let message = upstream
                .as_ref()
                .and_then(|status| status.reason.clone())
                .unwrap_or_default();
            let same = |failure: &SyncError| match (failure, reason) {
                (SyncError::Network { message: m, .. }, Offline::Network)
                | (SyncError::Auth { message: m, .. }, Offline::Auth)
                | (SyncError::Stopped { message: m, .. }, Offline::Stopped) => *m == message,
                _ => false,
            };
            if !self.link_failure.as_ref().is_some_and(same) {
                let at = now_ms();
                self.link_failure = match reason {
                    Offline::Network => Some(SyncError::Network { message, at }),
                    Offline::Auth => Some(SyncError::Auth { message, at }),
                    Offline::Stopped => Some(SyncError::Stopped { message, at }),
                    Offline::Disconnected => self.link_failure.take(),
                };
            }
        }
        let uploads = client.upload_status();
        let rejection = uploads.last_rejection.map(|refusal| SyncError::Rejected {
            code: refusal.code,
            message: refusal.message,
            at: refusal.at,
        });
        let last_error = match (rejection, self.link_failure.clone()) {
            (Some(rejection), Some(link)) if link.at() > rejection.at() => Some(link),
            (Some(rejection), _) => Some(rejection),
            (None, link) => link,
        };
        SyncStatus {
            synced: uploads.pending == 0 && matches!(link, Link::Connected | Link::LocalOnly),
            link,
            pending_uploads: uploads.pending,
            oldest_pending_at: uploads.oldest_pending_at.filter(|_| uploads.pending > 0),
            rejected: uploads.rejected,
            last_error,
        }
    }
}

/// Publishes the client's sync status as it changes, until the client
/// shuts down.
pub(crate) fn watch_sync(client: JazzClient) -> watch::Receiver<SyncStatus> {
    let mut tracker = SyncTracker::default();
    let mut changes = client.status_changes();
    let (sender, receiver) = watch::channel(tracker.status(&client));
    tokio::task::spawn_local(async move {
        loop {
            if changes.changed().await.is_err() {
                return;
            }
            let status = tracker.status(&client);
            sender.send_if_modified(|current| {
                let changed = *current != status;
                *current = status;
                changed
            });
        }
    });
    receiver
}

/// What the session proves its user with, and renews.
pub(crate) enum Credential {
    Device { key: DeviceKey, app: AppId },
    Bearer { provider: Bearer, token: String },
}

impl Credential {
    /// When to renew the credential the client now holds: at 80% of its
    /// life, as the TypeScript client does.
    fn renew_at(&self, issued: SystemTime) -> Option<SystemTime> {
        let lifetime = match self {
            Self::Device { .. } => DEVICE_TOKEN_TTL_SECS,
            Self::Bearer { token, .. } => {
                let issued_secs = issued.duration_since(UNIX_EPOCH).ok()?.as_secs();
                jwt_expiry(token)?.saturating_sub(issued_secs)
            }
        };
        // A bearer about to expire is renewed when the backend refuses it.
        (lifetime >= MIN_RENEWAL_LIFETIME_SECS)
            .then(|| issued + Duration::from_secs(lifetime * 4 / 5))
    }
}

/// A credential with less life left than this is renewed on refusal only.
const MIN_RENEWAL_LIFETIME_SECS: u64 = 30;

enum Command {
    Refresh(oneshot::Sender<Result<()>>),
    SignOut(oneshot::Sender<()>),
}

/// An end user's session with the app: `Send + Clone`, from
/// [`crate::Jazz::session`]. The FFI's `Session` object wraps it.
#[derive(Clone)]
pub struct Session {
    state: watch::Receiver<SessionState>,
    commands: mpsc::UnboundedSender<Command>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Session")
            .field(&*self.state.borrow())
            .finish()
    }
}

impl Session {
    pub fn state(&self) -> SessionState {
        self.state.borrow().clone()
    }

    /// The state now, then each change.
    pub fn changes(&self) -> impl Stream<Item = SessionState> + Send + Unpin + 'static {
        changes(self.state.clone())
    }

    /// Renews the credential now: a fresh device token, or a fresh bearer
    /// from the token provider ([`TokenReason::Refresh`]). Signs out if the provider
    /// names no one.
    pub async fn refresh(&self) -> Result<()> {
        let (reply, done) = oneshot::channel();
        self.commands
            .send(Command::Refresh(reply))
            .map_err(|_| Error::Stopped)?;
        done.await.map_err(|_| Error::Stopped)?
    }

    /// Stops renewing and reports `SignedOut`. [`crate::Jazz::sign_out`]
    /// also stops the client.
    pub(crate) async fn sign_out(&self) {
        let (reply, done) = oneshot::channel();
        if self.commands.send(Command::SignOut(reply)).is_ok() {
            let _ = done.await;
        }
    }
}

/// A watch's value now, then each change.
pub(crate) fn changes<T: Clone + Send + Sync + 'static>(
    receiver: watch::Receiver<T>,
) -> impl Stream<Item = T> + Send + Unpin + 'static {
    Box::pin(futures_util::stream::unfold(
        (receiver, true),
        |(mut receiver, first)| async move {
            if !first && receiver.changed().await.is_err() {
                return None;
            }
            let value = receiver.borrow_and_update().clone();
            Some((value, (receiver, false)))
        },
    ))
}

/// Starts `user`'s session on the client's thread.
pub(crate) fn start(
    client: JazzClient,
    user: User,
    credential: Credential,
    sync: watch::Receiver<SyncStatus>,
) -> Session {
    let (state, receiver) = watch::channel(SessionState::SignedIn { user: user.clone() });
    let (commands, inbox) = mpsc::unbounded_channel();
    let machine = Machine {
        client,
        user,
        credential,
        state,
        renew_at: None,
        renewed_since_live: false,
    };
    tokio::task::spawn_local(machine.run(sync, inbox));
    Session {
        state: receiver,
        commands,
    }
}

struct Machine {
    client: JazzClient,
    user: User,
    credential: Credential,
    state: watch::Sender<SessionState>,
    renew_at: Option<SystemTime>,
    /// A fresh credential was given after a refusal and not yet admitted:
    /// a second refusal is the credential's fault, not its age.
    renewed_since_live: bool,
}

/// What renewing found.
enum Renewed {
    Renewed,
    SignedOut,
}

impl Machine {
    async fn run(
        mut self,
        mut sync: watch::Receiver<SyncStatus>,
        mut inbox: mpsc::UnboundedReceiver<Command>,
    ) {
        self.renew_at = self.credential.renew_at(SystemTime::now());
        loop {
            let until_renewal = self
                .renew_at
                .map(|at| at.duration_since(SystemTime::now()).unwrap_or_default());
            tokio::select! {
                changed = sync.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let link = sync.borrow_and_update().link.clone();
                    if self.on_link(link).await.is_break() {
                        return;
                    }
                }
                () = sleep(until_renewal) => {
                    // Ahead of expiry: a failure here is retried on refusal.
                    match self.renew(TokenReason::Renew).await {
                        Ok(Renewed::SignedOut) => return self.signed_out(),
                        Ok(Renewed::Renewed) => {}
                        Err(_) => self.renew_at = None,
                    }
                }
                command = inbox.recv() => match command {
                    Some(Command::Refresh(reply)) => {
                        let result = self.renew(TokenReason::Refresh).await;
                        // The state changes before the caller hears back.
                        let signed_out = matches!(result, Ok(Renewed::SignedOut));
                        if signed_out {
                            self.signed_out();
                        }
                        let _ = reply.send(result.map(|_| ()));
                        if signed_out {
                            return;
                        }
                    }
                    Some(Command::SignOut(reply)) => {
                        self.signed_out();
                        let _ = reply.send(());
                        return;
                    }
                    None => return,
                },
            }
        }
    }

    async fn on_link(&mut self, link: Link) -> std::ops::ControlFlow<()> {
        match link {
            Link::Connected => {
                self.renewed_since_live = false;
                self.set(SessionState::SignedIn {
                    user: self.user.clone(),
                });
            }
            Link::Offline {
                auth: Some(reason), ..
            } => {
                if reason.is_renewable() && !self.renewed_since_live {
                    self.renewed_since_live = true;
                    match self.renew(TokenReason::Expired).await {
                        Ok(Renewed::Renewed) => return std::ops::ControlFlow::Continue(()),
                        Ok(Renewed::SignedOut) => {
                            self.signed_out();
                            return std::ops::ControlFlow::Break(());
                        }
                        Err(_) => {}
                    }
                }
                self.set(SessionState::Expired {
                    user: self.user.clone(),
                    reason,
                });
            }
            _ => {}
        }
        std::ops::ControlFlow::Continue(())
    }

    /// Gives the client a new credential.
    async fn renew(&mut self, reason: TokenReason) -> Result<Renewed> {
        let now = SystemTime::now();
        match &mut self.credential {
            Credential::Device { key, app } => {
                let token = key.mint(*app, DEVICE_TOKEN_TTL_SECS)?;
                self.client.update_auth_token(token)?;
            }
            Credential::Bearer { provider, token } => match provider.token(reason).await? {
                None => return Ok(Renewed::SignedOut),
                Some(next) => {
                    // An unchanged bearer needs no new connection.
                    if reason.is_fresh() || next != *token {
                        self.client.update_bearer(next.clone())?;
                    }
                    *token = next;
                }
            },
        }
        self.renew_at = self.credential.renew_at(now);
        Ok(Renewed::Renewed)
    }

    fn signed_out(&mut self) {
        self.renew_at = None;
        self.set(SessionState::SignedOut);
    }

    fn set(&self, state: SessionState) {
        self.state.send_if_modified(|current| {
            let changed = *current != state;
            *current = state;
            changed
        });
    }
}

async fn sleep(duration: Option<Duration>) {
    match duration {
        Some(duration) => tokio::time::sleep(duration).await,
        None => std::future::pending().await,
    }
}

impl crate::Jazz {
    /// The end user's session, for a client opened with
    /// [`crate::Config::user`]: who is signed in, and whether their
    /// credential still holds.
    pub fn session(&self) -> Option<&Session> {
        self.info.session.as_ref()
    }

    /// How the client syncs now.
    pub fn sync_status(&self) -> SyncStatus {
        self.info.sync.borrow().clone()
    }

    /// How the client syncs now, then each change.
    pub fn sync_changes(&self) -> impl Stream<Item = SyncStatus> + Send + Unpin + 'static {
        changes(self.info.sync.clone())
    }

    /// Signs the end user out: the session reports `SignedOut`, and the
    /// client stops ([`crate::Jazz::shutdown`]). Their local store stays
    /// (in [`crate::Storage::Dir`]) for when they sign in again; a device
    /// account's key is the app's to keep or forget.
    pub async fn sign_out(&self) -> Result<()> {
        let session = self.session().ok_or_else(|| {
            Error::Config("only an end user's client signs out (Config::user)".into())
        })?;
        session.sign_out().await;
        self.shutdown().await
    }
}
