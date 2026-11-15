//! An end user's session: who is signed in, whether their credential still
//! holds, and the upstream link's status. Moved here from the TypeScript
//! client (`runtime/auth-state.ts`, the renewal in `runtime/db.ts` and
//! `accounts/context.ts`), so every binding drives the same machine.
//!
//! The machine runs on the client's thread, next to the [`JazzClient`] it
//! renews credentials for. [`Session`] is its `Send + Clone` face: state and
//! status as values and streams, and commands sent to the machine.

use std::ops::ControlFlow;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::time::Instant;

use futures_core::Stream;
use jazz::account_registry::AccountId;
use jazz::link::{AuthFailureReason, LinkPhase, LinkStatus};
use jazz::tools::{AppId, JazzClient};
use tokio::sync::{mpsc, oneshot, watch};

use crate::auth::{Bearer, DEVICE_TOKEN_TTL_SECS, DeviceKey, TokenFuture, TokenReason, jwt_expiry};
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
    /// How many times the server refused the credential: a refusal of a
    /// renewed one leaves `link` as it was.
    #[serde(skip)]
    pub(crate) refusals: u64,
}

/// The upstream link, as TypeScript's `SyncStatus.link`: `{"state":
/// "connected"}`, `{"state": "offline", "reason": "network", "retrying":
/// true}` and so on. A dropped link that is retrying reads as `Connecting`
/// until the client publishes an outage, so a short blip never shows as
/// offline.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum Link {
    /// No server: the app runs locally.
    LocalOnly,
    Connecting,
    Connected,
    /// Not syncing; `retrying` when the client reconnects on its own.
    Offline {
        reason: Offline,
        retrying: bool,
        /// Why the server refused the credential, with `reason: auth`. An
        /// extension of TypeScript's shape, which has no such field.
        #[serde(skip_serializing_if = "Option::is_none")]
        auth: Option<AuthFailure>,
    },
}

/// Why the link is offline, as TypeScript's `OfflineReason`. This client
/// does not report TypeScript's `authority-changed` (its link has no such
/// phase yet) nor the browser's `held-by-another-tab`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
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
#[serde(tag = "kind", rename_all = "kebab-case")]
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
/// latest link failure (the client keeps only its rejections), as
/// TypeScript's `noteLinkFailure`: a failure is recorded the first time the
/// link is seen failing that way, and stays after the link recovers.
#[derive(Default)]
struct SyncTracker {
    /// How the link is failing now, if it is.
    failing_as: Option<Offline>,
    link_failure: Option<SyncError>,
}

impl SyncTracker {
    fn status(&mut self, client: &JazzClient) -> SyncStatus {
        let upstream = client.upstream_status();
        let link = link_of(upstream.as_ref());
        let failing = match &link {
            Link::Offline { reason, .. } => Some(*reason),
            _ => None,
        };
        if failing != self.failing_as {
            self.failing_as = failing;
            let message = upstream.as_ref().and_then(|status| status.reason.clone());
            if let Some(failure) = failing.and_then(|reason| link_failure(reason, message)) {
                self.link_failure = Some(failure);
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
            refusals: client.auth_refusals(),
        }
    }
}

/// The failure an offline link reports, as TypeScript's `linkFailureOf`:
/// none for a link the app took offline, and TypeScript's words when the
/// client gave none.
fn link_failure(reason: Offline, message: Option<String>) -> Option<SyncError> {
    let said = |fallback: &str| {
        message
            .clone()
            .filter(|message| !message.is_empty())
            .unwrap_or_else(|| fallback.to_owned())
    };
    let at = now_ms();
    match reason {
        Offline::Disconnected => None,
        Offline::Network => Some(SyncError::Network {
            message: said("The server is unreachable; retrying."),
            at,
        }),
        Offline::Auth => Some(SyncError::Auth {
            message: said("The server refused this client's credential."),
            at,
        }),
        Offline::Stopped => Some(SyncError::Stopped {
            message: said("The link stopped for good; only reopening the client tries again."),
            at,
        }),
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
    fn renew_in(&self) -> Option<Duration> {
        let lifetime = match self {
            Self::Device { .. } => DEVICE_TOKEN_TTL_SECS,
            Self::Bearer { token, .. } => {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
                jwt_expiry(token)?.saturating_sub(now)
            }
        };
        // A bearer about to expire is renewed when the backend refuses it.
        (lifetime >= MIN_RENEWAL_LIFETIME_SECS).then(|| Duration::from_secs(lifetime * 4 / 5))
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

/// Stops the client, as [`crate::Jazz::shutdown`] does: when the token
/// provider signs the user out, nothing may sync as them any more.
pub(crate) type Stop = Box<dyn FnOnce()>;

/// Starts `user`'s session on the client's thread.
pub(crate) fn start(
    client: JazzClient,
    user: User,
    credential: Credential,
    sync: watch::Receiver<SyncStatus>,
    stop: Stop,
) -> Session {
    let (state, receiver) = watch::channel(SessionState::SignedIn { user: user.clone() });
    let (commands, inbox) = mpsc::unbounded_channel();
    let machine = Machine {
        client,
        user,
        credential,
        state,
        stop: Some(stop),
        renewal: None,
        pending: None,
        streak: 0,
        failures: 0,
        live_since: None,
        refusals: sync.borrow().refusals,
    };
    tokio::task::spawn_local(machine.run(sync, inbox));
    Session {
        state: receiver,
        commands,
    }
}

/// Delay before retry `attempt` (from 0): 1 s doubling, capped at 5 minutes,
/// as TypeScript's `authRetryDelay`.
fn auth_retry_delay(attempt: u32) -> Duration {
    Duration::from_millis((1_000u64 << attempt.min(20)).min(5 * 60_000))
}

/// A link admitted at least this long ago was not refused on arrival: its
/// refusal starts a new renewal streak.
const LIVED: Duration = Duration::from_secs(30);

/// A renewal waiting for its time.
struct Scheduled {
    at: Instant,
    reason: TokenReason,
}

/// A token the provider is still working on, and who waits for it.
struct Pending {
    reason: TokenReason,
    token: TokenFuture,
    replies: Vec<oneshot::Sender<Result<()>>>,
}

/// The session state machine, as the TypeScript client renews
/// (`accounts/context.ts`, `runtime/auth-renewal-backoff.ts`):
///
/// - a credential is renewed at 80% of its life;
/// - a renewable refusal renews it: at once, then with a backoff for each
///   refusal in a row (a streak ends once a credential lives), and the
///   session reads `Expired` from the second;
/// - a failed renewal (the provider erred) is retried with a backoff;
/// - a refusal no credential fixes (revoked, disabled) is `Expired` for good;
/// - a provider that names no one signs out, and stops the client.
///
/// The provider's future is polled beside the machine's other inputs, so a
/// slow provider (an interactive sign-in) never holds up a sign-out.
struct Machine {
    client: JazzClient,
    user: User,
    credential: Credential,
    state: watch::Sender<SessionState>,
    stop: Option<Stop>,
    renewal: Option<Scheduled>,
    pending: Option<Pending>,
    /// Refusals renewed since a credential last lived.
    streak: u32,
    /// Failed renewals in a row.
    failures: u32,
    live_since: Option<Instant>,
    /// The refusals seen so far ([`SyncStatus::refusals`]).
    refusals: u64,
}

impl Machine {
    async fn run(
        mut self,
        mut sync: watch::Receiver<SyncStatus>,
        mut inbox: mpsc::UnboundedReceiver<Command>,
    ) {
        self.schedule_ahead();
        loop {
            let at = self.renewal.as_ref().map(|renewal| renewal.at);
            let flow = tokio::select! {
                changed = sync.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let status = sync.borrow_and_update().clone();
                    self.on_link(status)
                }
                () = sleep_until(at), if self.pending.is_none() => {
                    let Scheduled { reason, .. } = self.renewal.take().expect("scheduled");
                    // A credential that lived to its renewal ends the streak.
                    if reason == TokenReason::Renew {
                        self.streak = 0;
                    }
                    self.renew(reason, None)
                }
                token = poll_pending(&mut self.pending) => {
                    let Pending { reason, replies, .. } =
                        self.pending.take().expect("pending");
                    self.received(reason, token, replies)
                }
                command = inbox.recv() => match command {
                    Some(Command::Refresh(reply)) => {
                        self.renew(TokenReason::Refresh, Some(reply))
                    }
                    Some(Command::SignOut(reply)) => {
                        // The caller stops the client itself.
                        self.stop = None;
                        self.sign_out();
                        let _ = reply.send(());
                        ControlFlow::Break(())
                    }
                    None => ControlFlow::Break(()),
                },
            };
            if flow.is_break() {
                return;
            }
        }
    }

    fn on_link(&mut self, status: SyncStatus) -> ControlFlow<()> {
        // Only a new refusal is acted on: the status stays refused while a
        // renewal is on its way.
        let refused = status.refusals > self.refusals;
        self.refusals = status.refusals;
        match status.link {
            Link::Connected => {
                self.live_since.get_or_insert_with(Instant::now);
                self.set(SessionState::SignedIn {
                    user: self.user.clone(),
                });
            }
            Link::Offline {
                auth: Some(reason), ..
            } if refused => {
                if self
                    .live_since
                    .take()
                    .is_some_and(|since| since.elapsed() >= LIVED)
                {
                    self.streak = 0;
                }
                // Refused again right after a renewal, or for good.
                if self.streak > 0 || !reason.is_renewable() {
                    self.set(SessionState::Expired {
                        user: self.user.clone(),
                        reason,
                    });
                }
                if reason.is_renewable() && self.pending.is_none() {
                    let delay = match self.streak {
                        0 => Duration::ZERO,
                        streak => auth_retry_delay(streak - 1),
                    };
                    self.streak += 1;
                    self.renewal = Some(Scheduled {
                        at: Instant::now() + delay,
                        reason: TokenReason::Expired,
                    });
                }
            }
            Link::Offline { .. } | Link::LocalOnly | Link::Connecting => {}
        }
        ControlFlow::Continue(())
    }

    /// Gives the client a new credential: a device's at once, a bearer's
    /// once the provider gives it.
    fn renew(
        &mut self,
        reason: TokenReason,
        reply: Option<oneshot::Sender<Result<()>>>,
    ) -> ControlFlow<()> {
        let replies = reply.into_iter().collect();
        match &self.credential {
            Credential::Device { key, app } => {
                let renewed = key
                    .mint(*app, DEVICE_TOKEN_TTL_SECS)
                    .and_then(|token| Ok(self.client.update_auth_token(token)?));
                self.renewed(reason, renewed, replies);
                ControlFlow::Continue(())
            }
            Credential::Bearer { provider, .. } => {
                match &mut self.pending {
                    // One request at a time: this caller waits for it too.
                    Some(pending) => pending.replies.extend(replies),
                    None => {
                        self.pending = Some(Pending {
                            reason,
                            token: provider.request(reason),
                            replies,
                        });
                    }
                }
                ControlFlow::Continue(())
            }
        }
    }

    fn received(
        &mut self,
        reason: TokenReason,
        token: Result<Option<String>>,
        replies: Vec<oneshot::Sender<Result<()>>>,
    ) -> ControlFlow<()> {
        let next = match token {
            Ok(Some(next)) => next,
            Ok(None) => {
                self.sign_out();
                for reply in replies {
                    let _ = reply.send(Ok(()));
                }
                return ControlFlow::Break(());
            }
            Err(error) => {
                self.renewed(reason, Err(error), replies);
                return ControlFlow::Continue(());
            }
        };
        let Credential::Bearer { token, .. } = &mut self.credential else {
            unreachable!("only a bearer is requested");
        };
        // An unchanged bearer needs no new connection, unless it was asked
        // for because the last one was refused.
        let renewed = if reason.is_fresh() || next != *token {
            self.client.update_bearer(next.clone()).map_err(Error::from)
        } else {
            Ok(())
        };
        if renewed.is_ok() {
            *token = next;
        }
        self.renewed(reason, renewed, replies);
        ControlFlow::Continue(())
    }

    /// After a renewal: the next one ahead of expiry, or a retry.
    fn renewed(
        &mut self,
        reason: TokenReason,
        result: Result<()>,
        replies: Vec<oneshot::Sender<Result<()>>>,
    ) {
        match result {
            Ok(()) => {
                self.failures = 0;
                // A refusal's renewal is already followed by the link.
                if self.renewal.is_none() {
                    self.schedule_ahead();
                }
                for reply in replies {
                    let _ = reply.send(Ok(()));
                }
            }
            Err(error) => {
                // A failed renewal must not end renewing: the credential
                // still expires, and nothing else would renew it.
                self.renewal = Some(Scheduled {
                    at: Instant::now() + auth_retry_delay(self.failures),
                    reason: if reason == TokenReason::Renew {
                        TokenReason::Renew
                    } else {
                        TokenReason::Expired
                    },
                });
                self.failures += 1;
                let message = error.to_string();
                let mut error = Some(error);
                for reply in replies {
                    let error = error
                        .take()
                        .unwrap_or_else(|| Error::Server(message.clone()));
                    let _ = reply.send(Err(error));
                }
            }
        }
    }

    /// Schedules the renewal at 80% of the credential's life.
    fn schedule_ahead(&mut self) {
        self.renewal = self.credential.renew_in().map(|after| Scheduled {
            at: Instant::now() + after,
            reason: TokenReason::Renew,
        });
    }

    fn sign_out(&mut self) {
        self.renewal = None;
        self.pending = None;
        self.set(SessionState::SignedOut);
        if let Some(stop) = self.stop.take() {
            stop();
        }
    }

    fn set(&self, state: SessionState) {
        self.state.send_if_modified(|current| {
            let changed = *current != state;
            *current = state;
            changed
        });
    }
}

async fn sleep_until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

async fn poll_pending(pending: &mut Option<Pending>) -> Result<Option<String>> {
    match pending {
        Some(pending) => pending.token.as_mut().await,
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
