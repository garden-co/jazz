//! **Unstable** (feature `unstable-endpoint`, off by default, not part of a
//! release yet): run an app's main backend with TS `createJazz`.
//!
//! The app's Jazz endpoint, served by its backend: what `createJazz`'s
//! handler at `/jazz` does for the app's users (`packages/jazz-tools/src/
//! backend/create-jazz.ts`), so a Rust backend fronts its users' browser and
//! native clients the same way.
//!
//! Cores admit only their app's backend. An end user's client talks to the
//! backend's endpoint, never to the core:
//! - `POST <base>/apps/<id>/accounts/backend-sign-in` and `backend-user`:
//!   the app's own sign-in ([`Authenticate`]) names the user, and the core
//!   resolves (or creates) their account;
//! - `<base>/apps/<id>/ws`: the user's sync socket, whose prelude the
//!   backend admits on its own word and relays to the core, pinned to the
//!   permissions bundle it published ([`Endpoint::serve_socket`]). A thin
//!   client's transactions are served here, by an in-memory client linked
//!   to the core as the same session (SPEC 8.14): the core only accepts
//!   them finished.
//! - `POST <base>/apps/<id>/accounts/backend-link[/challenge]`: a device
//!   account links to the user who signs up, proving its key on a
//!   challenge issued for them.
//!
//! The endpoint is framework-neutral: the host's HTTP server passes requests
//! to [`Endpoint::handle`], and the sockets it upgrades to
//! [`Endpoint::serve_socket`]. With the `axum` feature, `Endpoint::router`
//! does both.
//!
//! Not supported yet, as explicit answers: trusted backend clients (`backendClients`,
//! 501), the HTTP-stream fallback for hosts without WebSockets (501),
//! renewing a cookie session in place (sessions reconnect at their expiry
//! instead), and device-account proofs signed for an endpoint URL rather than
//! the app id.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use futures_util::{Sink, SinkExt as _, Stream, StreamExt as _};
use jazz::account_registry::AccountId;
use jazz::tools::Session;
use jazz::wire::{
    WireError, WireErrorCode, WireErrorReason, WireFrame, WireRetry, encode_websocket_frame_batch,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::error::{Error, Result};
use crate::handle::{BackendLink, Jazz};

#[cfg(feature = "axum")]
mod axum;
mod thin;

/// The issuer `createJazz` names the app's users by.
const BACKEND_ISSUER: &str = "jazz:backend";
/// The issuer of a device account's self-signed token.
const DEVICE_ACCOUNT_ISSUER: &str = "urn:jazz:local-first";
/// The prefix of the error that asks a client to connect again.
const REAUTHENTICATE_PREFIX: &str = "reauthenticate:";
const DEFAULT_BASE_PATH: &str = "/jazz";
const DEFAULT_SESSION_TTL: Duration = Duration::from_secs(3600);
/// How long a socket may take to send its prelude.
const PRELUDE_DEADLINE: Duration = Duration::from_secs(30);
/// How long before a session's expiry its client is asked to reconnect.
const REAUTHENTICATE_LEAD: Duration = Duration::from_secs(30);
/// The least a session runs before it is asked to reconnect.
const MIN_SESSION: Duration = Duration::from_secs(5);
const ACCOUNT_CACHE_LIMIT: usize = 10_000;
const CORE_TIMEOUT: Duration = Duration::from_secs(30);
/// The close code of a socket ended for its credentials (policy violation).
const CLOSE_POLICY: u16 = 1008;
/// How long a link challenge holds.
const LINK_CHALLENGE_TTL: Duration = Duration::from_secs(120);
/// The core's refusal of every device-account identity, when it admits none.
const DEVICE_ACCOUNTS_DISABLED: &str = "local_first_auth_disabled";
/// The largest prelude a socket may open with.
const MAX_PRELUDE_BYTES: usize = 64 * 1024;
/// The largest message a client may send: the core's own cap on a
/// WebSocket message (one wire frame).
const MAX_CLIENT_MESSAGE_BYTES: usize = jazz::protocol_limits::MAX_WIRE_FRAME_BYTES;
/// The largest request body the account routes read.
const MAX_BODY_BYTES: usize = 64 * 1024;
/// How long connecting to the core may take.
const CORE_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// How many sockets an endpoint serves at once, by default.
const DEFAULT_MAX_SOCKETS: usize = 10_000;
/// Thin sessions with transactions served at once, each by a client (and a
/// thread) of its own.
const DEFAULT_MAX_TRANSACTION_CLIENTS: usize = 256;

/// An HTTP request as the host's server received it: what
/// [`Authenticate`] reads (cookies, `Authorization`), and what
/// [`Endpoint::handle`] answers. Header names compare case-insensitively.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    pub method: String,
    /// The request's path, with its query string if any
    /// (`/jazz/apps/<id>/accounts/backend-sign-in`).
    pub path: String,
    pub headers: Vec<(String, String)>,
    #[serde(default)]
    pub body: Vec<u8>,
}

impl Request {
    pub fn new(method: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            method: method.into(),
            path: path.into(),
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// The first value of header `name`.
    pub fn header_value(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// The `Authorization: Bearer` token.
    pub fn bearer(&self) -> Option<&str> {
        let header = self.header_value("authorization")?;
        let (scheme, token) = header.split_once(' ')?;
        scheme
            .eq_ignore_ascii_case("bearer")
            .then(|| token.trim())
            .filter(|token| !token.is_empty())
    }

    /// The cookie `name`.
    pub fn cookie(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .filter(|(key, _)| key.eq_ignore_ascii_case("cookie"))
            .flat_map(|(_, value)| value.split(';'))
            .find_map(|pair| {
                let (key, value) = pair.trim().split_once('=')?;
                (key == name).then_some(value)
            })
    }

    /// This request without the credentials a browser sends with it (its
    /// cookies, its `Authorization` header), as `createJazz` asks
    /// `authenticate` about a device account's socket.
    fn without_credentials(&self) -> Self {
        Self {
            method: self.method.clone(),
            path: self.path.clone(),
            headers: self
                .headers
                .iter()
                .filter(|(key, _)| {
                    !key.eq_ignore_ascii_case("cookie")
                        && !key.eq_ignore_ascii_case("authorization")
                })
                .cloned()
                .collect(),
            body: Vec::new(),
        }
    }

    fn path_only(&self) -> &str {
        self.path.split(['?', '#']).next().unwrap_or_default()
    }
}

/// What [`Endpoint::handle`] answers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    fn text(status: u16, body: &str) -> Self {
        Self {
            status,
            headers: vec![("content-type".into(), "text/plain; charset=utf-8".into())],
            body: body.as_bytes().to_vec(),
        }
    }

    fn json(value: &Value) -> Self {
        Self {
            status: 200,
            headers: vec![
                ("content-type".into(), "application/json".into()),
                ("cache-control".into(), "no-store".into()),
            ],
            body: serde_json::to_vec(value).unwrap_or_default(),
        }
    }

    /// The body as text, for tests and logs.
    pub fn text_body(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// A user the app's own auth signed in, as `authenticate` names them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthUser {
    /// Stable user id in the app's auth. Becomes the identity's subject.
    pub user_id: String,
    /// Extra claims the permissions read through `session.claims`.
    #[serde(default)]
    pub claims: Map<String, Value>,
    /// When this answer stops holding, as Unix time in seconds: the app
    /// session's expiry, or its token's `exp`. A sync session is asked to
    /// reconnect shortly before, and `authenticate` decides again.
    pub expires_at: u64,
}

impl AuthUser {
    pub fn new(user_id: impl Into<String>, expires_at: u64) -> Self {
        Self {
            user_id: user_id.into(),
            claims: Map::new(),
            expires_at,
        }
    }

    pub fn claims(mut self, claims: Map<String, Value>) -> Self {
        self.claims = claims;
        self
    }
}

/// `authenticate`'s answer for a request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Answer {
    /// This user is signed in.
    User(AuthUser),
    /// Let a device account (a guest whose key lives on the
    /// user's device) through, when the request comes from one: TS
    /// `acceptDeviceAccounts`. For any other request it means no one is
    /// signed in. Device accounts are refused unless `authenticate` answers
    /// this for them, since only the permissions stand between them and the
    /// app's data.
    DeviceAccount,
}

/// What becomes of a device key once it linked to a user, as TS
/// `guestKeyAfterLink`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum GuestKeyAfterLink {
    /// The key stays a credential of the account (TS's default): the
    /// device syncs on as before. A key that linked can link again, and
    /// a second user it links to joins the same account and reads its
    /// data, so only keep keys when a device is its user's alone.
    #[default]
    Keep,
    /// The key is revoked once it linked: the device signs in as the user
    /// from then on, a leaked key opens nothing, and it cannot link a
    /// second user.
    Revoke,
}

/// What [`Authenticate::authenticate`] returns.
pub type AuthFuture = Pin<Box<dyn Future<Output = Result<Option<Answer>>> + Send>>;

/// The app's sign-in: who a request's user is, from its cookies or bearer.
/// `None` means no one is signed in. An error answers the request with a
/// server error (`authenticate_failed`) and refuses the socket.
///
/// Closures implement it:
///
/// ```no_run
/// use jazz_app_support::endpoint::{Answer, AuthUser, Request};
/// let authenticate = |request: Request| async move {
///     Ok(request.bearer().map(|token| Answer::User(AuthUser::new(token, u64::MAX))))
/// };
/// # let _: &dyn jazz_app_support::endpoint::Authenticate = &authenticate;
/// ```
pub trait Authenticate: Send + Sync + 'static {
    fn authenticate(&self, request: Request) -> AuthFuture;
}

impl<F, Fut> Authenticate for F
where
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Option<Answer>>> + Send + 'static,
{
    fn authenticate(&self, request: Request) -> AuthFuture {
        Box::pin(self(request))
    }
}

/// How a socket [`Endpoint::serve_socket`] served ended: the host closes
/// its WebSocket with this code and reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Closed {
    pub code: u16,
    pub reason: String,
}

impl Closed {
    fn normal() -> Self {
        Self {
            code: 1000,
            reason: String::new(),
        }
    }

    fn policy(reason: &str) -> Self {
        Self {
            code: CLOSE_POLICY,
            reason: reason.into(),
        }
    }
}

/// The app's Jazz endpoint, served by its backend. `Clone + Send + Sync`:
/// every clone serves the same endpoint.
///
/// - **Linking device accounts** (`backend-link`): by default a device key
///   that linked to a user stays a credential of their account
///   ([`GuestKeyAfterLink::Keep`], TS's default). A key that linked can
///   link again, and a second user it links to joins the same account. Use
///   [`Endpoint::guest_key_after_link`] with [`GuestKeyAfterLink::Revoke`]
///   unless a device is its user's alone.
/// - **The admin secret must be random** (at least 32 bytes): link
///   challenges, which every signed-in user receives, are MACs under a key
///   derived from it, so a guessable secret could be found offline.
///
/// ```no_run
/// # async fn serve(jazz: jazz_app_support::Jazz) -> jazz_app_support::Result<()> {
/// use jazz_app_support::endpoint::{Answer, AuthUser, Endpoint, Request};
/// let endpoint = Endpoint::new(&jazz, |request: Request| async move {
///     // The app's own sign-in, e.g. its session cookie.
///     Ok(request.cookie("session").map(|id| Answer::User(AuthUser::new(id, u64::MAX))))
/// })?;
/// let response = endpoint
///     .handle(Request::new("POST", "/jazz/apps/<id>/accounts/backend-sign-in"))
///     .await;
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct Endpoint {
    inner: Arc<Inner>,
}

struct Inner {
    jazz: Jazz,
    /// The client the endpoint acts for users with, opened on first use:
    /// trusted by the core to act for the sessions its calls name.
    users: Arc<tokio::sync::OnceCell<Jazz>>,
    core: BackendLink,
    bundle: String,
    authenticate: Arc<dyn Authenticate>,
    base_path: String,
    session_ttl: Duration,
    guest_key_after_link: GuestKeyAfterLink,
    max_sockets: usize,
    /// Sockets open now.
    sockets: Arc<std::sync::atomic::AtomicUsize>,
    max_transaction_clients: usize,
    /// How long a thin socket's requests may wait on an answer.
    answer_deadline: Duration,
    /// Thin sessions' transaction clients running now.
    transaction_clients: Arc<std::sync::atomic::AtomicUsize>,
    http: reqwest::Client,
    /// What link challenges are signed with: derived from the admin secret,
    /// so every instance of the app's backend reads the others'.
    challenge_key: Vec<u8>,
    /// Users' accounts, which never change once assigned, kept for a
    /// while so the core isn't asked on every request.
    accounts: Mutex<HashMap<String, (AccountId, Instant)>>,
}

impl std::fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Endpoint")
            .field("app_id", &self.inner.core.app_id)
            .field("base_path", &self.inner.base_path)
            .finish_non_exhaustive()
    }
}

/// Who an admitted socket is for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admitted {
    /// A signed-in user, by the id `authenticate` gave.
    User(String),
    /// A device account, on its own proof.
    DeviceAccount,
}

enum Admission {
    Admitted {
        prelude: Vec<u8>,
        who: Admitted,
        /// Who the socket's session is, for the client that serves a thin
        /// session's transactions.
        auth: thin::SessionAuth,
        /// When the client is asked to reconnect.
        until: Instant,
    },
    /// The client sent a credential this backend did not accept.
    Unverified,
    /// The client sent no credential that names anyone.
    Missing,
    Expired,
}

impl Endpoint {
    /// The endpoint of `jazz`, a backend ([`crate::Config::backend`]), with
    /// the app's sign-in. It serves under `/jazz`.
    pub fn new(jazz: &Jazz, authenticate: impl Authenticate) -> Result<Self> {
        let (Some(core), Some(bundle)) = (jazz.info.backend.clone(), jazz.bundle()) else {
            return Err(Error::Config(
                "an endpoint is served by the app's backend: open it with Config::backend".into(),
            ));
        };
        if jazz.acts_for_user() {
            return Err(Error::Config(
                "an endpoint is served by the backend's own handle, not one acting for a user"
                    .into(),
            ));
        }
        let challenge_key = {
            use hmac::Mac as _;
            let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(core.admin_secret.as_bytes())
                .expect("HMAC takes a key of any length");
            mac.update(format!("jazz-link-challenge-v1\0{}", core.app_id).as_bytes());
            mac.finalize().into_bytes().to_vec()
        };
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(CORE_TIMEOUT)
            .build()
            .map_err(|error| Error::Server(error.to_string()))?;
        Ok(Self {
            inner: Arc::new(Inner {
                jazz: jazz.clone(),
                users: Arc::default(),
                core,
                bundle: bundle.to_owned(),
                authenticate: Arc::new(authenticate),
                base_path: DEFAULT_BASE_PATH.into(),
                session_ttl: DEFAULT_SESSION_TTL,
                guest_key_after_link: GuestKeyAfterLink::Keep,
                max_sockets: DEFAULT_MAX_SOCKETS,
                sockets: Arc::default(),
                max_transaction_clients: DEFAULT_MAX_TRANSACTION_CLIENTS,
                answer_deadline: thin::ANSWER_DEADLINE,
                transaction_clients: Arc::default(),
                http,
                challenge_key,
                accounts: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Serve under `base_path` instead of `/jazz` (`""` for the root).
    pub fn base_path(self, base_path: impl Into<String>) -> Self {
        let base_path = base_path.into().trim_end_matches('/').to_owned();
        self.reconfigure(|inner| inner.base_path = base_path)
    }

    /// The longest a sync session runs before its client is asked to
    /// reconnect (TS `sessionTtlSeconds`, default an hour), and how long a
    /// user's account is cached.
    /// What becomes of a device key once it linked to a user
    /// (`backend-link`; TS `guestKeyAfterLink`). See [`GuestKeyAfterLink`].
    pub fn guest_key_after_link(self, after: GuestKeyAfterLink) -> Self {
        self.reconfigure(|inner| inner.guest_key_after_link = after)
    }

    pub fn session_ttl(self, ttl: Duration) -> Self {
        self.reconfigure(|inner| inner.session_ttl = ttl)
    }

    /// How many sockets this endpoint serves at once (default 10 000): one
    /// more is closed with 1013, try again later.
    pub fn max_sockets(self, max: usize) -> Self {
        self.reconfigure(|inner| inner.max_sockets = max)
    }

    /// How many thin sessions this endpoint serves transactions for at once
    /// (default 256), each with an in-memory client and a thread of its
    /// own, from its first transaction request to the end of its socket.
    /// One more session's transaction requests are refused with
    /// `backpressure`, to try again later.
    pub fn max_transaction_clients(self, max: usize) -> Self {
        self.reconfigure(|inner| inner.max_transaction_clients = max)
    }

    /// How long a thin socket's requests may wait on an answer, from the
    /// core or the session's client, since the last one came (default 60
    /// s): past it the socket is closed with 1011, and the client
    /// reconnects.
    #[doc(hidden)]
    pub fn answer_deadline(self, deadline: Duration) -> Self {
        self.reconfigure(|inner| inner.answer_deadline = deadline)
    }

    /// How many thin sessions' transaction clients run now.
    #[doc(hidden)]
    pub fn transaction_clients(&self) -> usize {
        self.inner
            .transaction_clients
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn reconfigure(self, change: impl FnOnce(&mut Inner)) -> Self {
        let mut inner = match Arc::try_unwrap(self.inner) {
            Ok(inner) => inner,
            Err(shared) => Inner {
                jazz: shared.jazz.clone(),
                users: shared.users.clone(),
                core: shared.core.clone(),
                bundle: shared.bundle.clone(),
                authenticate: shared.authenticate.clone(),
                base_path: shared.base_path.clone(),
                session_ttl: shared.session_ttl,
                guest_key_after_link: shared.guest_key_after_link,
                max_sockets: shared.max_sockets,
                sockets: shared.sockets.clone(),
                max_transaction_clients: shared.max_transaction_clients,
                answer_deadline: shared.answer_deadline,
                transaction_clients: shared.transaction_clients.clone(),
                http: shared.http.clone(),
                challenge_key: shared.challenge_key.clone(),
                accounts: Mutex::new(HashMap::new()),
            },
        };
        change(&mut inner);
        Self {
            inner: Arc::new(inner),
        }
    }

    /// The path this endpoint serves under (`/jazz`).
    pub fn path(&self) -> &str {
        &self.inner.base_path
    }

    /// Whether `path` is this endpoint's sync socket (`<base>/apps/<id>/ws`):
    /// a host upgrades such a request and passes the socket to
    /// [`Endpoint::serve_socket`].
    pub fn is_socket(&self, path: &str) -> bool {
        let request = Request::new("GET", path);
        matches!(self.route(&request), Some(Route::Socket))
    }

    /// Answers an HTTP request to the endpoint: the account routes, and 404
    /// for any path it does not serve. A socket's path answers 426: the host
    /// upgrades it and calls [`Endpoint::serve_socket`].
    pub async fn handle(&self, request: Request) -> Response {
        let Some(route) = self.route(&request) else {
            return Response::text(404, "Not found");
        };
        if request.body.len() > MAX_BODY_BYTES {
            return Response::text(413, "request body too large");
        }
        let result = match route {
            Route::SignIn => self.post(&request, Self::backend_sign_in).await,
            Route::User => self.post(&request, Self::backend_user).await,
            Route::LinkChallenge => self.post(&request, Self::backend_link_challenge).await,
            Route::Link => self.post(&request, Self::backend_link).await,
            Route::Socket => return Response::text(426, "upgrade to a WebSocket"),
            Route::Public(path) => return self.public(&request, &path).await,
            Route::NotYet(what) => {
                return Response::text(501, &format!("not supported yet: {what}"));
            }
        };
        result.unwrap_or_else(|error| match error {
            Error::Config(_) => Response::text(500, "authenticate_failed"),
            _ => Response::text(503, "account_registry_unavailable"),
        })
    }

    async fn post<'a, F, Fut>(&'a self, request: &'a Request, route: F) -> Result<Response>
    where
        F: FnOnce(&'a Self, &'a Request) -> Fut,
        Fut: Future<Output = Result<Response>> + 'a,
    {
        if request.method != "POST" {
            return Ok(Response::text(405, "method not allowed"));
        }
        route(self, request).await
    }

    fn route(&self, request: &Request) -> Option<Route> {
        let path = request.path_only();
        let inner = path.strip_prefix(self.inner.base_path.as_str())?;
        let rest = inner.strip_prefix("/apps/")?;
        // Whatever app id the client names (a TS client derives one from
        // its endpoint URL), the endpoint serves its own, as `createJazz`
        // does: every route acts on the backend's app.
        let (app, rest) = rest.split_once('/')?;
        if app.is_empty() {
            return None;
        }
        Some(match rest {
            "accounts/backend-sign-in" => Route::SignIn,
            "accounts/backend-user" => Route::User,
            "ws" => Route::Socket,
            "schemas" => Route::Public(rest.to_owned()),
            _ if rest.starts_with("schema/") && !rest["schema/".len()..].contains('/') => {
                Route::Public(rest.to_owned())
            }
            "accounts/backend-link" => Route::Link,
            "accounts/backend-link/challenge" => Route::LinkChallenge,
            "backend/admit" | "backend/user-session" => {
                Route::NotYet("trusted backend clients (backendClients)")
            }
            _ if rest == "stream" || rest.starts_with("stream/") => {
                Route::NotYet("the HTTP-stream transport; connect over the WebSocket")
            }
            _ => return None,
        })
    }

    /// The user `request` is for, as the app's `authenticate` says. A
    /// device-account answer names no user.
    ///
    /// An answer whose `expires_at` has passed names no one, here and on
    /// every route: the user is signed out.
    pub async fn authenticate(&self, request: &Request) -> Result<Option<AuthUser>> {
        match self.ask(request.clone()).await? {
            Some(Answer::User(user)) if user.expires_at > unix_now() => Ok(Some(user)),
            Some(Answer::User(_) | Answer::DeviceAccount) | None => Ok(None),
        }
    }

    async fn ask(&self, request: Request) -> Result<Option<Answer>> {
        let answer = self
            .inner
            .authenticate
            .authenticate(request)
            .await
            .map_err(|error| Error::Config(format!("authenticate failed: {error}")))?;
        if let Some(Answer::User(user)) = &answer
            && user.user_id.is_empty()
        {
            return Err(Error::Config(
                "authenticate named a user with an empty user_id".into(),
            ));
        }
        Ok(answer)
    }

    /// Whether the app lets a device account make `request`: its
    /// `authenticate` answers [`Answer::DeviceAccount`] for it, asked
    /// without the request's cookies and `Authorization`.
    async fn accepts_device_account(&self, request: &Request) -> Result<bool> {
        Ok(matches!(
            self.ask(request.without_credentials()).await?,
            Some(Answer::DeviceAccount)
        ))
    }

    /// The account sign-in a client makes with only the app's own session:
    /// `authenticate` names the user, and the core resolves (or creates)
    /// their account.
    async fn backend_sign_in(&self, request: &Request) -> Result<Response> {
        let Some(user) = self.authenticate(request).await? else {
            return Ok(Response::text(401, "signed_out"));
        };
        let account = self.account_of(&user.user_id).await?;
        Ok(Response::json(&json!({
            "account": account.0.to_string(),
            "identity": identity(&user.user_id),
            "claims": user.claims,
            "expiresAt": unix_now() + self.inner.session_ttl.as_secs(),
        })))
    }

    /// Who the request's user is, without changing anything. No one signed
    /// in is `{ "user": null }`, not a 401.
    async fn backend_user(&self, request: &Request) -> Result<Response> {
        let Some(user) = self.authenticate(request).await? else {
            return Ok(Response::json(&json!({ "user": null })));
        };
        let account = match self.existing_account_of(&user.user_id).await {
            Ok(account) => account,
            Err(Refused::Status(403)) => {
                return Ok(Response::text(403, "identity_not_authorized"));
            }
            Err(error) => return Err(error.into()),
        };
        Ok(Response::json(&json!({
            "account": account.map(|account| account.0.to_string()),
            "identity": identity(&user.user_id),
            "claims": user.claims,
        })))
    }

    /// A challenge for a device account to sign before it links to the
    /// signed-in user (TS `backendLinkChallenge`).
    async fn backend_link_challenge(&self, request: &Request) -> Result<Response> {
        let Some(user) = self.authenticate(request).await? else {
            return Ok(Response::text(401, "signed_out"));
        };
        let expires_at = unix_now() + LINK_CHALLENGE_TTL.as_secs();
        let nonce = jazz::tools::ObjectId::new().to_string();
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(
            &json!({ "v": 1, "sub": user.user_id, "nonce": nonce, "exp": expires_at }),
        )?);
        let mac = self.challenge_mac(&payload);
        Ok(Response::json(&json!({
            "challenge": format!("{payload}.{mac}"),
            "expiresAt": expires_at,
        })))
    }

    fn challenge_mac(&self, payload: &str) -> String {
        use hmac::Mac as _;
        let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(&self.inner.challenge_key)
            .expect("HMAC takes a key of any length");
        mac.update(payload.as_bytes());
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    }

    /// The nonce of a challenge this endpoint issued for `user_id`, still
    /// fresh.
    fn read_link_challenge(&self, challenge: &str, user_id: &str) -> Option<String> {
        use hmac::Mac as _;
        if challenge.len() > 1024 {
            return None;
        }
        let (payload, mac) = challenge.split_once('.')?;
        if mac.contains('.') {
            return None;
        }
        let given = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(mac)
            .ok()?;
        let mut expected = hmac::Hmac::<sha2::Sha256>::new_from_slice(&self.inner.challenge_key)
            .expect("HMAC takes a key of any length");
        expected.update(payload.as_bytes());
        expected.verify_slice(&given).ok()?;
        let claims: Value = serde_json::from_slice(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(payload)
                .ok()?,
        )
        .ok()?;
        (claims["v"] == json!(1)
            && claims["sub"].as_str() == Some(user_id)
            && claims["exp"].as_u64().is_some_and(|exp| exp > unix_now()))
        .then(|| claims["nonce"].as_str().map(str::to_owned))
        .flatten()
    }

    /// Links a device account to the user `authenticate` now names, so the
    /// account and its data carry over to them (TS `backendLink`). The
    /// device proves its key by signing a challenge issued for this user,
    /// never with its sync token, so a proof links once and only to them.
    async fn backend_link(&self, request: &Request) -> Result<Response> {
        let Some(user) = self.authenticate(request).await? else {
            return Ok(Response::text(401, "signed_out"));
        };
        let Ok(body) = serde_json::from_slice::<Value>(&request.body) else {
            return Ok(Response::text(400, "local_first_proof_required"));
        };
        let Some(challenge) = body["challenge"].as_str() else {
            return Ok(Response::text(400, "invalid_link_challenge"));
        };
        let Some(nonce) = self.read_link_challenge(challenge, &user.user_id) else {
            return Ok(Response::text(400, "invalid_link_challenge"));
        };
        let proof = body["proof"].as_str().unwrap_or_default();
        let Ok(subject) = jazz::tools::identity::verify_device_account_challenge(proof, challenge)
        else {
            return Ok(Response::text(401, "invalid_local_first_proof"));
        };
        // The device's account follows from its key and the app it was
        // founded in. Accounts founded for an endpoint URL are not
        // supported yet.
        let app = self.inner.core.app_id;
        match &body["audience"] {
            Value::Null => {}
            Value::String(audience) if *audience == app.to_string() => {}
            _ => return Ok(Response::text(401, "invalid_local_first_proof")),
        }
        let subject = subject.to_string();
        let account = jazz::account_registry::device_account_id(*app.uuid(), &subject);
        let guest = json!({ "issuer": DEVICE_ACCOUNT_ISSUER, "subject": subject });
        let user_is = async |expected: AccountId| matches!(self.accounts("resolve", &user.user_id).await, Ok(found) if found == expected);
        let linked = async {
            match self
                .admin("found-local-first", json!({ "identity": guest }))
                .await
            {
                Ok(_) => {}
                // A device key an earlier link retired: only that link's own
                // user gets an answer (the retry of a link whose response
                // was lost).
                Err(Refused::Coded(403, code))
                    if code != DEVICE_ACCOUNTS_DISABLED && user_is(account).await =>
                {
                    return Ok(true);
                }
                Err(error) => return Err(error),
            }
            match self
                .admin(
                    "link",
                    json!({ "from": guest, "to": identity(&user.user_id), "nonce": nonce }),
                )
                .await
            {
                Ok(_) => Ok(false),
                // Linking is idempotent for this device and this user: a
                // 403 means the link that won already retired the key.
                Err(Refused::Coded(status @ (409 | 403), _)) if user_is(account).await => {
                    Ok(status == 403)
                }
                Err(error) => Err(error),
            }
        };
        let revoked = match linked.await {
            Ok(revoked) => revoked,
            Err(Refused::Coded(_, code)) if code == DEVICE_ACCOUNTS_DISABLED => {
                return Ok(Response::text(503, DEVICE_ACCOUNTS_DISABLED));
            }
            // The core's own refusals refuse the link for good.
            Err(Refused::Coded(400 | 403 | 404 | 409, _)) => {
                return Ok(Response::text(409, "link_refused"));
            }
            Err(_) => return Ok(Response::text(503, "account_registry_unavailable")),
        };
        if self.inner.guest_key_after_link == GuestKeyAfterLink::Revoke && !revoked {
            // The link already stands. If the revoke fails, the key stays
            // usable until the device retries the link (idempotent, and it
            // revokes then): a server error, not a silent success.
            let revoke = self
                .admin(
                    "revoke",
                    json!({ "approver": identity(&user.user_id), "target": guest }),
                )
                .await;
            if revoke.is_err() {
                return Ok(Response::text(500, "guest_key_revoke_failed"));
            }
        }
        self.remember(&user.user_id, account);
        Ok(Response::json(&json!({
            "account": account.0.to_string(),
            "identity": identity(&user.user_id),
            "claims": user.claims,
            "expiresAt": unix_now() + self.inner.session_ttl.as_secs(),
        })))
    }

    /// The core's public schema routes, which clients read unauthenticated.
    async fn public(&self, request: &Request, path: &str) -> Response {
        if request.method != "GET" && request.method != "HEAD" {
            return Response::text(405, "method not allowed");
        }
        let url = format!(
            "{}/apps/{}/{path}",
            self.inner.core.server.trim_end_matches('/'),
            self.inner.core.app_id
        );
        let mut forwarded = self.inner.http.get(url);
        for name in ["accept", "if-none-match"] {
            if let Some(value) = request.header_value(name) {
                forwarded = forwarded.header(name, value);
            }
        }
        let Ok(response) = forwarded.send().await else {
            return Response::text(502, "the sync server is unreachable");
        };
        let status = response.status().as_u16();
        let headers = ["content-type", "etag", "cache-control"]
            .into_iter()
            .filter_map(|name| {
                let value = response.headers().get(name)?.to_str().ok()?;
                Some((name.to_owned(), value.to_owned()))
            })
            .collect();
        let body = response
            .bytes()
            .await
            .map(|b| b.to_vec())
            .unwrap_or_default();
        Response {
            status,
            headers,
            body,
        }
    }

    /// The Jazz account of the request's signed-in user, or `None` when
    /// `authenticate` names no one or the user has no account yet. It never
    /// creates one (see [`Endpoint::create_account_for`]).
    pub async fn account_for(&self, request: &Request) -> Result<Option<AccountId>> {
        let Some(user) = self.authenticate(request).await? else {
            return Ok(None);
        };
        Ok(self.existing_account_of(&user.user_id).await?)
    }

    /// The Jazz account of `user_id` (the id `authenticate` gives them),
    /// created if they have none: for a user who never opens a client.
    pub async fn create_account_for(&self, user_id: &str) -> Result<AccountId> {
        if user_id.is_empty() {
            return Err(Error::Config(
                "create_account_for: pass the user's id, as authenticate names them".into(),
            ));
        }
        self.account_of(user_id).await
    }

    /// The backend's handle acting as the request's user (TS `forReqUser`):
    /// its reads and writes run as them, under their permissions.
    /// [`Error::SignedOut`] when `authenticate` names no one, and
    /// [`Error::NoAccount`] when the user has no account yet.
    pub async fn for_user(&self, request: &Request) -> Result<Jazz> {
        let user = self.authenticate(request).await?.ok_or(Error::SignedOut)?;
        self.for_auth_user(&user).await
    }

    /// The backend's handle acting as `user` (TS `forSession`), on the
    /// account they already have.
    ///
    /// Users' handles share one client of the endpoint's, opened on first
    /// use beside the backend's own: its link to the core is trusted to act
    /// for the sessions its calls name, as `createJazz`'s backend session
    /// is, and it keeps its data in memory. Byte-range reads, streaming
    /// inserts and permission advice are not supported on a user's handle
    /// yet (`jazz` refuses them for an explicit session).
    pub async fn for_auth_user(&self, user: &AuthUser) -> Result<Jazz> {
        let session = self.session_for(user).await?;
        let users = self
            .inner
            .users
            .get_or_try_init(|| self.inner.jazz.open_for_users())
            .await?;
        Ok(users.scoped(session))
    }

    /// Stops the client users' handles share, if it was opened; the
    /// backend's own handle runs on. Users' handles fail with
    /// [`Error::Stopped`] afterwards.
    pub async fn shutdown(&self) -> Result<()> {
        match self.inner.users.get() {
            Some(users) => users.shutdown().await,
            None => Ok(()),
        }
    }

    async fn session_for(&self, user: &AuthUser) -> Result<Session> {
        let account = self
            .existing_account_of(&user.user_id)
            .await?
            .ok_or_else(|| Error::NoAccount(user.user_id.clone()))?;
        let mut session = Session::new(BACKEND_ISSUER, user.user_id.clone())
            .with_claims(Value::Object(user.claims.clone()));
        session.account_id = Some(account);
        Ok(session)
    }

    fn cached_account(&self, user_id: &str) -> Option<AccountId> {
        let accounts = self
            .inner
            .accounts
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        accounts
            .get(user_id)
            .filter(|(_, until)| *until > Instant::now())
            .map(|(account, _)| *account)
    }

    fn remember(&self, user_id: &str, account: AccountId) -> AccountId {
        let mut accounts = self
            .inner
            .accounts
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if accounts.len() >= ACCOUNT_CACHE_LIMIT {
            accounts.retain(|_, (_, until)| *until > Instant::now());
            if accounts.len() >= ACCOUNT_CACHE_LIMIT {
                accounts.clear();
            }
        }
        accounts.insert(
            user_id.to_owned(),
            (account, Instant::now() + self.inner.session_ttl),
        );
        account
    }

    /// The user's account, or `None` while they have none.
    async fn existing_account_of(
        &self,
        user_id: &str,
    ) -> std::result::Result<Option<AccountId>, Refused> {
        if let Some(account) = self.cached_account(user_id) {
            return Ok(Some(account));
        }
        match self.accounts("resolve", user_id).await {
            Ok(account) => Ok(Some(self.remember(user_id, account))),
            Err(Refused::NotAssigned) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// The user's account, created if they have none.
    async fn account_of(&self, user_id: &str) -> Result<AccountId> {
        if let Some(account) = self.cached_account(user_id) {
            return Ok(account);
        }
        let account = self.accounts("login-or-register", user_id).await?;
        Ok(self.remember(user_id, account))
    }

    /// One admin account request to the core: the backend vouches for the
    /// user's identity.
    async fn accounts(
        &self,
        operation: &str,
        user_id: &str,
    ) -> std::result::Result<AccountId, Refused> {
        let body = match self
            .admin(operation, json!({ "identity": identity(user_id) }))
            .await
        {
            Ok(body) => body,
            Err(Refused::Coded(404, code)) if code == "identity_not_assigned" => {
                return Err(Refused::NotAssigned);
            }
            Err(Refused::Coded(status, _)) => return Err(Refused::Status(status)),
            Err(error) => return Err(error),
        };
        #[derive(Deserialize)]
        struct Assigned {
            account: AccountId,
        }
        let assigned: Assigned =
            serde_json::from_str(&body).map_err(|error| Refused::Unreachable(error.to_string()))?;
        if assigned.account.is_system() {
            return Err(Refused::Unreachable(
                "the core assigned the system account".into(),
            ));
        }
        Ok(assigned.account)
    }

    /// One admin account request to the core (`/admin/accounts/<operation>`),
    /// vouched for with the admin secret: its body, or the core's status and
    /// refusal code.
    async fn admin(&self, operation: &str, body: Value) -> std::result::Result<String, Refused> {
        let core = &self.inner.core;
        let response = self
            .inner
            .http
            .post(format!(
                "{}/apps/{}/admin/accounts/{operation}",
                core.server.trim_end_matches('/'),
                core.app_id
            ))
            .header("X-Jazz-Admin-Secret", &core.admin_secret)
            .json(&body)
            .send()
            .await
            .map_err(|error| Refused::Unreachable(error.to_string()))?;
        let status = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        if status / 100 != 2 {
            return Err(Refused::Coded(status, refusal_code(&body)));
        }
        Ok(body)
    }

    /// Serves an upgraded sync socket (`<base>/apps/<id>/ws`): `request` is
    /// the upgrade request (its cookies or `Authorization` are what
    /// `authenticate` reads), `incoming` the client's messages, binary and
    /// text alike, as bytes (TS clients send their prelude as text; it
    /// ends when the client goes away), `outgoing` where the client's
    /// messages go. Returns how the socket ended, for the host to close it
    /// with.
    ///
    /// The client's first message, its prelude, is admitted as TS
    /// `admitPrelude` does: a device account's own proof, if `authenticate`
    /// lets device accounts in; or the session of the user `authenticate`
    /// names, which the client must have stated. It is rewritten to the
    /// backend's word (its secret, the user's account and claims) and pinned
    /// to the bundle the backend published, then the socket is relayed to
    /// the core. Shortly before the session's expiry the client is asked to
    /// reconnect, and `authenticate` decides again.
    pub async fn serve_socket<I, O>(&self, request: Request, incoming: I, outgoing: O) -> Closed
    where
        I: Stream<Item = Vec<u8>> + Unpin + Send,
        O: Sink<Vec<u8>> + Unpin + Send,
    {
        let mut incoming = incoming;
        let mut outgoing = outgoing;
        let closed = self
            .serve_socket_inner(&request, &mut incoming, &mut outgoing)
            .await;
        let _ = outgoing.close().await;
        closed
    }

    async fn serve_socket_inner<I, O>(
        &self,
        request: &Request,
        incoming: &mut I,
        outgoing: &mut O,
    ) -> Closed
    where
        I: Stream<Item = Vec<u8>> + Unpin + Send,
        O: Sink<Vec<u8>> + Unpin + Send,
    {
        if !matches!(self.route(request), Some(Route::Socket)) {
            return Closed::policy("not this endpoint's socket");
        }
        let _slot = match SocketSlot::take(&self.inner.sockets, self.inner.max_sockets) {
            Some(slot) => slot,
            None => {
                return Closed {
                    code: 1013,
                    reason: "too many sockets; try again later".into(),
                };
            }
        };
        let prelude = match tokio::time::timeout(PRELUDE_DEADLINE, incoming.next()).await {
            Ok(Some(prelude)) => prelude,
            Ok(None) => return Closed::normal(),
            Err(_) => return Closed::policy("no prelude"),
        };
        let admission = match self.admit(request, &prelude).await {
            Ok(admission) => admission,
            Err(_) => {
                let _ = outgoing
                    .send(error_batch(
                        WireRetry::Never,
                        "session unavailable: the backend could not admit it".to_owned(),
                        None,
                        0,
                    ))
                    .await;
                return Closed {
                    code: 1011,
                    reason: "could not admit".into(),
                };
            }
        };
        let (prelude, until, auth) = match admission {
            Admission::Admitted {
                prelude,
                until,
                auth,
                ..
            } => (prelude, until, auth),
            Admission::Unverified => {
                let features = refused_client_features(incoming).await;
                let _ = outgoing
                    .send(auth_failed(
                        "session invalid: this backend did not accept the credential the client sent",
                        WireErrorReason::Invalid,
                        features,
                    ))
                    .await;
                return Closed::policy("session invalid");
            }
            Admission::Missing => {
                let features = refused_client_features(incoming).await;
                let _ = outgoing
                    .send(auth_failed(
                        "session missing: not signed in",
                        WireErrorReason::Missing,
                        features,
                    ))
                    .await;
                return Closed::policy("session missing");
            }
            Admission::Expired => {
                let features = refused_client_features(incoming).await;
                let _ = outgoing
                    .send(auth_failed(
                        "session expired: authenticate returned an expiresAt in the past",
                        WireErrorReason::Expired,
                        features,
                    ))
                    .await;
                return Closed::policy("session expired");
            }
        };
        let core = &self.inner.core;
        let url = format!(
            "{}/apps/{}/ws",
            core.server.trim_end_matches('/').replacen("http", "ws", 1),
            core.app_id
        );
        let Ok(Ok((socket, _))) =
            tokio::time::timeout(CORE_CONNECT_TIMEOUT, tokio_tungstenite::connect_async(url)).await
        else {
            return Closed {
                code: 1011,
                reason: "the sync server is unreachable".into(),
            };
        };
        use tokio_tungstenite::tungstenite::Message;
        let (mut to_core, mut from_core) = socket.split();
        if to_core.send(Message::Binary(prelude.into())).await.is_err() {
            return Closed {
                code: 1011,
                reason: "the sync server is unreachable".into(),
            };
        }
        let expiry = tokio::time::sleep_until(until.into());
        tokio::pin!(expiry);
        let target = thin::SessionTarget {
            server: core.server.clone(),
            app_id: core.app_id,
            backend_secret: core.backend_secret.clone(),
            bundle: self.inner.bundle.clone(),
            auth,
        };
        // A thin client (its Hello asks for thin results) has its
        // transaction requests served here, not by the core (SPEC 8.14).
        let mut hello_seen = false;
        // What the client's Hello offered: a refusal names its reason to a
        // client that reads one (`FEATURE_TYPED_ERROR_REASON`).
        let mut client_features = 0;
        let mut thin: Option<ThinSocket> = None;
        let (answers, mut answered) = tokio::sync::mpsc::unbounded_channel();
        // Since when the thin socket's requests wait on an answer: one that
        // never comes (the core dropped it) must not hold them forever.
        let mut waiting_since: Option<tokio::time::Instant> = None;
        let clients = (
            &self.inner.transaction_clients,
            self.inner.max_transaction_clients,
        );
        let closed = loop {
            // (At the top: arms that `continue` pass through here too.)
            waiting_since = match &thin {
                Some(thin) if thin.router.is_waiting() => {
                    Some(waiting_since.unwrap_or_else(tokio::time::Instant::now))
                }
                _ => None,
            };
            let waited_out = waiting_since.map(|since| since + self.inner.answer_deadline);
            tokio::select! {
                () = async {
                    match waited_out {
                        Some(at) => tokio::time::sleep_until(at).await,
                        None => std::future::pending().await,
                    }
                } => {
                    break Closed { code: 1011, reason: "an answer never came".into() };
                },
                message = incoming.next() => match message {
                    // More than the core takes in one message: it would
                    // close the link, so the endpoint does, for this socket.
                    Some(message) if message.len() > MAX_CLIENT_MESSAGE_BYTES => {
                        break Closed { code: 1009, reason: "message too big".into() };
                    }
                    Some(message) => {
                        if !hello_seen {
                            hello_seen = true;
                            client_features = hello_features(&message);
                            if asks_thin_results(&message) {
                                thin = Some(ThinSocket::default());
                            }
                        } else if let Some(thin) = &mut thin {
                            if !thin.take_client_message(&message) {
                                break Closed::policy("undecodable thin frame");
                            }
                            if !send_all(&mut to_core, thin.release(&target, clients, &answers)).await {
                                break Closed { code: 1011, reason: "the sync server went away".into() };
                            }
                            continue;
                        }
                        if to_core.send(Message::Binary(message.into())).await.is_err() {
                            break Closed { code: 1011, reason: "the sync server went away".into() };
                        }
                    }
                    None => break Closed::normal(),
                },
                Some(answer) = answered.recv() => {
                    if let Some(thin) = &mut thin {
                        thin.router.answered(&answer);
                        waiting_since = None;
                    }
                    let mut sent = true;
                    for frame in thin::answer_frames(answer) {
                        let Ok(message) = encode_websocket_frame_batch(&[frame]) else {
                            continue;
                        };
                        if outgoing.send(message).await.is_err() {
                            sent = false;
                            break;
                        }
                    }
                    if !sent {
                        break Closed::normal();
                    }
                    // The backend's answer may free frames waiting on it.
                    if let Some(thin) = &mut thin
                        && !send_all(&mut to_core, thin.release(&target, clients, &answers)).await
                    {
                        break Closed { code: 1011, reason: "the sync server went away".into() };
                    }
                },
                message = from_core.next() => match message {
                    Some(Ok(Message::Binary(data))) => {
                        if let Some(thin) = &mut thin
                            && thin.note_core_message(&data)
                        {
                            // An answer: the wait goes on from here.
                            waiting_since = None;
                        }
                        if outgoing.send(data.to_vec()).await.is_err() {
                            break Closed::normal();
                        }
                        // The core's answers may free requests waiting on them.
                        if let Some(thin) = &mut thin
                            && !send_all(&mut to_core, thin.release(&target, clients, &answers)).await
                        {
                            break Closed { code: 1011, reason: "the sync server went away".into() };
                        }
                    }
                    Some(Ok(Message::Close(frame))) => {
                        break frame.map_or_else(Closed::normal, |frame| Closed {
                            code: frame.code.into(),
                            reason: frame.reason.to_string(),
                        });
                    }
                    Some(Ok(_)) => {}
                    Some(Err(_)) | None => {
                        break Closed { code: 1011, reason: "the sync server went away".into() };
                    }
                },
                () = &mut expiry => {
                    // A planned reconnect: the client connects again, and
                    // `authenticate` decides on its new request.
                    let _ = outgoing.send(reauthenticate(client_features)).await;
                    break Closed::policy("reauthenticate");
                }
            }
        };
        let _ = to_core.close().await;
        closed
    }

    /// `admitPrelude`: a device account's own proof, or the session of the
    /// user `authenticate` names, rewritten to the backend's word and pinned
    /// to its bundle.
    ///
    /// What reaches the core is built here, never copied from the client:
    /// `peer_identity` and `requested_link` from the prelude, the pinned
    /// `bundle`, and an `auth` holding the backend secret and exactly one
    /// identity (the device's token, or the vouched session). Credentials
    /// are read from `auth` only: one stated beside it that differs (TS
    /// clients repeat theirs there), a secret, or a request only a backend
    /// makes (`bootstrap_catalogue`, `local_first_audience`) refuses the
    /// socket. Every other field is dropped.
    async fn admit(&self, request: &Request, prelude: &[u8]) -> Result<Admission> {
        if prelude.len() > MAX_PRELUDE_BYTES {
            return Ok(Admission::Unverified);
        }
        let Ok(Value::Object(parsed)) = serde_json::from_slice::<Value>(prelude) else {
            return Ok(Admission::Missing);
        };
        let Some(Value::Object(auth)) = parsed.get("auth") else {
            return Ok(Admission::Missing);
        };
        let mut forwarded = Map::new();
        for (key, value) in &parsed {
            match key.as_str() {
                // The core checks it against the session it admits.
                "peer_identity" | "requested_link" if value.is_string() => {
                    forwarded.insert(key.clone(), value.clone());
                }
                "peer_identity" | "requested_link" => return Ok(Admission::Unverified),
                // A TS client repeats its credentials beside `auth`: they
                // must be the same (or null), and only `auth`'s are read.
                "jwt_token" | "backend_secret" | "admin_secret" | "backend_session"
                    if !value.is_null() && auth.get(key) != Some(value) =>
                {
                    return Ok(Admission::Unverified);
                }
                // Requests only a backend makes.
                "bootstrap_catalogue" if value != &json!(false) => {
                    return Ok(Admission::Unverified);
                }
                "local_first_audience" if !value.is_null() => return Ok(Admission::Unverified),
                // Anything else (the client's `bundle`, `sub`, the renewal
                // field) never reaches the core.
                _ => {}
            }
        }
        let stated = |key: &str| auth.get(key).is_some_and(|value| !value.is_null());
        if stated("backend_secret") || stated("admin_secret") {
            return Ok(Admission::Unverified);
        }
        let secret = Value::String(self.inner.core.backend_secret.clone());
        let now = Instant::now();
        if stated("jwt_token") {
            if stated("backend_session") {
                return Ok(Admission::Unverified);
            }
            let Some(token) = auth.get("jwt_token").and_then(Value::as_str) else {
                return Ok(Admission::Unverified);
            };
            let claims = token_claims(token);
            let local_first = claims
                .as_ref()
                .is_some_and(|claims| claims.get("iss") == Some(&json!(DEVICE_ACCOUNT_ISSUER)));
            // The core verifies the token itself; the backend only checks
            // it is for this app. Proofs for an endpoint URL are not
            // supported yet.
            let app = self.inner.core.app_id.to_string();
            let for_this_app = claims
                .as_ref()
                .and_then(|claims| claims.get("aud"))
                .is_some_and(|aud| match aud {
                    Value::String(aud) => *aud == app,
                    Value::Array(auds) => auds.iter().any(|aud| aud.as_str() == Some(&app)),
                    _ => false,
                });
            if !local_first || !for_this_app || !self.accepts_device_account(request).await? {
                return Ok(Admission::Unverified);
            }
            forwarded.insert(
                "auth".into(),
                json!({ "jwt_token": token, "backend_secret": secret }),
            );
            return Ok(Admission::Admitted {
                prelude: self.pinned(forwarded),
                who: Admitted::DeviceAccount,
                auth: thin::SessionAuth::DeviceAccount(token.to_owned()),
                until: now + self.inner.session_ttl,
            });
        }
        let Some(Value::Object(stated_session)) = auth.get("backend_session") else {
            return Ok(Admission::Missing);
        };
        let Some(user) = self.authenticate(request).await? else {
            return Ok(Admission::Missing);
        };
        if stated_session.get("user_id").and_then(Value::as_str) != Some(user.user_id.as_str()) {
            return Ok(Admission::Missing);
        }
        // A client states a user's session only once its sign-in gave them
        // an account: this never makes one.
        let session = match self.session_for(&user).await {
            Ok(session) => session,
            Err(Error::NoAccount(_)) => return Ok(Admission::Missing),
            Err(error) => return Err(error),
        };
        let Some(until) = reauthenticate_at(now, user.expires_at, self.inner.session_ttl) else {
            return Ok(Admission::Expired);
        };
        let auth = thin::SessionAuth::User(session.clone());
        // As TS `sessionFor` states it to the core.
        let session = json!({
            "account_id": session.account_id.map(|account| account.0.to_string()),
            "issuer": session.issuer,
            "user_id": session.user_id,
            "claims": session.claims,
            "authMode": "external",
        });
        forwarded.insert(
            "auth".into(),
            json!({ "backend_secret": secret, "backend_session": session }),
        );
        Ok(Admission::Admitted {
            prelude: self.pinned(forwarded),
            who: Admitted::User(user.user_id),
            auth,
            until,
        })
    }

    /// `pinnedSocket`: the prelude with the bundle this backend published.
    /// The core honours the pin only on the backend's word, so whatever
    /// bundle the client stated is replaced.
    fn pinned(&self, mut prelude: Map<String, Value>) -> Vec<u8> {
        prelude.insert("bundle".into(), Value::String(self.inner.bundle.clone()));
        serde_json::to_vec(&prelude).unwrap_or_default()
    }

    /// The prelude the core would get for this socket, if admitted: for
    /// tests.
    #[doc(hidden)]
    pub async fn forwarded_prelude(
        &self,
        request: &Request,
        prelude: &[u8],
    ) -> Result<Option<Value>> {
        Ok(match self.admit(request, prelude).await? {
            Admission::Admitted { prelude, .. } => serde_json::from_slice(&prelude).ok(),
            _ => None,
        })
    }

    /// Who a socket would be admitted as, without opening it: for tests and
    /// the host's logs.
    #[doc(hidden)]
    pub async fn admits(&self, request: &Request, prelude: &[u8]) -> Result<Option<Admitted>> {
        Ok(match self.admit(request, prelude).await? {
            Admission::Admitted { who, .. } => Some(who),
            _ => None,
        })
    }
}

/// One of an endpoint's socket slots, given back when dropped.
struct SocketSlot(Arc<std::sync::atomic::AtomicUsize>);

impl SocketSlot {
    fn take(open: &Arc<std::sync::atomic::AtomicUsize>, max: usize) -> Option<Self> {
        use std::sync::atomic::Ordering;
        open.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
            (n < max).then_some(n + 1)
        })
        .ok()
        .map(|_| Self(open.clone()))
    }
}

impl Drop for SocketSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

enum Route {
    SignIn,
    User,
    LinkChallenge,
    Link,
    Socket,
    Public(String),
    NotYet(&'static str),
}

/// A refused admin account request.
#[derive(Debug)]
enum Refused {
    NotAssigned,
    Status(u16),
    /// The core's status and refusal code.
    Coded(u16, String),
    Unreachable(String),
}

impl From<Refused> for Error {
    fn from(refused: Refused) -> Self {
        match refused {
            Refused::NotAssigned => Error::Server("the user has no account".into()),
            Refused::Status(status) | Refused::Coded(status, _) => {
                Error::Server(format!("the account registry refused ({status})"))
            }
            Refused::Unreachable(error) => {
                Error::Server(format!("the account registry is unavailable: {error}"))
            }
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Error::Server(error.to_string())
    }
}

/// The code of a core's refusal: its body, or the `error`/`code` of a JSON
/// body.
fn refusal_code(body: &str) -> String {
    match serde_json::from_str::<Value>(body) {
        Ok(Value::Object(body)) => ["code", "error"]
            .iter()
            .find_map(|key| body.get(*key).and_then(Value::as_str))
            .unwrap_or_default()
            .to_owned(),
        _ => body.trim().to_owned(),
    }
}

fn identity(user_id: &str) -> Value {
    json!({ "issuer": BACKEND_ISSUER, "subject": user_id })
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// A JWT's claims, unverified: the core verifies it.
fn token_claims(token: &str) -> Option<Map<String, Value>> {
    let payload = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// When a session admitted `now`, good until `expires_at` (Unix seconds)
/// and at most `ttl`, is asked to reconnect; `None` when it has expired
/// already. As TS `reauthenticateAt(sessionDeadline(…))`.
fn reauthenticate_at(now: Instant, expires_at: u64, ttl: Duration) -> Option<Instant> {
    let left = Duration::from_secs(expires_at.saturating_sub(unix_now()));
    let remaining = left.min(ttl);
    if remaining.is_zero() {
        return None;
    }
    let lead = REAUTHENTICATE_LEAD.min(remaining / 2);
    Some(now + (remaining - lead).max(MIN_SESSION.min(remaining)))
}

/// An `auth_failed` error, naming `reason` to a client whose Hello offered
/// `client_features` with `FEATURE_TYPED_ERROR_REASON`
/// (`wire-error-reason-v1`); the plain `Error` every client reads otherwise.
fn error_batch(
    retry: WireRetry,
    message: String,
    reason: Option<WireErrorReason>,
    client_features: u64,
) -> Vec<u8> {
    let mut error = WireError::new(WireErrorCode::AuthFailed, retry, message);
    error.reason = reason;
    let frame =
        jazz::wire::encode_error_frame_for_client(&error, client_features).unwrap_or_default();
    encode_websocket_frame_batch(&[frame]).unwrap_or_default()
}

/// An auth failure the client does not retry: it asks its backend who is
/// signed in.
fn auth_failed(message: &str, reason: WireErrorReason, client_features: u64) -> Vec<u8> {
    error_batch(
        WireRetry::Never,
        message.to_owned(),
        Some(reason),
        client_features,
    )
}

/// Asks the client to connect again, at its session's expiry.
fn reauthenticate(client_features: u64) -> Vec<u8> {
    error_batch(
        WireRetry::AfterResume,
        format!("{REAUTHENTICATE_PREFIX} the session reached the expiry authenticate gave it"),
        Some(WireErrorReason::Reauthenticate),
        client_features,
    )
}

/// What a client's Hello (its first message after the prelude) offered; 0
/// for anything else.
fn hello_features(message: &[u8]) -> u64 {
    jazz::wire::decode_websocket_frame_batch(message)
        .ok()
        .and_then(
            |frames| match frames.first().map(|frame| jazz::wire::decode_frame(frame)) {
                Some(Ok(WireFrame::Hello(hello))) => Some(hello.features),
                _ => None,
            },
        )
        .unwrap_or(0)
}

/// How long a refusal before the client's Hello waits for it: a client sends
/// it right behind its prelude.
const HELLO_WAIT: Duration = Duration::from_secs(1);

/// The features of the Hello a refused client sends behind its prelude, read
/// before refusing it so the refusal names its reason to a client that reads
/// one. 0 when none comes in time.
async fn refused_client_features<I>(incoming: &mut I) -> u64
where
    I: Stream<Item = Vec<u8>> + Unpin + Send,
{
    match tokio::time::timeout(HELLO_WAIT, incoming.next()).await {
        Ok(Some(message)) => hello_features(&message),
        _ => 0,
    }
}

/// Send `messages` to the core, in order. False once it went away.
async fn send_all<S>(to_core: &mut S, messages: Vec<Vec<u8>>) -> bool
where
    S: Sink<tokio_tungstenite::tungstenite::Message> + Unpin,
{
    for message in messages {
        if to_core
            .send(tokio_tungstenite::tungstenite::Message::Binary(
                message.into(),
            ))
            .await
            .is_err()
        {
            return false;
        }
    }
    true
}

/// Whether a client's Hello (its first message after the prelude) asks
/// for thin results: a thin client, whose transactions the backend serves.
fn asks_thin_results(message: &[u8]) -> bool {
    let Ok(frames) = jazz::wire::decode_websocket_frame_batch(message) else {
        return false;
    };
    frames.first().is_some_and(|frame| {
        matches!(
            jazz::wire::decode_frame(frame),
            Ok(WireFrame::Hello(hello)) if hello.features & jazz::wire::FEATURE_THIN_RESULTS != 0
        )
    })
}

/// A thin client's socket, past its Hello: its frames split between the
/// core and the session's client, in the client's order.
#[derive(Default)]
struct ThinSocket {
    router: thin::ThinRouter,
    /// Started on the session's first transaction request.
    client: Option<thin::SessionClient>,
}

impl ThinSocket {
    /// Take one client message. False when it does not decode.
    fn take_client_message(&mut self, message: &[u8]) -> bool {
        jazz::wire::decode_websocket_frame_batch(message)
            .ok()
            .is_some_and(|frames| self.router.from_client(frames).is_ok())
    }

    /// Note one message the core sent the client: whether it answered a
    /// request the core was sent.
    fn note_core_message(&mut self, message: &[u8]) -> bool {
        jazz::wire::decode_websocket_frame_batch(message)
            .is_ok_and(|frames| self.router.from_core(&frames))
    }

    /// Release what may go now: backend requests to the session's client,
    /// and the messages for the core, returned (a frame each).
    fn release(
        &mut self,
        target: &thin::SessionTarget,
        clients: (&Arc<std::sync::atomic::AtomicUsize>, usize),
        answers: &tokio::sync::mpsc::UnboundedSender<jazz::thin::ThinResponse>,
    ) -> Vec<Vec<u8>> {
        let mut to_core = Vec::new();
        for step in self.router.ready() {
            match step {
                jazz::thin_transactions::Step::ToCore(frames) => {
                    to_core.extend(frames.iter().filter_map(|frame| {
                        encode_websocket_frame_batch(std::slice::from_ref(frame)).ok()
                    }))
                }
                jazz::thin_transactions::Step::ToBackend(request) => {
                    let client = match &self.client {
                        Some(client) => client,
                        None => {
                            let Some(schema) = self.router.schema() else {
                                let _ = answers.send(jazz::thin::ThinResponse {
                                    id: request.id,
                                    body: jazz::thin::ThinBody::Error {
                                        code: "Argument".to_owned(),
                                        message: "bind the connection to a schema first".to_owned(),
                                    },
                                });
                                continue;
                            };
                            let Some(slot) = SocketSlot::take(clients.0, clients.1) else {
                                let _ = answers.send(jazz::thin::ThinResponse {
                                    id: request.id,
                                    body: jazz::thin::ThinBody::Error {
                                        code: "backpressure".to_owned(),
                                        message: "the backend serves as many sessions' transactions as it may; try again later"
                                            .to_owned(),
                                    },
                                });
                                continue;
                            };
                            self.client.insert(thin::SessionClient::start(
                                target.clone(),
                                schema.to_vec(),
                                answers.clone(),
                                Box::new(slot),
                            ))
                        }
                    };
                    client.send(request);
                }
            }
        }
        to_core
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reauthenticates_before_the_expiry_and_never_after_the_ttl() {
        let now = Instant::now();
        let hour = Duration::from_secs(3600);
        let at = reauthenticate_at(now, unix_now() + 7200, hour).unwrap();
        assert_eq!(at - now, hour - REAUTHENTICATE_LEAD);
        let at = reauthenticate_at(now, unix_now() + 20, hour).unwrap();
        assert_eq!(at - now, Duration::from_secs(10));
        assert!(reauthenticate_at(now, unix_now().saturating_sub(1), hour).is_none());
    }

    #[test]
    fn reads_bearers_and_cookies() {
        let request = Request::new("GET", "/")
            .header("Authorization", "Bearer abc")
            .header("Cookie", "a=1; session=xyz");
        assert_eq!(request.bearer(), Some("abc"));
        assert_eq!(request.cookie("session"), Some("xyz"));
        let bare = request.without_credentials();
        assert_eq!(bare.bearer(), None);
        assert_eq!(bare.cookie("session"), None);
    }
}
