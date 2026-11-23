//! A stand-in for an app backend's Jazz endpoint (`createJazz`'s handler
//! at `/jazz`), as far as an end user's client meets it: `backend-sign-in`,
//! and the sync socket, whose prelude it admits and relays to the core the
//! way `admitPrelude` does (`packages/jazz-tools/src/backend/create-jazz.ts`).
//!
//! It also links a device account to a user (`backend-link`, with its
//! challenge). Its `authenticate` reads `Authorization: Bearer token-<user>`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::{SinkExt, StreamExt};
use jazz_app_support::AppId;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

const BACKEND_ISSUER: &str = "jazz:backend";

#[derive(Clone)]
struct Backend {
    core: String,
    app: AppId,
    backend_secret: String,
    admin_secret: String,
    bundle: String,
    /// Link challenges issued, with the user each is for.
    challenges: Arc<Mutex<std::collections::HashMap<String, String>>>,
    links: Arc<Mutex<Links>>,
    events: Events,
}

/// What the relay saw, with the time since it started: for a test to show
/// when it fails ([`Relay::event_log`]).
#[derive(Clone)]
struct Events(Arc<(std::time::Instant, Mutex<Vec<String>>)>);

impl Events {
    fn new() -> Self {
        Self(Arc::new((
            std::time::Instant::now(),
            Mutex::new(Vec::new()),
        )))
    }

    fn log(&self, what: impl std::fmt::Display) {
        let at = self.0.0.elapsed();
        self.0
            .1
            .lock()
            .unwrap()
            .push(format!("{:>9.3?} {what}", at));
    }
}

/// How `backend-link` behaves, beyond the core's own answer.
#[derive(Default)]
pub struct Links {
    /// Users whose links are refused (`409 link_refused`).
    pub refused: std::collections::HashSet<String>,
    /// Users who get an account of their own while their link runs, which
    /// then is refused (`409`): the race of a sign-in elsewhere.
    pub appear: std::collections::HashSet<String>,
    /// Retire the guest's key once linked (`guestKeyAfterLink: "revoke"`).
    pub revoke: bool,
    /// Links attempted so far.
    pub attempts: usize,
    /// Users whose next `backend-sign-in` fails (`503`), once each.
    pub fail_sign_in: std::collections::HashSet<String>,
}

pub struct Relay {
    /// The app's Jazz endpoint, as a client's `Config::server`.
    pub endpoint: String,
    /// Sockets admitted so far, by who they were for.
    pub admitted: Arc<Mutex<Vec<String>>>,
    /// Bearers whose sockets the backend refuses, with what it says (the
    /// client reads `expired`, `revoked` … from it), as `admitPrelude`
    /// answers an unverified credential.
    pub refusing: Arc<Mutex<HashMap<String, String>>>,
    connections: Arc<Mutex<Vec<tokio::task::AbortHandle>>>,
    /// How `backend-link` behaves.
    #[allow(dead_code)]
    pub links: Arc<Mutex<Links>>,
    /// While set, connections are dropped as they arrive: the backend is
    /// unreachable.
    unreachable: Arc<std::sync::atomic::AtomicBool>,
    task: tokio::task::JoinHandle<()>,
    events: Events,
}

impl Relay {
    /// What the relay saw, one line each with the time since it started
    /// (connections accepted, socket preludes passed on or refused, sockets
    /// admitted and ended, kicks), readable from any thread, at any time:
    /// for a test to show when it fails.
    #[allow(dead_code)]
    pub fn event_log(&self) -> impl Fn() -> Vec<String> + Send + 'static {
        let events = self.events.clone();
        move || events.0.1.lock().unwrap().clone()
    }

    /// Drops every open connection, as a backend restart would.
    pub fn kick(&self) {
        self.events.log(format_args!(
            "kick: {} connection(s)",
            self.connections.lock().unwrap().len()
        ));
        for connection in self.connections.lock().unwrap().drain(..) {
            connection.abort();
        }
    }

    /// Makes the backend unreachable (`true`: open connections drop, new
    /// ones are closed at once) or reachable again.
    pub fn set_unreachable(&self, unreachable: bool) {
        self.unreachable
            .store(unreachable, std::sync::atomic::Ordering::SeqCst);
        if unreachable {
            self.kick();
        }
    }

    pub fn refuse(&self, bearer: &str, message: &str) {
        self.refusing
            .lock()
            .unwrap()
            .insert(bearer.to_owned(), message.to_owned());
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub async fn start(
    core: &str,
    app: AppId,
    backend_secret: &str,
    admin_secret: &str,
    bundle: &str,
) -> Relay {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/jazz", listener.local_addr().unwrap());
    let backend = Backend {
        core: core.trim_end_matches('/').to_owned(),
        app,
        backend_secret: backend_secret.to_owned(),
        admin_secret: admin_secret.to_owned(),
        bundle: bundle.to_owned(),
        challenges: Arc::default(),
        links: Arc::default(),
        events: Events::new(),
    };
    let events = backend.events.clone();
    let links = backend.links.clone();
    let admitted = Arc::new(Mutex::new(Vec::new()));
    let refusing = Arc::new(Mutex::new(HashMap::new()));
    let connections = Arc::new(Mutex::new(Vec::new()));
    let unreachable = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (log, refused, open) = (admitted.clone(), refusing.clone(), connections.clone());
    let down = unreachable.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            backend.events.log("accepted a connection");
            if down.load(std::sync::atomic::Ordering::SeqCst) {
                drop(stream);
                continue;
            }
            let connection =
                tokio::spawn(serve(stream, backend.clone(), log.clone(), refused.clone()));
            open.lock().unwrap().push(connection.abort_handle());
        }
    });
    Relay {
        endpoint,
        admitted,
        refusing,
        connections,
        links,
        unreachable,
        task,
        events,
    }
}

/// `backendClients`: a machine credential trusted as the backend
/// (`trustWorkload({ as: "backend" })`), `machine-trusted[.<n>]`.
fn trusted_machine(authorization: Option<&str>) -> bool {
    authorization
        .and_then(|header| header.strip_prefix("Bearer machine-trusted"))
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
}

/// `existingAccountOf`: the account the core holds for `user`, if any.
async fn existing_account_of(backend: &Backend, user: &str) -> Result<Option<String>, u16> {
    let response = reqwest::Client::new()
        .post(format!(
            "{}/apps/{}/admin/accounts/resolve",
            backend.core, backend.app
        ))
        .header("X-Jazz-Backend-Secret", &backend.backend_secret)
        .json(&json!({ "identity": { "issuer": BACKEND_ISSUER, "subject": user } }))
        .send()
        .await
        .map_err(|_| 502u16)?;
    match response.status().as_u16() {
        404 => Ok(None),
        200 => {
            let body: Value = response.json().await.map_err(|_| 502u16)?;
            Ok(body["account"].as_str().map(str::to_owned))
        }
        status => Err(status),
    }
}

/// `authenticate`: the user a bearer names (`token-<user>[.<n>]`).
fn authenticate(authorization: Option<&str>) -> Option<String> {
    let token = authorization?.strip_prefix("Bearer token-")?;
    let user = token.split('.').next()?;
    (!user.is_empty()).then(|| user.to_owned())
}

async fn serve(
    stream: TcpStream,
    backend: Backend,
    admitted: Arc<Mutex<Vec<String>>>,
    refusing: Arc<Mutex<HashMap<String, String>>>,
) {
    let mut head = vec![0; 8192];
    let n = loop {
        let n = stream.peek(&mut head).await.unwrap_or(0);
        if n == 0 {
            return;
        }
        if head[..n].windows(4).any(|w| w == b"\r\n\r\n") {
            break n;
        }
        tokio::task::yield_now().await;
    };
    let text = String::from_utf8_lossy(&head[..n]).to_string();
    if text.to_ascii_lowercase().contains("upgrade: websocket") {
        relay_socket(stream, backend, admitted, refusing).await;
    } else {
        http(stream, &text, backend).await;
    }
}

async fn http(mut stream: TcpStream, head: &str, backend: Backend) {
    let end = head.find("\r\n\r\n").unwrap() + 4;
    let mut consumed = vec![0; end];
    stream.read_exact(&mut consumed).await.unwrap();
    let path = head.split_whitespace().nth(1).unwrap_or_default();
    let header = |name: &str| {
        head.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name)
                .then(|| value.trim().to_owned())
        })
    };
    let length: usize = header("content-length")
        .and_then(|length| length.parse().ok())
        .unwrap_or(0);
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.unwrap();
    let user = authenticate(header("authorization").as_deref());
    let authorization = header("authorization");
    let (status, body) = if path.ends_with("/backend/admit") {
        if trusted_machine(authorization.as_deref()) {
            ("204 No Content", String::new())
        } else {
            (
                "401 Unauthorized",
                "not a trusted backend client".to_owned(),
            )
        }
    } else if path.ends_with("/backend/user-session") {
        let user_id = serde_json::from_slice::<Value>(&body)
            .ok()
            .and_then(|body| body["userId"].as_str().map(str::to_owned));
        match (trusted_machine(authorization.as_deref()), user_id) {
            (false, _) => (
                "401 Unauthorized",
                "not a trusted backend client".to_owned(),
            ),
            (true, None) => (
                "400 Bad Request",
                "userId must be a non-empty string".to_owned(),
            ),
            (true, Some(user_id)) => match existing_account_of(&backend, &user_id).await {
                Ok(Some(account)) => (
                    "200 OK",
                    json!({ "account": account, "issuer": BACKEND_ISSUER }).to_string(),
                ),
                Ok(None) => ("404 Not Found", json!({ "code": "no_account" }).to_string()),
                Err(403) => (
                    "403 Forbidden",
                    json!({ "code": "identity_not_authorized" }).to_string(),
                ),
                Err(_) => ("502 Bad Gateway", String::new()),
            },
        }
    } else if path.ends_with("/accounts/backend-link/challenge") {
        match user {
            None => ("401 Unauthorized", "signed_out".to_owned()),
            Some(user) => {
                let challenge = format!("{user}:{}", uuid_v4());
                backend
                    .challenges
                    .lock()
                    .unwrap()
                    .insert(challenge.clone(), user);
                ("200 OK", json!({ "challenge": challenge }).to_string())
            }
        }
    } else if path.ends_with("/accounts/backend-link") {
        match user {
            None => ("401 Unauthorized", "signed_out".to_owned()),
            Some(user) => match link(&backend, &user, &body).await {
                Some(body) => ("200 OK", body.to_string()),
                None => ("409 Conflict", "link_refused".to_owned()),
            },
        }
    } else if path.ends_with("/accounts/backend-user") {
        // Who is signed in, assigning nothing: `account` null while they
        // have none yet.
        match user {
            None => ("200 OK", json!({ "user": null }).to_string()),
            Some(user) => match existing_account_of(&backend, &user).await {
                Ok(account) => (
                    "200 OK",
                    json!({
                        "account": account,
                        "identity": { "issuer": BACKEND_ISSUER, "subject": user },
                        "claims": {},
                    })
                    .to_string(),
                ),
                Err(_) => ("502 Bad Gateway", String::new()),
            },
        }
    } else if path.ends_with("/accounts/backend-sign-in") {
        let failing = user
            .as_ref()
            .is_some_and(|user| backend.links.lock().unwrap().fail_sign_in.remove(user));
        match user {
            None => ("401 Unauthorized", "signed_out".to_owned()),
            Some(_) if failing => (
                "503 Service Unavailable",
                "account_request_failed".to_owned(),
            ),
            Some(user) => {
                let account = account_of(&backend, &user).await;
                let body = json!({
                    "account": account,
                    "identity": { "issuer": BACKEND_ISSUER, "subject": user },
                    "claims": {},
                    "expiresAt": 0,
                });
                ("200 OK", body.to_string())
            }
        }
    } else {
        ("404 Not Found", "Not found".to_owned())
    };
    // `backend/admit` names the bundle the main backend serves (jazz#5808).
    let bundle = if path.ends_with("/backend/admit") {
        format!("x-jazz-bundle: {}\r\n", backend.bundle)
    } else {
        String::new()
    };
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n{bundle}connection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

fn uuid_v4() -> String {
    jazz_app_support::ObjectId::new().to_string()
}

/// `backendLink`: the challenge was issued for this user, the device's proof
/// signs it; the core founds the device's account and links the user to it.
async fn link(backend: &Backend, user: &str, body: &[u8]) -> Option<Value> {
    let body: Value = serde_json::from_slice(body).ok()?;
    let challenge = body["challenge"].as_str()?;
    let issued_for = backend.challenges.lock().unwrap().remove(challenge)?;
    if issued_for != user {
        return None;
    }
    let subject = jazz_app_support::jazz::tools::identity::verify_device_account_challenge(
        body["proof"].as_str()?,
        challenge,
    )
    .ok()?
    .to_string();
    let guest = json!({
        "issuer": jazz_app_support::jazz::tools::identity::DEVICE_ACCOUNT_ISSUER,
        "subject": subject,
    });
    let (refused, appears, revoke) = {
        let mut links = backend.links.lock().unwrap();
        links.attempts += 1;
        (
            links.refused.contains(user),
            links.appear.contains(user),
            links.revoke,
        )
    };
    if appears {
        account_of(backend, user).await;
    }
    if refused || appears {
        return None;
    }
    let admin = |operation: &str, body: Value| {
        reqwest::Client::new()
            .post(format!(
                "{}/apps/{}/admin/accounts/{operation}",
                backend.core, backend.app
            ))
            .header("X-Jazz-Admin-Secret", &backend.admin_secret)
            .json(&body)
            .send()
    };
    let founded = admin("found-local-first", json!({ "identity": guest }))
        .await
        .ok()?;
    if !founded.status().is_success() {
        return None;
    }
    let linked = admin(
        "link",
        json!({
            "from": guest,
            "to": { "issuer": BACKEND_ISSUER, "subject": user },
            // The challenge's single-use intent, as createJazz passes it.
            "nonce": challenge.rsplit(':').next(),
        }),
    )
    .await
    .ok()?;
    if !linked.status().is_success() {
        return None;
    }
    let account: Value = linked.json().await.ok()?;
    if revoke {
        // The link stands; the guest's key is retired.
        let revoked = admin(
            "revoke",
            json!({
                "approver": { "issuer": BACKEND_ISSUER, "subject": user },
                "target": guest,
            }),
        )
        .await
        .ok()?;
        if !revoked.status().is_success() {
            return None;
        }
    }
    Some(json!({
        "account": account["account"],
        "identity": { "issuer": BACKEND_ISSUER, "subject": user },
        "claims": {},
        "expiresAt": 0,
    }))
}

/// The core resolves (or creates) the user's account.
async fn account_of(backend: &Backend, user: &str) -> String {
    let response: Value = reqwest::Client::new()
        .post(format!(
            "{}/apps/{}/admin/accounts/login-or-register",
            backend.core, backend.app
        ))
        .header("X-Jazz-Backend-Secret", &backend.backend_secret)
        .json(&json!({ "identity": { "issuer": BACKEND_ISSUER, "subject": user } }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    response["account"].as_str().unwrap().to_owned()
}

async fn relay_socket(
    stream: TcpStream,
    backend: Backend,
    admitted: Arc<Mutex<Vec<String>>>,
    refusing: Arc<Mutex<HashMap<String, String>>>,
) {
    let authorization = Arc::new(Mutex::new(None::<String>));
    let seen = authorization.clone();
    let Ok(mut client) = tokio_tungstenite::accept_hdr_async(
        stream,
        move |request: &Request, response: Response| {
            *seen.lock().unwrap() = request
                .headers()
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            Ok(response)
        },
    )
    .await
    else {
        return;
    };
    let Some(Ok(Message::Binary(prelude))) = client.next().await else {
        return;
    };
    let authorization = authorization.lock().unwrap().clone();
    let refusal = authorization
        .as_deref()
        .and_then(|header| header.strip_prefix("Bearer "))
        .and_then(|bearer| refusing.lock().unwrap().get(bearer).cloned());
    backend.events.log(format_args!(
        "socket prelude for {:?}: {}",
        authorization.as_deref().unwrap_or("no authorization"),
        if refusal.is_some() {
            "refused"
        } else {
            "passed on"
        }
    ));
    if let Some(message) = refusal {
        use jazz_app_support::jazz::wire::{
            WireError, WireErrorCode, WireFrame, WireRetry, encode_frame,
            encode_websocket_frame_batch,
        };
        let error = WireError::new(WireErrorCode::AuthFailed, WireRetry::Never, message);
        let frame = encode_frame(&WireFrame::Error(error)).unwrap();
        let batch = encode_websocket_frame_batch(&[frame]).unwrap();
        let _ = client.send(Message::Binary(batch.into())).await;
        let _ = client.close(None).await;
        return;
    }
    let Some((prelude, who)) = admit(&backend, &prelude, authorization.as_deref()).await else {
        backend.events.log("socket: not admitted");
        let _ = client.close(None).await;
        return;
    };
    backend
        .events
        .log(format_args!("socket admitted for {who}"));
    admitted.lock().unwrap().push(who);
    let url = format!(
        "{}/apps/{}/ws",
        backend.core.replacen("http", "ws", 1),
        backend.app
    );
    let Ok((mut core, _)) = tokio_tungstenite::connect_async(url).await else {
        backend
            .events
            .log("socket: the core refused the connection");
        return;
    };
    if core.send(Message::Binary(prelude.into())).await.is_err() {
        return;
    }
    let (mut to_client, mut from_client) = client.split();
    let (mut to_core, mut from_core) = core.split();
    let up = async {
        while let Some(Ok(message)) = from_client.next().await {
            if to_core.send(message).await.is_err() {
                break;
            }
        }
        let _ = to_core.close().await;
    };
    let down = async {
        while let Some(Ok(message)) = from_core.next().await {
            if to_client.send(message).await.is_err() {
                break;
            }
        }
        let _ = to_client.close().await;
    };
    tokio::select! {
        () = up => backend.events.log("socket ended by the client"),
        () = down => backend.events.log("socket ended by the core"),
    }
}

/// `admitPrelude`: a device's own proof, or a session its bearer vouches
/// for, rewritten with the backend's secret and the bundle it serves.
async fn admit(
    backend: &Backend,
    prelude: &[u8],
    authorization: Option<&str>,
) -> Option<(Vec<u8>, String)> {
    let mut prelude: Value = serde_json::from_slice(prelude).ok()?;
    let auth = prelude.get_mut("auth")?.as_object_mut()?;
    if trusted_machine(authorization) {
        // A trusted backend client states a stand-in secret (and maybe a
        // user's session), never a token or the admin secret of its own.
        for credential in ["jwt_token", "admin_secret"] {
            if !auth.get(credential).unwrap_or(&Value::Null).is_null() {
                return None;
            }
        }
        auth.get("backend_secret")?.as_str()?;
        auth.insert(
            "backend_secret".into(),
            backend.backend_secret.clone().into(),
        );
        prelude["bundle"] = backend.bundle.clone().into();
        return Some((serde_json::to_vec(&prelude).ok()?, "trusted".to_owned()));
    }
    for credential in ["backend_secret", "admin_secret"] {
        if !auth.get(credential).unwrap_or(&Value::Null).is_null() {
            return None;
        }
    }
    let token = auth.get("jwt_token").cloned().unwrap_or(Value::Null);
    let who = if let Some(token) = token.as_str() {
        // A guest: alone, and its own token says who it is.
        if !auth
            .get("backend_session")
            .unwrap_or(&Value::Null)
            .is_null()
        {
            return None;
        }
        auth.insert(
            "backend_secret".into(),
            backend.backend_secret.clone().into(),
        );
        format!("device:{}", token.len())
    } else {
        let stated = auth.get("backend_session")?.get("user_id")?.as_str()?;
        let user = authenticate(authorization)?;
        if user != stated {
            return None;
        }
        let account = account_of(backend, &user).await;
        let session = json!({
            "account_id": account,
            "issuer": BACKEND_ISSUER,
            "user_id": user,
            "claims": {},
        });
        auth.insert(
            "backend_secret".into(),
            backend.backend_secret.clone().into(),
        );
        auth.insert("backend_session".into(), session);
        format!("user:{user}")
    };
    prelude["bundle"] = backend.bundle.clone().into();
    Some((serde_json::to_vec(&prelude).ok()?, who))
}
