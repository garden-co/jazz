//! A Rust backend serving its users' clients: the app's Jazz endpoint
//! ([`jazz_app::Endpoint`]) over a plain host server, with an in-process
//! core, the app's `authenticate` deciding who each request's user is, and
//! the published permissions enforced on what they do.

#[path = "bindings/todos.rs"]
mod bindings;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bindings::{NewProject, NewTodo, Todo, projects, todos};
use futures::{SinkExt, StreamExt};
use jazz_app::endpoint::{Admitted, Answer, AuthUser, Endpoint, Request};
use jazz_app::{
    AppFiles, AppId, Auth, Backend, Config, DeviceKey, DurabilityTier, Error, Jazz, ReadTier,
    SessionState, TokenReason,
};
use jazz_server::JazzServer;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request as Upgrade, Response as Upgraded};

/// Todos are public. A project is private to the user who made it, and
/// only an admin (by their claims) makes one.
const PERMISSIONS: &str = r#"{
  "format": 1,
  "tables": {
    "projects": {
      "read": "$createdBy.account = session.user",
      "insert": "session.claims.role = 'admin'"
    },
    "todos": { "read": "true", "insert": "true", "update": "true", "delete": "true" }
  }
}"#;

fn client_files() -> AppFiles {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/todos");
    AppFiles::new(std::fs::read(format!("{dir}/jazz.app.json")).unwrap(), None)
}

/// The issuer and key of the app's own JWTs (HS256), as an app's auth
/// provider would sign them.
const JWT_ISSUER: &str = "https://auth.test";
const JWT_KEY: &[u8] = b"the app's signing key";

fn b64(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn hs256(input: &str, key: &[u8]) -> String {
    use hmac::Mac as _;
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(key).unwrap();
    mac.update(input.as_bytes());
    b64(&mac.finalize().into_bytes())
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// A JWT for `sub` from `issuer`, expiring at `exp`, signed with `key`.
fn jwt(sub: &str, issuer: &str, exp: u64, key: &[u8]) -> String {
    let head = b64(br#"{"alg":"HS256","typ":"JWT"}"#);
    let claims = b64(json!({ "iss": issuer, "sub": sub, "exp": exp })
        .to_string()
        .as_bytes());
    let input = format!("{head}.{claims}");
    let signature = hs256(&input, key);
    format!("{input}.{signature}")
}

/// The app's check of its own JWT: its signature, issuer and expiry. The
/// answer holds until the token's `exp`.
fn verify_jwt(token: &str) -> Option<AuthUser> {
    use base64::Engine as _;
    let (input, signature) = token.rsplit_once('.')?;
    if hs256(input, JWT_KEY) != signature {
        return None;
    }
    let claims: Value = serde_json::from_slice(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(input.split('.').nth(1)?)
            .ok()?,
    )
    .ok()?;
    if claims["iss"] != json!(JWT_ISSUER) {
        return None;
    }
    // The expiry is passed on: the endpoint treats a lapsed answer as no one.
    Some(AuthUser::new(
        claims["sub"].as_str()?,
        claims["exp"].as_u64()?,
    ))
}

/// The app's sign-in: `Bearer token-<user>[:admin]` names a user (an admin
/// with `:admin`), as does a JWT `verify_jwt` accepts; a request without a
/// bearer may be a device account's.
fn authenticate(request: Request) -> impl Future<Output = jazz_app::Result<Option<Answer>>> {
    let answer = match request.bearer() {
        Some(token) if token.matches('.').count() == 2 => verify_jwt(token).map(Answer::User),
        Some(token) => token.strip_prefix("token-").map(|user| {
            let (user, admin) = match user.split_once(':') {
                Some((user, "admin")) => (user, true),
                _ => (user, false),
            };
            let mut claims = serde_json::Map::new();
            claims.insert("role".into(), json!(if admin { "admin" } else { "member" }));
            Answer::User(AuthUser::new(user, u64::MAX).claims(claims))
        }),
        None => Some(Answer::DeviceAccount),
    };
    std::future::ready(Ok(answer))
}

struct App {
    app_id: AppId,
    server: JazzServer,
    backend: Jazz,
    endpoint: Endpoint,
    /// The endpoint's URL, as a client's `Config::server`.
    url: String,
    /// Who each socket was admitted as.
    admitted: Arc<Mutex<Vec<Admitted>>>,
    host: tokio::task::JoinHandle<()>,
}

impl App {
    async fn start() -> Self {
        let app_id = AppId::random();
        let server = JazzServer::builder()
            .with_app_id(app_id)
            .start()
            .await
            .expect("start a Jazz server");
        let backend = bindings::open(
            Config::new(app_id).server(server.base_url()).backend(
                Backend::new(
                    server.backend_secret(),
                    server.admin_secret(),
                    "todo-service",
                )
                .permissions(PERMISSIONS.as_bytes().to_vec()),
            ),
        )
        .await
        .expect("open the app's backend");
        let endpoint = Endpoint::new(&backend, authenticate).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/jazz", listener.local_addr().unwrap());
        let admitted = Arc::new(Mutex::new(Vec::new()));
        let host = tokio::spawn(host(listener, endpoint.clone(), admitted.clone()));
        Self {
            app_id,
            server,
            backend,
            endpoint,
            url,
            admitted,
            host,
        }
    }

    fn user(&self, auth: Auth) -> Config {
        Config::new(self.app_id).server(&self.url).user(auth)
    }

    fn path(&self, route: &str) -> String {
        format!("/jazz/apps/{}/{route}", self.app_id)
    }

    async fn stop(self) {
        self.host.abort();
        self.endpoint.shutdown().await.unwrap();
        self.backend.shutdown().await.unwrap();
        self.server.shutdown().await;
    }
}

/// A host's HTTP server, as small as it gets: requests go to
/// `Endpoint::handle`, upgraded sockets to `Endpoint::serve_socket`.
async fn host(listener: TcpListener, endpoint: Endpoint, admitted: Arc<Mutex<Vec<Admitted>>>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        tokio::spawn(serve(stream, endpoint.clone(), admitted.clone()));
    }
}

async fn serve(stream: TcpStream, endpoint: Endpoint, admitted: Arc<Mutex<Vec<Admitted>>>) {
    let mut head = vec![0; 16384];
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
        let seen = Arc::new(Mutex::new(None::<Request>));
        let upgrade = seen.clone();
        let Ok(socket) = tokio_tungstenite::accept_hdr_async(
            stream,
            move |request: &Upgrade, response: Upgraded| {
                *upgrade.lock().unwrap() = Some(Request {
                    method: request.method().to_string(),
                    path: request.uri().to_string(),
                    headers: request
                        .headers()
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or_default().to_owned()))
                        .collect(),
                    body: Vec::new(),
                });
                Ok(response)
            },
        )
        .await
        else {
            return;
        };
        let request = seen.lock().unwrap().take().unwrap();
        let (mut sink, stream) = socket.split();
        let (prelude_seen, prelude) = tokio::sync::oneshot::channel::<Vec<u8>>();
        let mut prelude_seen = Some(prelude_seen);
        let incoming = Box::pin(
            stream
                .take_while(|message| std::future::ready(matches!(message, Ok(m) if !m.is_close())))
                .filter_map(|message| {
                    std::future::ready(match message {
                        Ok(Message::Binary(data)) => Some(data.to_vec()),
                        // TS clients send their prelude as text.
                        Ok(Message::Text(text)) => Some(text.as_bytes().to_vec()),
                        _ => None,
                    })
                })
                .inspect(move |data| {
                    if let Some(seen) = prelude_seen.take() {
                        let _ = seen.send(data.clone());
                    }
                }),
        );
        let (to_client, mut from_relay) = tokio::sync::mpsc::channel::<Vec<u8>>(64);
        let outgoing = Box::pin(futures::sink::unfold(
            to_client,
            |sender, data: Vec<u8>| async move { sender.send(data).await.map(|()| sender) },
        ));
        // Who the endpoint admits this socket as, logged by the test.
        let log = {
            let (endpoint, request) = (endpoint.clone(), request.clone());
            async move {
                if let Ok(prelude) = prelude.await
                    && let Ok(Some(who)) = endpoint.admits(&request, &prelude).await
                {
                    admitted.lock().unwrap().push(who);
                }
            }
        };
        tokio::spawn(log);
        let relay = endpoint.serve_socket(request, incoming, outgoing);
        let forward = async {
            while let Some(data) = from_relay.recv().await {
                if sink.send(Message::Binary(data.into())).await.is_err() {
                    break;
                }
            }
        };
        let (_closed, ()) = futures::future::join(relay, forward).await;
        let _ = sink.close().await;
        return;
    }
    let mut stream = stream;
    let end = text.find("\r\n\r\n").unwrap() + 4;
    let mut consumed = vec![0; end];
    stream.read_exact(&mut consumed).await.unwrap();
    let mut lines = text[..end].lines();
    let mut start = lines.next().unwrap_or_default().split_whitespace();
    let (method, path) = (
        start.next().unwrap_or_default(),
        start.next().unwrap_or_default(),
    );
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| {
            let (key, value) = line.split_once(':')?;
            Some((key.trim().to_owned(), value.trim().to_owned()))
        })
        .collect();
    let length = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.unwrap();
    let response = endpoint
        .handle(Request {
            method: method.into(),
            path: path.into(),
            headers,
            body,
        })
        .await;
    let mut out = format!("HTTP/1.1 {} X\r\n", response.status);
    for (name, value) in &response.headers {
        out.push_str(&format!("{name}: {value}\r\n"));
    }
    out.push_str(&format!(
        "content-length: {}\r\nconnection: close\r\n\r\n",
        response.body.len()
    ));
    let _ = stream.write_all(out.as_bytes()).await;
    let _ = stream.write_all(&response.body).await;
}

async fn eventually<T>(mut probe: impl AsyncFnMut() -> Option<T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(found) = probe().await {
                return found;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("in time")
}

fn bearer(token: &'static str) -> Auth {
    Auth::bearer(move |_reason: TokenReason| async move { Ok(Some(token.to_owned())) })
}

/// Every project `jazz` reads from the server.
async fn remote_projects(jazz: &Jazz) -> Vec<String> {
    jazz.all_with(projects::query(), ReadTier::Remote)
        .await
        .unwrap()
        .into_iter()
        .map(|project| project.name)
        .collect()
}

/// Check 2 (a user sees and writes only what the permissions allow) and
/// check 3 (a second user can't read the first user's private rows), over
/// the endpoint's sockets.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signed_in_user_syncs_through_the_backend_under_their_permissions() {
    let app = App::start().await;
    let server = DurabilityTier::GlobalServer;

    let alice = Jazz::open(client_files(), app.user(bearer("token-alice:admin")))
        .await
        .expect("alice signs in through the endpoint");
    let SessionState::SignedIn { user } = alice.session().unwrap().state() else {
        panic!("signed in");
    };
    assert_eq!(user.subject, "alice");
    // The core takes alice's project (she is an admin) and her todo.
    let project = alice
        .durable(server)
        .insert(NewProject::new("Launch"))
        .await
        .expect("the core takes an admin's project");
    let todo = alice
        .durable(server)
        .insert(NewTodo::new("Ship it", false))
        .await
        .unwrap();
    assert!(
        app.admitted
            .lock()
            .unwrap()
            .contains(&Admitted::User("alice".into()))
    );
    // The backend sees the public todo, through the relay, as alice's.
    let seen: Vec<Todo> = eventually(async || {
        let rows = app
            .backend
            .all_with(todos::query(), ReadTier::Remote)
            .await
            .unwrap();
        (!rows.is_empty()).then_some(rows)
    })
    .await;
    assert_eq!(seen, vec![todo.clone()]);
    assert_eq!(remote_projects(&alice).await, vec![project.name]);

    // Bob is no admin: the core refuses his project; his todo goes through.
    let bob = Jazz::open(client_files(), app.user(bearer("token-bob")))
        .await
        .expect("bob signs in");
    assert!(
        bob.durable(server)
            .insert(NewProject::new("Not bob's to make"))
            .await
            .is_err(),
        "the core refuses a non-admin's project"
    );
    let bobs = bob
        .durable(server)
        .insert(NewTodo::new("Bob's todo", false))
        .await
        .unwrap();
    // Bob reads the public todos, alice's among them, and none of her
    // private projects.
    let todos_bob_sees = eventually(async || {
        let rows = bob
            .all_with(todos::query(), ReadTier::Remote)
            .await
            .unwrap();
        (rows.len() == 2).then_some(rows)
    })
    .await;
    assert!(todos_bob_sees.contains(&todo) && todos_bob_sees.contains(&bobs));
    assert_eq!(remote_projects(&bob).await, Vec::<String>::new());
    // Nor does the backend's own user.
    assert_eq!(remote_projects(&app.backend).await, Vec::<String>::new());

    bob.shutdown().await.unwrap();
    alice.shutdown().await.unwrap();
    app.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_device_account_syncs_when_the_app_lets_it_in() {
    let app = App::start().await;
    let key = DeviceKey::generate().unwrap();
    let phone = Jazz::open(client_files(), app.user(Auth::Device(key.clone())))
        .await
        .expect("open as a device account");
    assert_eq!(phone.account(), Some(key.account(app.app_id)));
    let todo = phone
        .insert(NewTodo::new("From the phone", false))
        .await
        .unwrap();
    let seen = eventually(async || {
        let rows = app
            .backend
            .all_with(todos::query(), ReadTier::Remote)
            .await
            .unwrap();
        (!rows.is_empty()).then_some(rows)
    })
    .await;
    assert_eq!(seen, vec![todo]);
    assert!(
        app.admitted
            .lock()
            .unwrap()
            .contains(&Admitted::DeviceAccount)
    );
    phone.shutdown().await.unwrap();
    app.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_backend_acts_as_a_request_user() {
    let app = App::start().await;
    let as_alice = Request::new("POST", "/").header("authorization", "Bearer token-alice:admin");
    let as_bob = Request::new("POST", "/").header("authorization", "Bearer token-bob");

    // No account until they sign in, or the backend makes one.
    assert!(matches!(
        app.endpoint.for_user(&as_alice).await,
        Err(Error::NoAccount(user)) if user == "alice"
    ));
    assert_eq!(app.endpoint.account_for(&as_alice).await.unwrap(), None);
    let account = app.endpoint.create_account_for("alice").await.unwrap();
    assert_eq!(
        app.endpoint.account_for(&as_alice).await.unwrap(),
        Some(account)
    );
    app.endpoint.create_account_for("bob").await.unwrap();
    assert!(matches!(
        app.endpoint.for_user(&Request::new("POST", "/")).await,
        Err(Error::SignedOut)
    ));

    // Checks 2 and 3, acting as each user on the backend.
    let server = DurabilityTier::GlobalServer;
    let alice = app.endpoint.for_user(&as_alice).await.unwrap();
    assert_eq!(alice.account(), Some(account));
    alice
        .durable(server)
        .insert(NewProject::new("Alice's"))
        .await
        .expect("the core takes an admin's project");
    let bob = app.endpoint.for_user(&as_bob).await.unwrap();
    assert!(
        bob.durable(server)
            .insert(NewProject::new("Bob's"))
            .await
            .is_err(),
        "the core refuses a non-admin's project"
    );
    bob.durable(server)
        .insert(NewTodo::new("Bob's todo", false))
        .await
        .unwrap();
    assert_eq!(remote_projects(&alice).await, vec!["Alice's".to_owned()]);
    assert_eq!(remote_projects(&bob).await, Vec::<String>::new());
    // A user's handle never stops the backend.
    bob.shutdown().await.unwrap();
    assert!(app.backend.all(todos::query()).await.is_ok());
    app.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_endpoint_refuses_what_it_should() {
    let app = App::start().await;
    let sign_in = app.path("accounts/backend-sign-in");

    // HTTP: no one signed in, wrong methods, other apps, not yet supported.
    let response = app.endpoint.handle(Request::new("POST", &sign_in)).await;
    assert_eq!(
        (response.status, response.text_body().as_str()),
        (401, "signed_out")
    );
    let response = app.endpoint.handle(Request::new("GET", &sign_in)).await;
    assert_eq!(response.status, 405);
    let response = app
        .endpoint
        .handle(Request::new("POST", app.path("accounts/backend-user")))
        .await;
    assert_eq!(
        serde_json::from_slice::<Value>(&response.body).unwrap(),
        json!({ "user": null })
    );
    let response = app
        .endpoint
        .handle(Request::new("POST", &sign_in).header("authorization", "Bearer token-carol"))
        .await;
    assert_eq!(response.status, 200);
    let signed_in: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(
        signed_in["identity"],
        json!({ "issuer": "jazz:backend", "subject": "carol" })
    );
    assert_eq!(signed_in["claims"], json!({ "role": "member" }));
    let other = format!("/jazz/apps/{}/accounts/backend-sign-in", AppId::random());
    assert_eq!(
        app.endpoint
            .handle(Request::new("POST", other))
            .await
            .status,
        404
    );
    let link = app.path("accounts/backend-link");
    assert_eq!(
        app.endpoint.handle(Request::new("POST", link)).await.status,
        501
    );
    assert_eq!(
        app.endpoint
            .handle(Request::new("GET", app.path("ws")))
            .await
            .status,
        426
    );

    // A bearer that names no one never opens.
    assert!(matches!(
        Jazz::open(client_files(), app.user(bearer("nobody"))).await,
        Err(Error::SignedOut)
    ));

    // Preludes: a stated user other than the bearer's, or a client stating
    // a backend secret, is not admitted.
    let as_carol =
        Request::new("GET", app.path("ws")).header("authorization", "Bearer token-carol");
    let stating = |user: &str| {
        serde_json::to_vec(&json!({ "auth": { "backend_session": { "user_id": user } } })).unwrap()
    };
    assert_eq!(
        app.endpoint
            .admits(&as_carol, &stating("carol"))
            .await
            .unwrap(),
        Some(Admitted::User("carol".into()))
    );
    assert_eq!(
        app.endpoint
            .admits(&as_carol, &stating("alice"))
            .await
            .unwrap(),
        None
    );
    let with_secret = serde_json::to_vec(&json!({
        "auth": { "backend_secret": "guess", "backend_session": { "user_id": "carol" } }
    }))
    .unwrap();
    assert_eq!(
        app.endpoint.admits(&as_carol, &with_secret).await.unwrap(),
        None
    );
    let forged = serde_json::to_vec(&json!({ "auth": { "jwt_token": "a.b.c" } })).unwrap();
    assert_eq!(app.endpoint.admits(&as_carol, &forged).await.unwrap(), None);
    app.stop().await;
}

/// Check 1: a missing, expired, wrong-issuer or badly signed JWT signs no
/// one in, on the account routes, the sockets and `for_user`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_missing_expired_or_foreign_jwt_is_refused() {
    let app = App::start().await;
    let hour = now() + 3600;
    let valid = jwt("gail", JWT_ISSUER, hour, JWT_KEY);
    let refused = [
        ("missing", None),
        ("expired", Some(jwt("gail", JWT_ISSUER, now() - 1, JWT_KEY))),
        (
            "wrong issuer",
            Some(jwt("gail", "https://evil.test", hour, JWT_KEY)),
        ),
        (
            "bad signature",
            Some(jwt("gail", JWT_ISSUER, hour, b"another key")),
        ),
    ];
    let sign_in = app.path("accounts/backend-sign-in");
    let request = |token: Option<&str>| {
        let request = Request::new("POST", &sign_in);
        match token {
            Some(token) => request.header("authorization", format!("Bearer {token}")),
            None => request,
        }
    };
    let ok = app.endpoint.handle(request(Some(&valid))).await;
    assert_eq!(ok.status, 200, "a valid JWT signs in");
    let stating_gail =
        serde_json::to_vec(&json!({ "auth": { "backend_session": { "user_id": "gail" } } }))
            .unwrap();
    for (what, token) in &refused {
        let response = app.endpoint.handle(request(token.as_deref())).await;
        assert_eq!(
            (response.status, response.text_body().as_str()),
            (401, "signed_out"),
            "{what}"
        );
        let mut socket = request(token.as_deref());
        socket.path = app.path("ws");
        assert_eq!(
            app.endpoint.admits(&socket, &stating_gail).await.unwrap(),
            None,
            "{what}"
        );
        assert!(
            matches!(app.endpoint.for_user(&socket).await, Err(Error::SignedOut)),
            "{what}"
        );
    }
    // A client whose bearer is an expired JWT never opens.
    let expired = jwt("gail", JWT_ISSUER, now() - 1, JWT_KEY);
    let expired = Auth::bearer(move |_| {
        let expired = expired.clone();
        async move { Ok(Some(expired)) }
    });
    assert!(matches!(
        Jazz::open(client_files(), app.user(expired)).await,
        Err(Error::SignedOut)
    ));
    // The valid one syncs.
    let valid = Auth::bearer(move |_| {
        let valid = valid.clone();
        async move { Ok(Some(valid)) }
    });
    let gail = Jazz::open(client_files(), app.user(valid)).await.unwrap();
    gail.durable(DurabilityTier::GlobalServer)
        .insert(NewTodo::new("Gail's", false))
        .await
        .unwrap();
    gail.shutdown().await.unwrap();
    app.stop().await;
}

/// Only the credential the endpoint vouched for, inside `auth`, is
/// forwarded to the core, beside the backend secret; other prelude fields
/// are refused or dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_the_vouched_credential_reaches_the_core() {
    let app = App::start().await;
    app.endpoint.create_account_for("hal").await.unwrap();
    let seed: [u8; 32] = rand_seed();
    let device_token = jazz_app::jazz::tools::identity::mint_jazz_self_signed_token(
        &seed,
        "urn:jazz:local-first",
        &app.app_id.to_string(),
        3600,
    )
    .unwrap();
    let socket = Request::new("GET", app.path("ws"));
    let as_hal = socket.clone().header("authorization", "Bearer token-hal");
    let forwarded = |request: Request, prelude: Value| {
        let endpoint = app.endpoint.clone();
        async move {
            endpoint
                .forwarded_prelude(&request, &serde_json::to_vec(&prelude).unwrap())
                .await
                .unwrap()
        }
    };
    let keys = |value: &Value| {
        let mut keys: Vec<String> = value.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        keys
    };

    // A device account: its token, and the secret.
    let guest = forwarded(
        socket.clone(),
        json!({ "peer_identity": "p", "auth": { "jwt_token": device_token }, "bundle": "laxer" }),
    )
    .await
    .expect("a device account is admitted");
    assert_eq!(keys(&guest), ["auth", "bundle", "peer_identity"]);
    assert_eq!(keys(&guest["auth"]), ["backend_secret", "jwt_token"]);
    assert_eq!(guest["auth"]["jwt_token"], json!(device_token));
    assert_eq!(guest["bundle"], json!(app.backend.bundle().unwrap()));

    // A user: the vouched session, and the secret.
    let user = forwarded(
        as_hal.clone(),
        json!({ "auth": { "jwt_token": null, "backend_session": { "user_id": "hal", "claims": { "role": "admin" } } } }),
    )
    .await
    .expect("hal is admitted");
    assert_eq!(keys(&user), ["auth", "bundle"]);
    assert_eq!(keys(&user["auth"]), ["backend_secret", "backend_session"]);
    // The claims are the ones `authenticate` gave, not the client's.
    assert_eq!(
        user["auth"]["backend_session"]["claims"],
        json!({ "role": "member" })
    );

    // A TS client's prelude repeats `auth` beside it, with `sub`: admitted,
    // and nothing but the rebuilt fields reaches the core.
    let ts_shaped = forwarded(
        socket.clone(),
        json!({
            "peer_identity": "p",
            "jwt_token": device_token,
            "backend_secret": null,
            "sub": "s",
            "auth": { "jwt_token": device_token, "backend_secret": null, "sub": "s" },
            "jazz_renewal": 1,
        }),
    )
    .await
    .expect("a TS client's prelude is admitted");
    assert_eq!(keys(&ts_shaped), ["auth", "bundle", "peer_identity"]);
    assert_eq!(keys(&ts_shaped["auth"]), ["backend_secret", "jwt_token"]);

    // Refused shapes.
    let refused = [
        json!({ "jwt_token": device_token, "auth": {} }),
        json!({ "jwt_token": device_token }),
        json!({ "jwt_token": "another", "auth": { "jwt_token": device_token } }),
        json!({ "backend_secret": "x", "auth": { "jwt_token": device_token } }),
        json!({ "backend_session": { "user_id": "hal" }, "auth": { "jwt_token": device_token } }),
        json!({ "auth": { "jwt_token": device_token, "backend_session": { "user_id": "hal" } } }),
        json!({ "auth": { "jwt_token": device_token, "backend_secret": "x" } }),
        json!({ "auth": { "jwt_token": device_token }, "bootstrap_catalogue": true }),
        json!({ "auth": { "jwt_token": device_token }, "local_first_audience": "https://x" }),
        json!({ "auth": { "admin_secret": "x", "backend_session": { "user_id": "hal" } } }),
        json!({ "peer_identity": 1, "auth": { "jwt_token": device_token } }),
    ];
    for prelude in refused {
        assert_eq!(
            forwarded(as_hal.clone(), prelude.clone()).await,
            None,
            "{prelude}"
        );
    }
    app.stop().await;
}

fn rand_seed() -> [u8; 32] {
    let mut seed = [0; 32];
    for (i, byte) in seed.iter_mut().enumerate() {
        *byte = (now() as u8).wrapping_add(i as u8).wrapping_mul(31);
    }
    seed
}

/// An `authenticate` answer whose expiry has passed signs no one in,
/// anywhere: `backend-sign-in`, `backend-user`, sockets, `for_user` and
/// `account_for` (S1). Without the check, sign-in answers 200 with the
/// account.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expired_answer_is_no_one() {
    let app = App::start().await;
    app.endpoint.create_account_for("ivy").await.unwrap();
    let lapsed = jwt("ivy", JWT_ISSUER, now() - 5, JWT_KEY);
    // The test's `verify_jwt` passes a lapsed `exp` through, as an app
    // that forgets to check it would.
    let request = Request::new("POST", app.path("accounts/backend-user"))
        .header("authorization", format!("Bearer {lapsed}"));
    let user = app.endpoint.handle(request.clone()).await;
    assert_eq!(
        serde_json::from_slice::<Value>(&user.body).unwrap(),
        json!({ "user": null })
    );
    assert_eq!(app.endpoint.account_for(&request).await.unwrap(), None);
    assert!(matches!(
        app.endpoint.for_user(&request).await,
        Err(Error::SignedOut)
    ));
    // Nor does sign-in answer with her account, nor a socket open.
    let sign_in = Request {
        path: app.path("accounts/backend-sign-in"),
        ..request.clone()
    };
    let signed_in = app.endpoint.handle(sign_in).await;
    assert_eq!(
        (signed_in.status, signed_in.text_body().as_str()),
        (401, "signed_out")
    );
    let socket = Request {
        path: app.path("ws"),
        method: "GET".into(),
        ..request
    };
    let stating_ivy =
        serde_json::to_vec(&json!({ "auth": { "backend_session": { "user_id": "ivy" } } }))
            .unwrap();
    assert_eq!(
        app.endpoint.admits(&socket, &stating_ivy).await.unwrap(),
        None
    );
    app.stop().await;
}

/// Check 4: a malformed or oversized prelude, an oversized message and a
/// flood of sockets are refused, and the endpoint keeps serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_and_oversized_input_is_refused_and_the_endpoint_serves_on() {
    let app = App::start().await;
    let ws = app.url.replacen("http", "ws", 1) + &format!("/apps/{}/ws", app.app_id);
    // Sends `messages` on a raw socket, and reads until the endpoint closes it.
    let refused = async |messages: Vec<Vec<u8>>| {
        let (mut socket, _) = tokio_tungstenite::connect_async(&ws).await.unwrap();
        for message in messages {
            if socket.send(Message::Binary(message.into())).await.is_err() {
                break;
            }
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(Ok(message)) = socket.next().await {
                if message.is_close() {
                    break;
                }
            }
        })
        .await
        .expect("the endpoint closes the socket");
    };
    refused(vec![b"\xff\x00not json".to_vec()]).await;
    refused(vec![b"[1, 2, 3]".to_vec()]).await;
    refused(vec![vec![b' '; 70 * 1024]]).await;
    let mut big = br#"{"auth":{"jwt_token":""#.to_vec();
    big.extend(std::iter::repeat_n(b'a', 70 * 1024));
    big.extend(br#""}}"#);
    refused(vec![big]).await;

    // An admitted socket that sends more than the core takes in one
    // message is closed with 1009.
    app.endpoint.create_account_for("jo").await.unwrap();
    let prelude =
        serde_json::to_vec(&json!({ "auth": { "backend_session": { "user_id": "jo" } } })).unwrap();
    let incoming = futures::stream::iter(vec![prelude, vec![0; 3 * 1024 * 1024]])
        .chain(futures::stream::pending());
    let closed = app
        .endpoint
        .serve_socket(
            Request::new("GET", app.path("ws")).header("authorization", "Bearer token-jo"),
            Box::pin(incoming),
            futures::sink::drain(),
        )
        .await;
    assert_eq!(closed.code, 1009, "{closed:?}");

    // Past the socket cap, a socket is turned away with 1013.
    let capped = app.endpoint.clone().max_sockets(0);
    let closed = capped
        .serve_socket(
            Request::new("GET", app.path("ws")),
            Box::pin(futures::stream::pending()),
            futures::sink::drain(),
        )
        .await;
    assert_eq!(closed.code, 1013);
    // An oversized request body is refused.
    let mut sign_in = Request::new("POST", app.path("accounts/backend-sign-in"));
    sign_in.body = vec![0; 65 * 1024];
    assert_eq!(app.endpoint.handle(sign_in).await.status, 413);

    // And the endpoint still serves a user.
    let kim = Jazz::open(client_files(), app.user(bearer("token-kim")))
        .await
        .unwrap();
    kim.durable(DurabilityTier::GlobalServer)
        .insert(NewTodo::new("Still serving", false))
        .await
        .unwrap();
    kim.shutdown().await.unwrap();
    app.stop().await;
}

/// A TS client's socket: its prelude as a text frame, shaped as TS's
/// `encodeWebSocketPrelude` writes it (auth repeated beside `auth`, with
/// `sub`), its bearer on the upgrade. It is admitted and stays open.
async fn a_ts_shaped_socket_is_admitted(app: &App, ws: &str) {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
    let token = jwt("tess", JWT_ISSUER, now() + 3600, JWT_KEY);
    let sign_in = app
        .endpoint
        .handle(
            Request::new("POST", app.path("accounts/backend-sign-in"))
                .header("authorization", format!("Bearer {token}")),
        )
        .await;
    assert_eq!(sign_in.status, 200);
    let account = serde_json::from_slice::<Value>(&sign_in.body).unwrap()["account"]
        .as_str()
        .unwrap()
        .to_owned();
    let session = json!({
        "account_id": account,
        "issuer": "jazz:backend",
        "user_id": "tess",
        "claims": {},
        "authMode": "external",
    });
    let auth = json!({ "jwt_token": null, "backend_session": session });
    let peer = serde_json::to_string(&(account.as_str(), "jazz:backend", "tess")).unwrap();
    let prelude = json!({
        "peer_identity": peer,
        "jwt_token": null,
        "backend_session": session,
        "auth": { "jwt_token": null, "backend_session": auth["backend_session"], "sub": "tess" },
        "sub": "tess",
    });
    let mut request = ws.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    socket
        .send(Message::Text(prelude.to_string().into()))
        .await
        .unwrap();
    // Then a binary frame. Had the text prelude been dropped, this frame
    // would be read as the prelude and refused (1008, `session missing`);
    // admitted, it reaches the core, which ends the socket its own way.
    socket
        .send(Message::Binary(b"not a wire frame".to_vec().into()))
        .await
        .unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(Ok(message)) = socket.next().await {
            if let Message::Close(frame) = message {
                return frame;
            }
        }
        None
    })
    .await
    .expect("the socket ends");
    assert!(
        !matches!(&closed, Some(frame) if u16::from(frame.code) == 1008),
        "a TS client's text prelude is admitted: {closed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ts_clients_text_prelude_is_admitted() {
    let app = App::start().await;
    let ws = app.url.replacen("http", "ws", 1) + &format!("/apps/{}/ws", app.app_id);
    a_ts_shaped_socket_is_admitted(&app, &ws).await;
    assert!(
        app.admitted
            .lock()
            .unwrap()
            .contains(&Admitted::User("tess".into()))
    );
    app.stop().await;
}

/// The same through the Axum adapter.
#[cfg(feature = "axum")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_axum_router_admits_a_ts_clients_text_prelude() {
    let app = App::start().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = format!(
        "ws://{}/jazz/apps/{}/ws",
        listener.local_addr().unwrap(),
        app.app_id
    );
    let router = app.endpoint.router();
    let served = tokio::spawn(async move { axum::serve(listener, router).await });
    a_ts_shaped_socket_is_admitted(&app, &ws).await;
    served.abort();
    app.stop().await;
}
