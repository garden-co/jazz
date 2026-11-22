//! A Rust backend serving its users' clients: the app's Jazz endpoint
//! ([`jazz_app_support::Endpoint`]) over a plain host server, with an in-process
//! core, the app's `authenticate` deciding who each request's user is, and
//! the published permissions enforced on what they do.

#[path = "bindings/todos.rs"]
mod bindings;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bindings::{NewProject, NewTodo, Todo, projects, todos};
use futures::{SinkExt, StreamExt};
use jazz_app_support::endpoint::{
    Admitted, Answer, AuthUser, Endpoint, GuestKeyAfterLink, Request,
};
use jazz_app_support::{
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
fn authenticate(
    request: Request,
) -> impl Future<Output = jazz_app_support::Result<Option<Answer>>> {
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
        Self::start_with(|endpoint| endpoint).await
    }

    async fn start_with(configure: impl FnOnce(Endpoint) -> Endpoint) -> Self {
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
        let endpoint = configure(Endpoint::new(&backend, authenticate).unwrap());
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
        let (closed, ()) = futures::future::join(relay, forward).await;
        // Close as the endpoint says, as a host's server does.
        let _ = sink
            .send(Message::Close(Some(
                tokio_tungstenite::tungstenite::protocol::CloseFrame {
                    code: closed.code.into(),
                    reason: closed.reason.into(),
                },
            )))
            .await;
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

/// A user's handle shares the backend's trusted client: it can't
/// disconnect it for every user, see every user's refusals, or delete
/// the backend's store.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_user_handle_refuses_the_clients_lifecycle() {
    let app = App::start().await;
    app.endpoint.create_account_for("bob").await.unwrap();
    let as_bob = Request::new("POST", "/").header("authorization", "Bearer token-bob");
    let bob = app.endpoint.for_user(&as_bob).await.unwrap();

    #[track_caller]
    fn unsupported(result: Result<(), Error>) {
        assert!(matches!(result, Err(Error::Unsupported(_))), "{result:?}");
    }
    unsupported(bob.disconnect().await);
    unsupported(bob.reconnect().await);
    unsupported(bob.mutation_errors().await.map(drop));
    unsupported(bob.delete_client_storage().await);

    // The shared client is still connected, and still runs.
    tokio::time::timeout(
        Duration::from_secs(10),
        bob.durable(DurabilityTier::GlobalServer)
            .insert(NewTodo::new("still synced", false)),
    )
    .await
    .expect("the shared client is still connected")
    .unwrap();
    assert!(app.backend.all(todos::query()).await.is_ok());
    app.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_endpoint_refuses_what_it_should() {
    let app = App::start().await;
    let sign_in = app.path("accounts/backend-sign-in");

    // HTTP: no one signed in (also for linking), wrong methods, other apps.
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
    // Another app id in the path (a TS client derives one from its
    // endpoint URL) is served as this endpoint's own app, as by createJazz.
    let other = format!("/jazz/apps/{}/accounts/backend-sign-in", AppId::random());
    let served = app
        .endpoint
        .handle(Request::new("POST", &other).header("authorization", "Bearer token-carol"))
        .await;
    assert_eq!(served.status, 200);
    assert_eq!(
        serde_json::from_slice::<Value>(&served.body).unwrap()["identity"]["subject"],
        json!("carol")
    );
    assert!(
        app.endpoint
            .is_socket(&format!("/jazz/apps/{}/ws", AppId::random()))
    );
    assert_eq!(
        app.endpoint
            .handle(Request::new("POST", "/jazz/apps//accounts/backend-sign-in"))
            .await
            .status,
        404
    );
    let link = app.path("accounts/backend-link");
    assert_eq!(
        app.endpoint.handle(Request::new("POST", link)).await.status,
        401
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_device_account_links_to_the_user_who_signs_up() {
    let app = App::start().await;
    let key = DeviceKey::generate().unwrap();
    let device = Jazz::open(client_files(), app.user(Auth::Device(key.clone())))
        .await
        .unwrap();
    let written = device
        .insert(NewTodo::new("Before signing up", false))
        .await
        .unwrap();
    let session = device.session().unwrap().clone();

    // Dana signs up with the app's own auth, and keeps her device's data.
    let dana = session
        .link("token-dana")
        .await
        .expect("link the device account");
    assert_eq!(dana.account, key.account(app.app_id));
    assert_eq!(dana.subject, "dana");
    assert!(matches!(
        session.link("not-a-token").await,
        Err(Error::SignedOut)
    ));

    // Signed in with her bearer elsewhere, she reaches the same account.
    let laptop = Jazz::open(client_files(), app.user(bearer("token-dana")))
        .await
        .unwrap();
    assert_eq!(laptop.account(), Some(dana.account));
    let seen = eventually(async || {
        let rows = laptop
            .all_with(todos::query(), ReadTier::Remote)
            .await
            .unwrap();
        (!rows.is_empty()).then_some(rows)
    })
    .await;
    assert_eq!(seen, vec![written]);

    // A challenge links only its own user, and only with a proof of it.
    let as_erin = |route: &str| {
        Request::new("POST", app.path(route)).header("authorization", "Bearer token-erin")
    };
    let issued = app
        .endpoint
        .handle(as_erin("accounts/backend-link/challenge"))
        .await;
    assert_eq!(issued.status, 200);
    let challenge = serde_json::from_slice::<Value>(&issued.body).unwrap()["challenge"]
        .as_str()
        .unwrap()
        .to_owned();
    let with = |body: Value| {
        let mut request = as_erin("accounts/backend-link");
        request.body = serde_json::to_vec(&body).unwrap();
        request
    };
    let forged = app
        .endpoint
        .handle(with(json!({ "challenge": challenge, "proof": "a.b" })))
        .await;
    assert_eq!(forged.status, 401);
    let as_frank = Request {
        headers: vec![("authorization".into(), "Bearer token-frank".into())],
        ..with(json!({ "challenge": challenge, "proof": "a.b" }))
    };
    let foreign = app.endpoint.handle(as_frank).await;
    assert_eq!(
        (foreign.status, foreign.text_body().as_str()),
        (400, "invalid_link_challenge")
    );
    let tampered = format!("{challenge}x");
    let tampered = app
        .endpoint
        .handle(with(json!({ "challenge": tampered, "proof": "a.b" })))
        .await;
    assert_eq!(tampered.status, 400);

    laptop.shutdown().await.unwrap();
    device.shutdown().await.unwrap();
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
    let device_token = jazz_app_support::jazz::tools::identity::mint_jazz_self_signed_token(
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

/// A device key's seed, from its `jazz-auth-v1:` form.
fn seed_of(key: &DeviceKey) -> [u8; 32] {
    use base64::Engine as _;
    let text = key.to_string();
    let encoded = text
        .strip_prefix("jazz-auth-v1:")
        .unwrap()
        .trim_end_matches('=');
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .unwrap()
        .try_into()
        .unwrap()
}

fn sign(seed: &[u8; 32], challenge: &str) -> String {
    jazz_app_support::jazz::tools::identity::sign_device_account_challenge(seed, challenge).unwrap()
}

/// A link challenge `endpoint` issues for `user`.
async fn challenge_for(app: &App, endpoint: &Endpoint, user: &str) -> String {
    let issued = endpoint
        .handle(
            Request::new("POST", app.path("accounts/backend-link/challenge"))
                .header("authorization", format!("Bearer token-{user}")),
        )
        .await;
    assert_eq!(issued.status, 200);
    assert!(
        issued
            .headers
            .contains(&("cache-control".into(), "no-store".into())),
        "a challenge is never cached"
    );
    serde_json::from_slice::<Value>(&issued.body).unwrap()["challenge"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// Links the device with `seed` to `user`, proving it on `proven` and
/// presenting `presented`.
async fn link_as(
    app: &App,
    endpoint: &Endpoint,
    user: &str,
    seed: &[u8; 32],
    proven: &str,
    presented: &str,
) -> jazz_app_support::endpoint::Response {
    let mut request = Request::new("POST", app.path("accounts/backend-link"))
        .header("authorization", format!("Bearer token-{user}"));
    request.body = serde_json::to_vec(&json!({
        "challenge": presented,
        "proof": sign(seed, proven),
    }))
    .unwrap();
    endpoint.handle(request).await
}

async fn link(app: &App, endpoint: &Endpoint, user: &str, seed: &[u8; 32]) -> (u16, Value) {
    let challenge = challenge_for(app, endpoint, user).await;
    let response = link_as(app, endpoint, user, seed, &challenge, &challenge).await;
    let body = serde_json::from_slice(&response.body).unwrap_or(Value::Null);
    (response.status, body)
}

/// By default (TS's `keep`), a key that linked can link a second user into
/// its account; with `Revoke` it is retired once it linked, so it can't,
/// and the first link still answers on a retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_linked_key_links_a_second_user_unless_the_endpoint_revokes_it() {
    let app = App::start().await;

    // Keep: the second user joins the device's account.
    let key = DeviceKey::generate().unwrap();
    let seed = seed_of(&key);
    let (status, first) = link(&app, &app.endpoint, "lena", &seed).await;
    assert_eq!(status, 200);
    assert_eq!(
        first["account"],
        json!(key.account(app.app_id).0.to_string())
    );
    let (status, second) = link(&app, &app.endpoint, "max", &seed).await;
    assert_eq!(status, 200, "keep lets a linked key link again");
    assert_eq!(second["account"], first["account"]);

    // Revoke: the key links once.
    let revoking = app
        .endpoint
        .clone()
        .guest_key_after_link(GuestKeyAfterLink::Revoke);
    let key = DeviceKey::generate().unwrap();
    let seed = seed_of(&key);
    let (status, first) = link(&app, &revoking, "nia", &seed).await;
    assert_eq!(status, 200);
    assert_eq!(
        first["account"],
        json!(key.account(app.app_id).0.to_string())
    );
    let (status, _) = link(&app, &revoking, "otto", &seed).await;
    assert_eq!(status, 409, "a revoked key links no second user");
    let other = Request::new("POST", app.path("accounts/backend-user"))
        .header("authorization", "Bearer token-otto");
    assert_eq!(
        serde_json::from_slice::<Value>(&revoking.handle(other).await.body).unwrap()["account"],
        Value::Null,
        "otto has no account"
    );
    // A retry of nia's link (its answer lost) still answers.
    let (status, again) = link(&app, &revoking, "nia", &seed).await;
    assert_eq!(status, 200);
    assert_eq!(again["account"], first["account"]);
    app.stop().await;
}

/// An expired challenge, and a proof made over another challenge, link
/// nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expired_or_mismatched_challenge_links_nothing() {
    use base64::Engine as _;
    use hmac::Mac as _;
    let app = App::start().await;
    let seed = seed_of(&DeviceKey::generate().unwrap());

    // A proof over challenge A, presented with challenge B.
    let a = challenge_for(&app, &app.endpoint, "pia").await;
    let b = challenge_for(&app, &app.endpoint, "pia").await;
    let response = link_as(&app, &app.endpoint, "pia", &seed, &a, &b).await;
    assert_eq!(
        (response.status, response.text_body().as_str()),
        (401, "invalid_local_first_proof")
    );

    // A challenge signed with the endpoint's own key, but expired.
    let key = {
        let mut mac =
            hmac::Hmac::<sha2::Sha256>::new_from_slice(app.server.admin_secret().as_bytes())
                .unwrap();
        mac.update(format!("jazz-link-challenge-v1\0{}", app.app_id).as_bytes());
        mac.finalize().into_bytes().to_vec()
    };
    let signed = |exp: u64| {
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
            json!({ "v": 1, "sub": "pia", "nonce": jazz_app_support::ObjectId::new().to_string(), "exp": exp })
                .to_string(),
        );
        let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(&key).unwrap();
        mac.update(payload.as_bytes());
        let mac =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        format!("{payload}.{mac}")
    };
    let expired = signed(now() - 1);
    let response = link_as(&app, &app.endpoint, "pia", &seed, &expired, &expired).await;
    assert_eq!(
        (response.status, response.text_body().as_str()),
        (400, "invalid_link_challenge")
    );
    // Its fresh twin links: only the expiry refused it.
    let fresh = signed(now() + 60);
    let response = link_as(&app, &app.endpoint, "pia", &seed, &fresh, &fresh).await;
    assert_eq!(response.status, 200);
    app.stop().await;
}

/// A TS client's socket: its prelude as a text frame, shaped as TS's
/// `encodeWebSocketPrelude` writes it (auth repeated beside `auth`, with
/// `sub`), its bearer on the upgrade. It is admitted.
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

/// A client that names another app id (a TS client derives its own from
/// the endpoint URL) signs in and syncs: the endpoint serves its own app.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_naming_another_app_id_syncs_with_the_endpoints_app() {
    let app = App::start().await;
    let derived = AppId::random();
    assert_ne!(derived, app.app_id);
    let uma = Jazz::open(
        client_files(),
        Config::new(derived)
            .server(&app.url)
            .user(bearer("token-uma")),
    )
    .await
    .expect("sign in through the endpoint under another app id");
    let todo = uma
        .durable(DurabilityTier::GlobalServer)
        .insert(NewTodo::new("Under a derived id", false))
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
    uma.shutdown().await.unwrap();
    app.stop().await;
}

/// A thin client's socket through the endpoint, speaking the thin protocol
/// as `jazz-tools/thin` does.
struct ThinSocket {
    socket: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    next: u64,
}

impl ThinSocket {
    /// Sign `user` in (`token-<user>[:admin]`) and open their thin socket,
    /// bound to the app's schema.
    async fn open(app: &App, token: &str, user: &str) -> Self {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
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
            "user_id": user,
            "claims": {},
            "authMode": "external",
        });
        let peer = serde_json::to_string(&(account.as_str(), "jazz:backend", user)).unwrap();
        let prelude = json!({
            "peer_identity": peer,
            "auth": { "jwt_token": null, "backend_session": session },
        });
        let ws = app.url.replacen("http", "ws", 1) + &format!("/apps/{}/ws", app.app_id);
        let mut request = ws.into_client_request().unwrap();
        request
            .headers_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        socket
            .send(Message::Text(prelude.to_string().into()))
            .await
            .unwrap();
        let hello = jazz_app_support::jazz::wire::WireFrame::Hello(
            jazz_app_support::jazz::wire::WireHello::current(
                jazz_app_support::jazz::wire::WirePeerRole::Client,
                jazz_app_support::jazz::wire::FEATURE_THIN_RESULTS,
            ),
        );
        let hello = jazz_app_support::jazz::wire::encode_frame(&hello).unwrap();
        socket
            .send(Message::Binary(
                jazz_app_support::jazz::wire::encode_websocket_frame_batch(&[hello])
                    .unwrap()
                    .into(),
            ))
            .await
            .unwrap();
        let mut thin = Self { socket, next: 1 };
        let schema = app
            .backend
            .with_client(|client| async move { Ok(serde_json::to_vec(&client.schema()?).unwrap()) })
            .await
            .unwrap();
        assert_eq!(
            thin.call(jazz_app_support::jazz::thin::ThinOp::Schema { schema })
                .await,
            jazz_app_support::jazz::thin::ThinBody::Ok
        );
        thin
    }

    /// Send requests in one message, without waiting for their answers.
    async fn send_all(&mut self, ops: Vec<jazz_app_support::jazz::thin::ThinOp>) {
        use jazz_app_support::jazz::wire::{WireFrame, encode_frame, encode_websocket_frame_batch};
        let mut frames = Vec::new();
        for op in ops {
            let id = self.next;
            self.next += 1;
            frames.push(
                encode_frame(&WireFrame::ThinRequest(
                    jazz_app_support::jazz::thin::ThinRequest { id, op },
                ))
                .unwrap(),
            );
        }
        self.socket
            .send(Message::Binary(
                encode_websocket_frame_batch(&frames).unwrap().into(),
            ))
            .await
            .unwrap();
    }

    /// Send a request without waiting for its answer; its id.
    async fn send(&mut self, op: jazz_app_support::jazz::thin::ThinOp) -> u64 {
        use jazz_app_support::jazz::wire::{WireFrame, encode_frame, encode_websocket_frame_batch};
        let id = self.next;
        self.next += 1;
        let frame = encode_frame(&WireFrame::ThinRequest(
            jazz_app_support::jazz::thin::ThinRequest { id, op },
        ))
        .unwrap();
        self.socket
            .send(Message::Binary(
                encode_websocket_frame_batch(&[frame]).unwrap().into(),
            ))
            .await
            .unwrap();
        id
    }

    /// The answers to `ids`, by id, in whatever order they come.
    async fn answers(
        &mut self,
        ids: &[u64],
    ) -> std::collections::BTreeMap<u64, jazz_app_support::jazz::thin::ThinBody> {
        use jazz_app_support::jazz::wire::{WireFrame, decode_frame, decode_websocket_frame_batch};
        let mut answers = std::collections::BTreeMap::new();
        tokio::time::timeout(Duration::from_secs(20), async {
            while answers.len() < ids.len() {
                let Some(Ok(Message::Binary(message))) = self.socket.next().await else {
                    panic!("the socket ended before answering");
                };
                for frame in decode_websocket_frame_batch(&message).unwrap() {
                    if let Ok(WireFrame::ThinResponse(response)) = decode_frame(&frame)
                        && ids.contains(&response.id)
                    {
                        answers.insert(response.id, response.body);
                    }
                }
            }
        })
        .await
        .expect("answered");
        answers
    }

    async fn call(
        &mut self,
        op: jazz_app_support::jazz::thin::ThinOp,
    ) -> jazz_app_support::jazz::thin::ThinBody {
        use jazz_app_support::jazz::wire::{
            WireFrame, decode_frame, decode_websocket_frame_batch, encode_frame,
            encode_websocket_frame_batch,
        };
        let id = self.next;
        self.next += 1;
        let frame = encode_frame(&WireFrame::ThinRequest(
            jazz_app_support::jazz::thin::ThinRequest { id, op },
        ))
        .unwrap();
        self.socket
            .send(Message::Binary(
                encode_websocket_frame_batch(&[frame]).unwrap().into(),
            ))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let Some(Ok(Message::Binary(message))) = self.socket.next().await else {
                    panic!("the socket ended before answering {id}");
                };
                for frame in decode_websocket_frame_batch(&message).unwrap() {
                    if let Ok(WireFrame::ThinResponse(response)) = decode_frame(&frame)
                        && response.id == id
                    {
                        return response.body;
                    }
                }
            }
        })
        .await
        .expect("answered")
    }
}

fn cells(values: &[(&str, jazz_app_support::jazz::records::Value)]) -> Vec<u8> {
    use jazz_app_support::jazz::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
    let descriptor = RecordDescriptor::new(values.iter().map(|(name, value)| {
        (
            name.to_string(),
            match value {
                Value::Bool(_) => ValueType::Bool,
                _ => ValueType::String,
            },
        )
    }));
    let values: Vec<Value> = values.iter().map(|(_, value)| value.clone()).collect();
    let record = OwnedRecord::new(descriptor.create(&values).unwrap(), descriptor);
    jazz_app_support::jazz::binding_codec::encode_named_cells(&record).unwrap()
}

fn text(value: &str) -> jazz_app_support::jazz::records::Value {
    jazz_app_support::jazz::records::Value::String(value.to_owned())
}

fn rows_of(body: jazz_app_support::jazz::thin::ThinBody) -> usize {
    let jazz_app_support::jazz::thin::ThinBody::Rows { rows, .. } = body else {
        panic!("expected rows, got {body:?}");
    };
    jazz_app_support::jazz::binding_codec::decode_rows(&rows)
        .unwrap()
        .iter()
        .map(|batch| batch.rows.len())
        .sum()
}

/// A thin client's transaction runs in the backend, as its user, and the
/// core takes it only once committed; its other requests reach the core.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_thin_clients_transactions_run_in_the_backend_as_its_user() {
    use jazz_app_support::jazz::thin::{ThinBody, ThinOp};
    let app = App::start().await;
    let seeded = app
        .backend
        .durable(DurabilityTier::GlobalServer)
        .insert(NewTodo::new("seeded", false))
        .await
        .unwrap();
    let todo_query =
        postcard::to_allocvec(&todos::query().id(seeded.id).into_untyped().unwrap()).unwrap();
    let projects_query = postcard::to_allocvec(&projects::query().into_untyped().unwrap()).unwrap();

    let mut carol = ThinSocket::open(&app, "token-carol:admin", "carol").await;
    let tx = jazz_app_support::jazz::tools::OpenTransactionId::new().to_string();
    let in_tx = Some(tx.clone());
    assert_eq!(
        carol
            .call(ThinOp::Begin {
                transaction: tx.clone(),
                kind: "mergeable".into(),
            })
            .await,
        ThinBody::Ok
    );
    // A row only the core holds, read in the transaction.
    let read = carol
        .call(ThinOp::Query {
            query: todo_query.clone(),
            options: Vec::new(),
            transaction: in_tx.clone(),
        })
        .await;
    assert_eq!(rows_of(read), 1);
    let staged = carol
        .call(ThinOp::Update {
            table: "todos".into(),
            row: *seeded.id.uuid().as_bytes(),
            patch: cells(&[("title", text("done by carol"))]),
            options: Vec::new(),
            transaction: in_tx.clone(),
        })
        .await;
    assert!(matches!(staged, ThinBody::Staged { .. }), "{staged:?}");
    let staged = carol
        .call(ThinOp::Insert {
            table: "projects".into(),
            cells: cells(&[("name", text("carol's"))]),
            options: Vec::new(),
            transaction: in_tx.clone(),
        })
        .await;
    assert!(matches!(staged, ThinBody::Staged { .. }), "{staged:?}");
    // Nothing reached the core yet: a read outside the transaction is the
    // core's, as carol.
    assert_eq!(
        rows_of(
            carol
                .call(ThinOp::Query {
                    query: projects_query.clone(),
                    options: Vec::new(),
                    transaction: None,
                })
                .await
        ),
        0
    );
    let committed = carol.call(ThinOp::Commit { transaction: tx }).await;
    assert!(
        matches!(committed, ThinBody::Written { .. }),
        "{committed:?}"
    );
    // Committed means the core has it: carol's project (hers alone to
    // read), and the todo, as the backend sees it.
    assert_eq!(
        rows_of(
            carol
                .call(ThinOp::Query {
                    query: projects_query.clone(),
                    options: Vec::new(),
                    transaction: None,
                })
                .await
        ),
        1
    );
    let todo = app
        .backend
        .all_with(todos::query().id(seeded.id), ReadTier::Remote)
        .await
        .unwrap();
    assert_eq!(todo[0].title, "done by carol");

    // Dave is no admin: the core refuses his project when he commits, as
    // it would refuse dave himself, and nothing of the transaction applies.
    let mut dave = ThinSocket::open(&app, "token-dave", "dave").await;
    let tx = jazz_app_support::jazz::tools::OpenTransactionId::new().to_string();
    assert_eq!(
        dave.call(ThinOp::Begin {
            transaction: tx.clone(),
            kind: "mergeable".into(),
        })
        .await,
        ThinBody::Ok
    );
    dave.call(ThinOp::Insert {
        table: "todos".into(),
        cells: cells(&[
            ("title", text("dave's")),
            ("done", jazz_app_support::jazz::records::Value::Bool(false)),
        ]),
        options: Vec::new(),
        transaction: Some(tx.clone()),
    })
    .await;
    dave.call(ThinOp::Insert {
        table: "projects".into(),
        cells: cells(&[("name", text("not dave's to make"))]),
        options: Vec::new(),
        transaction: Some(tx.clone()),
    })
    .await;
    let refused = dave.call(ThinOp::Commit { transaction: tx }).await;
    assert!(matches!(refused, ThinBody::Error { .. }), "{refused:?}");
    let todos_now = app
        .backend
        .all_with(todos::query(), ReadTier::Remote)
        .await
        .unwrap();
    assert!(todos_now.iter().all(|todo| todo.title != "dave's"));

    app.stop().await;
}

/// Requests keep the client's order across the backend and the core: a
/// transaction reads a plain write sent just before it, unawaited, and a
/// plain read sent right after a commit sees the commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_thin_clients_pipelined_requests_keep_their_order() {
    use jazz_app_support::jazz::thin::{ThinBody, ThinOp};
    let app = App::start().await;
    let mut erin = ThinSocket::open(&app, "token-erin", "erin").await;
    let row = jazz_app_support::ObjectId::new();
    let by_id = postcard::to_allocvec(&todos::query().id(row).into_untyped().unwrap()).unwrap();
    let tx = jazz_app_support::jazz::tools::OpenTransactionId::new().to_string();
    let todo = |title: &str| {
        cells(&[
            ("title", text(title)),
            ("done", jazz_app_support::jazz::records::Value::Bool(false)),
        ])
    };
    let inserted = erin
        .send(ThinOp::Insert {
            table: "todos".into(),
            cells: todo("plain"),
            options: serde_json::to_vec(&json!({ "rowId": row.uuid().as_bytes() })).unwrap(),
            transaction: None,
        })
        .await;
    let begun = erin
        .send(ThinOp::Begin {
            transaction: tx.clone(),
            kind: "mergeable".into(),
        })
        .await;
    let read_in_tx = erin
        .send(ThinOp::Query {
            query: by_id.clone(),
            options: Vec::new(),
            transaction: Some(tx.clone()),
        })
        .await;
    let staged = erin
        .send(ThinOp::Update {
            table: "todos".into(),
            row: *row.uuid().as_bytes(),
            patch: cells(&[("title", text("in the transaction"))]),
            options: Vec::new(),
            transaction: Some(tx.clone()),
        })
        .await;
    let committed = erin.send(ThinOp::Commit { transaction: tx }).await;
    let read_after = erin
        .send(ThinOp::Query {
            query: by_id,
            options: Vec::new(),
            transaction: None,
        })
        .await;
    let answers = erin
        .answers(&[inserted, begun, read_in_tx, staged, committed, read_after])
        .await;
    assert!(
        matches!(answers[&inserted], ThinBody::Written { .. }),
        "{answers:?}"
    );
    assert_eq!(
        rows_of(answers[&read_in_tx].clone()),
        1,
        "the transaction reads the write before it"
    );
    assert!(
        matches!(answers[&committed], ThinBody::Written { .. }),
        "{answers:?}"
    );
    let ThinBody::Rows { rows, .. } = answers[&read_after].clone() else {
        panic!("{answers:?}");
    };
    let after = jazz_app_support::jazz::binding_codec::decode_rows(&rows).unwrap();
    assert_eq!(
        after[0].rows[0].cells["title"],
        text("in the transaction"),
        "the read after the commit sees it"
    );
    app.stop().await;
}

/// A thin session's transaction client ends with its socket, rolling back
/// what it left open, and only so many run at once: past the cap, a
/// transaction is refused with `backpressure`, to try again later.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_thin_sessions_transaction_client_ends_with_its_socket_and_is_capped() {
    use jazz_app_support::jazz::thin::{ThinBody, ThinOp};
    let app = App::start_with(|endpoint| endpoint.max_transaction_clients(1)).await;
    let begin = |tx: &str| ThinOp::Begin {
        transaction: tx.to_owned(),
        kind: "mergeable".into(),
    };
    let todo = |tx: &str, title: &str| ThinOp::Insert {
        table: "todos".into(),
        cells: cells(&[
            ("title", text(title)),
            ("done", jazz_app_support::jazz::records::Value::Bool(false)),
        ]),
        options: Vec::new(),
        transaction: Some(tx.to_owned()),
    };
    let new_tx = || jazz_app_support::jazz::tools::OpenTransactionId::new().to_string();

    let mut frank = ThinSocket::open(&app, "token-frank", "frank").await;
    let franks = new_tx();
    assert_eq!(frank.call(begin(&franks)).await, ThinBody::Ok);
    let staged = frank.call(todo(&franks, "left open")).await;
    assert!(matches!(staged, ThinBody::Staged { .. }), "{staged:?}");
    assert_eq!(app.endpoint.transaction_clients(), 1);

    let mut gina = ThinSocket::open(&app, "token-gina", "gina").await;
    let ginas = new_tx();
    let refused = gina.call(begin(&ginas)).await;
    assert!(
        matches!(&refused, ThinBody::Error { code, .. } if code == "backpressure"),
        "{refused:?}"
    );

    // Frank goes away mid-transaction: his client ends, and nothing of the
    // transaction reaches the core.
    frank.socket.close(None).await.unwrap();
    drop(frank);
    eventually(async || (app.endpoint.transaction_clients() == 0).then_some(())).await;
    let todos_now = app
        .backend
        .all_with(todos::query(), ReadTier::Remote)
        .await
        .unwrap();
    assert!(todos_now.iter().all(|todo| todo.title != "left open"));

    // Room again: gina's transaction goes through.
    let ginas = new_tx();
    assert_eq!(gina.call(begin(&ginas)).await, ThinBody::Ok);
    gina.call(todo(&ginas, "gina's")).await;
    let committed = gina.call(ThinOp::Commit { transaction: ginas }).await;
    assert!(
        matches!(committed, ThinBody::Written { .. }),
        "{committed:?}"
    );
    app.stop().await;
}

/// A thin socket whose requests wait on an answer past the deadline is
/// closed with 1011: here a plain read waits on a transaction's first
/// request, which the session's client takes longer to answer than the
/// (shortened) deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_thin_socket_whose_answer_never_comes_is_closed() {
    use jazz_app_support::jazz::thin::ThinOp;
    let app = App::start_with(|endpoint| endpoint.answer_deadline(Duration::from_millis(1))).await;
    let mut hana = ThinSocket::open(&app, "token-hana", "hana").await;
    let tx = jazz_app_support::jazz::tools::OpenTransactionId::new().to_string();
    // In one message, so the read is routed behind the transaction's first
    // request before that is answered.
    hana.send_all(vec![
        ThinOp::Begin {
            transaction: tx,
            kind: "mergeable".into(),
        },
        ThinOp::Query {
            query: postcard::to_allocvec(&todos::query().into_untyped().unwrap()).unwrap(),
            options: Vec::new(),
            transaction: None,
        },
    ])
    .await;
    let closed = tokio::time::timeout(Duration::from_secs(20), async {
        while let Some(Ok(message)) = hana.socket.next().await {
            if let Message::Close(frame) = message {
                return frame;
            }
        }
        None
    })
    .await
    .expect("the socket ends");
    let frame = closed.expect("a close frame");
    assert_eq!(u16::from(frame.code), 1011, "{frame:?}");
    assert_eq!(frame.reason.as_str(), "an answer never came");
    app.stop().await;
}

/// #5927 R3b: the endpoint refuses a sign-in right after the prelude, so it
/// reads the Hello the client sends behind it first, and names why it
/// refused to a client that offered `FEATURE_TYPED_ERROR_REASON`. A client
/// without the bit gets the plain `Error` bytes and reads the message.
///
/// ```text
/// client ─prelude (no one), Hello{+typed reasons}─► endpoint ─Error{missing}─► client
/// client ─prelude (no one), Hello─────────────────► endpoint ─Error──────────► client
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_sign_in_names_its_reason_to_a_client_that_reads_one() {
    use jazz::wire::{
        FEATURE_TYPED_ERROR_REASON, WireErrorCode, WireErrorReason, WireFrame, WireHello,
        WirePeerRole, current_wire_features, decode_frame, decode_frame_with_error_reason,
        decode_websocket_frame_batch, encode_frame, encode_websocket_frame_batch,
    };
    let app = App::start().await;
    let refusal = async |features: u64| {
        let prelude = serde_json::to_vec(&json!({ "auth": {} })).unwrap();
        let hello = encode_websocket_frame_batch(&[encode_frame(&WireFrame::Hello(
            WireHello::current(WirePeerRole::Client, features),
        ))
        .unwrap()])
        .unwrap();
        let incoming =
            futures::stream::iter(vec![prelude, hello]).chain(futures::stream::pending());
        let (sent, mut received) = futures::channel::mpsc::unbounded::<Vec<u8>>();
        let closed = app
            .endpoint
            .serve_socket(
                Request::new("GET", app.path("ws")),
                Box::pin(incoming),
                sent.sink_map_err(|_| ()),
            )
            .await;
        assert_eq!(closed.code, 1008, "{closed:?}");
        let batch = received.next().await.expect("a refusal");
        decode_websocket_frame_batch(&batch).unwrap().remove(0)
    };
    let named = refusal(current_wire_features() | FEATURE_TYPED_ERROR_REASON).await;
    let Ok(WireFrame::Error(error)) = decode_frame_with_error_reason(&named) else {
        panic!("an error frame")
    };
    assert_eq!(error.code, WireErrorCode::AuthFailed);
    assert_eq!(error.reason, Some(WireErrorReason::Missing));

    let plain = refusal(current_wire_features()).await;
    let Ok(WireFrame::Error(error)) = decode_frame(&plain) else {
        panic!("a plain error frame")
    };
    assert_eq!(error.reason, None);
    assert!(error.message.contains("missing"), "{}", error.message);

    // A signed-in user's socket, with the bit: the first error it gets.
    let account = app.endpoint.create_account_for("jo").await.unwrap();
    let peer = jazz::ids::AuthorSubject::authenticated("jazz:backend", "jo")
        .unwrap()
        .with_account(account)
        .canonical()
        .to_owned();
    let ws_path = app.path("ws");
    let first_error = async |endpoint: Endpoint| {
        let ws_path = ws_path.clone();
        let prelude = serde_json::to_vec(&json!({
            "peer_identity": peer,
            "auth": { "backend_session": { "user_id": "jo" } },
        }))
        .unwrap();
        let hello =
            encode_websocket_frame_batch(&[encode_frame(&WireFrame::Hello(WireHello::current(
                WirePeerRole::Client,
                current_wire_features() | FEATURE_TYPED_ERROR_REASON,
            )))
            .unwrap()])
            .unwrap();
        let incoming =
            futures::stream::iter(vec![prelude, hello]).chain(futures::stream::pending());
        let (sent, received) = futures::channel::mpsc::unbounded::<Vec<u8>>();
        let serving = tokio::spawn(async move {
            endpoint
                .serve_socket(
                    Request::new("GET", &ws_path).header("authorization", "Bearer token-jo"),
                    Box::pin(incoming),
                    sent.sink_map_err(|_| ()),
                )
                .await
        });
        let error = tokio::time::timeout(
            Duration::from_secs(10),
            received
                .filter_map(async |batch| {
                    let frame = decode_websocket_frame_batch(&batch).ok()?.remove(0);
                    match decode_frame_with_error_reason(&frame) {
                        Ok(WireFrame::Error(error)) => Some(error),
                        _ => None,
                    }
                })
                .boxed()
                .next(),
        )
        .await
        .expect("an error in time")
        .expect("an error");
        serving.abort();
        error
    };
    // A session whose deadline passed by its admission (a zero TTL).
    let error = first_error(app.endpoint.clone().session_ttl(Duration::ZERO)).await;
    assert_eq!(error.reason, Some(WireErrorReason::Expired), "{error:?}");
    // The planned reconnect before a short session ends.
    let error = first_error(app.endpoint.clone().session_ttl(Duration::from_secs(2))).await;
    assert_eq!(
        error.reason,
        Some(WireErrorReason::Reauthenticate),
        "{error:?}"
    );
    app.stop().await;
}

/// A revoke that races alice's sign-in refuses her session: the endpoint
/// states when her `authenticate` started, not when the core admitted her.
///
/// ```text
/// alice ──socket──► endpoint ──authenticate starts─┐
/// backend ──revoke-sessions──► core                │ (mark is newer)
/// core ◄──authenticatedAt: started──── endpoint ◄──┘ ──✗── refused
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_revoke_that_races_a_sign_in_refuses_the_session() {
    use jazz_app_support::jazz::wire::{
        WireFrame, WireHello, WirePeerRole, decode_frame, decode_websocket_frame_batch,
        encode_frame, encode_websocket_frame_batch,
    };
    use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;

    let mut app = App::start().await;
    let account = app.endpoint.create_account_for("alice").await.unwrap();
    // `authenticate` runs twice for the socket: for its admission, and for
    // this file's host, which logs who was admitted. Hold both until the
    // revoke is done, whichever is which.
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let held = {
        let (entered, release) = (entered.clone(), release.clone());
        move |request: Request| {
            let (entered, release) = (entered.clone(), release.clone());
            async move {
                let answer = authenticate(request).await;
                entered.add_permits(1);
                let _ = release.acquire().await;
                answer
            }
        }
    };
    app.endpoint = Endpoint::new(&app.backend, held).unwrap();
    app.host.abort();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    app.url = format!("http://{}/jazz", listener.local_addr().unwrap());
    app.host = tokio::spawn(host(listener, app.endpoint.clone(), app.admitted.clone()));

    let ws = app.url.replacen("http", "ws", 1) + &format!("/apps/{}/ws", app.app_id);
    let mut request = ws.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("authorization", "Bearer token-alice".parse().unwrap());
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    let account = account.0.to_string();
    let peer = serde_json::to_string(&(account.as_str(), "jazz:backend", "alice")).unwrap();
    let prelude = json!({
        "peer_identity": peer,
        "auth": { "backend_session": { "user_id": "alice" } },
    });
    socket
        .send(Message::Text(prelude.to_string().into()))
        .await
        .unwrap();
    let hello = WireFrame::Hello(WireHello::current(WirePeerRole::Client, 0));
    let hello = encode_websocket_frame_batch(&[encode_frame(&hello).unwrap()]).unwrap();
    socket.send(Message::Binary(hello.into())).await.unwrap();

    let both = tokio::time::timeout(Duration::from_secs(10), entered.acquire_many(2)).await;
    both.expect("authenticate ran for both").unwrap().forget();
    // The mark refuses only what is strictly older.
    tokio::time::sleep(Duration::from_millis(5)).await;
    let revoked_before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let revoked = reqwest::Client::new()
        .post(format!(
            "{}/apps/{}/admin/accounts/revoke-sessions",
            app.server.base_url(),
            app.app_id
        ))
        .header("X-Jazz-Admin-Secret", app.server.admin_secret())
        .json(&json!({
            "identity": { "issuer": "jazz:backend", "subject": "alice" },
            "revokedBefore": revoked_before,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(revoked.status().as_u16(), 200);
    release.close();

    let refusal = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(Ok(message)) = socket.next().await {
            let Message::Binary(message) = message else {
                continue;
            };
            for frame in decode_websocket_frame_batch(&message).unwrap() {
                if let Ok(WireFrame::Error(error)) = decode_frame(&frame) {
                    return Some(error.message);
                }
            }
        }
        None
    })
    .await
    .expect("the core refuses the raced session");
    assert!(
        refusal
            .as_deref()
            .is_some_and(|message| message.contains("revoked")),
        "{refusal:?}"
    );
    app.stop().await;
}
