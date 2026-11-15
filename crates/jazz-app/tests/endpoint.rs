//! A Rust backend serving its users' clients: the app's Jazz endpoint
//! ([`jazz_app::Endpoint`]) over a plain host server, with an in-process
//! core, the app's `authenticate` deciding who each request's user is, and
//! the published permissions enforced on what they do.

#[path = "bindings/todos.rs"]
mod bindings;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bindings::{NewProject, NewTodo, Project, Todo, projects, todos};
use futures::{SinkExt, StreamExt};
use jazz_app::endpoint::{Admitted, Answer, AuthUser, Endpoint, Request};
use jazz_app::{
    AppFiles, AppId, Auth, Backend, Config, DeviceKey, Error, Jazz, ReadTier, SessionState,
    TokenReason,
};
use jazz_server::JazzServer;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request as Upgrade, Response as Upgraded};

/// Anyone reads; only an admin (by their claims) makes projects.
const PERMISSIONS: &str = r#"{
  "format": 1,
  "tables": {
    "projects": { "read": "true", "insert": "session.claims.role = 'admin'" },
    "todos": { "read": "true", "insert": "true", "update": "true", "delete": "true" }
  }
}"#;

fn client_files() -> AppFiles {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/todos");
    AppFiles::new(std::fs::read(format!("{dir}/jazz.app.json")).unwrap(), None)
}

/// The app's sign-in: `Bearer token-<user>[:admin]` names a user (an admin
/// with `:admin`); a request without one may be a device account's.
fn authenticate(request: Request) -> impl Future<Output = jazz_app::Result<Option<Answer>>> {
    let answer = match request.bearer() {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signed_in_user_syncs_through_the_backend_under_their_permissions() {
    let app = App::start().await;

    let alice = Jazz::open(client_files(), app.user(bearer("token-alice:admin")))
        .await
        .expect("alice signs in through the endpoint");
    let SessionState::SignedIn { user } = alice.session().unwrap().state() else {
        panic!("signed in");
    };
    assert_eq!(user.subject, "alice");
    let project = alice.insert(NewProject::new("Launch")).await.unwrap();
    let todo = alice.insert(NewTodo::new("Ship it", false)).await.unwrap();

    // The writes reach the backend, through the relay, as alice's.
    let seen: Vec<Todo> = eventually(async || {
        let rows = app
            .backend
            .all_with(todos::query(), ReadTier::Remote)
            .await
            .unwrap();
        (!rows.is_empty()).then_some(rows)
    })
    .await;
    assert_eq!(seen, vec![todo]);
    let projects_seen: Vec<Project> = eventually(async || {
        let rows = app
            .backend
            .all_with(projects::query(), ReadTier::Remote)
            .await
            .unwrap();
        (!rows.is_empty()).then_some(rows)
    })
    .await;
    assert_eq!(projects_seen, vec![project]);
    assert!(
        app.admitted
            .lock()
            .unwrap()
            .contains(&Admitted::User("alice".into()))
    );

    // Bob is no admin: the core refuses his project, and his todo goes through.
    let bob = Jazz::open(client_files(), app.user(bearer("token-bob")))
        .await
        .expect("bob signs in");
    let _ = bob.insert(NewProject::new("Not bob's to make")).await;
    let bobs = bob.insert(NewTodo::new("Bob's todo", false)).await.unwrap();
    eventually(async || {
        let rows = app
            .backend
            .all_with(todos::query(), ReadTier::Remote)
            .await
            .unwrap();
        rows.contains(&bobs).then_some(())
    })
    .await;
    let names: Vec<String> = app
        .backend
        .all_with(projects::query(), ReadTier::Remote)
        .await
        .unwrap()
        .into_iter()
        .map(|project| project.name)
        .collect();
    assert_eq!(names, vec!["Launch".to_owned()]);

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

    let alice = app.endpoint.for_user(&as_alice).await.unwrap();
    assert_eq!(alice.account(), Some(account));
    alice.insert(NewProject::new("Alice's")).await.unwrap();
    let bob = app.endpoint.for_user(&as_bob).await.unwrap();
    // Bob's permissions refuse his project at the core; his todo goes through.
    let _ = bob.insert(NewProject::new("Bob's")).await;
    let bobs = bob.insert(NewTodo::new("Bob's todo", false)).await.unwrap();
    // A user's handle never stops the backend.
    bob.shutdown().await.unwrap();
    assert!(app.backend.all(todos::query()).await.is_ok());

    // Another user, through the endpoint, sees what the core took.
    let reader = Jazz::open(client_files(), app.user(bearer("token-carol")))
        .await
        .unwrap();
    eventually(async || {
        let rows = reader
            .all_with(todos::query(), ReadTier::Remote)
            .await
            .unwrap();
        rows.contains(&bobs).then_some(())
    })
    .await;
    let names: Vec<String> = eventually(async || {
        let rows = reader
            .all_with(projects::query(), ReadTier::Remote)
            .await
            .unwrap();
        (!rows.is_empty()).then(|| rows.into_iter().map(|project| project.name).collect())
    })
    .await;
    assert_eq!(names, vec!["Alice's".to_owned()]);
    reader.shutdown().await.unwrap();
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
