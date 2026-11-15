//! jazz-ffi v2: a backend serves its app's `/jazz` endpoint through the
//! host's own HTTP server (here a tiny one in the test), with the host's
//! sign-in as `Authenticator`; an end user signs in through it.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use common::*;
use futures::{SinkExt, StreamExt};
use jazz_ffi::{Authenticator, Endpoint, JazzHandle, SocketListener, TokenProvider};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request as Upgrade, Response as Upgraded};

const BASE64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

/// The host's sign-in: `Bearer token-<user>` is `<user>`.
struct Bearers;
impl Authenticator for Bearers {
    fn authenticate(&self, request_json: String) -> Option<String> {
        let request = parse(&request_json);
        let bearer = request["headers"].as_array()?.iter().find_map(|h| {
            (h[0].as_str()?.eq_ignore_ascii_case("authorization"))
                .then(|| h[1].as_str().map(str::to_owned))
                .flatten()
        })?;
        let user = bearer.strip_prefix("Bearer token-")?;
        let expires = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        Some(json!({ "kind": "user", "userId": user, "expiresAt": expires }).to_string())
    }
}

struct Tokens(&'static str);
impl TokenProvider for Tokens {
    fn token(&self, _reason: String) -> Result<Option<String>, jazz_ffi::JazzError> {
        Ok(Some(format!("token-{}", self.0)))
    }
}

struct Pump {
    to_client: tokio::sync::mpsc::UnboundedSender<Option<Vec<u8>>>,
}
impl SocketListener for Pump {
    fn on_message(&self, message: Vec<u8>) {
        let _ = self.to_client.send(Some(message));
    }
    fn on_close(&self, _code: u16, _reason: String) {
        let _ = self.to_client.send(None);
    }
}

fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> T + Send + 'static,
) -> tokio::task::JoinHandle<T> {
    tokio::task::spawn_blocking(f)
}

/// One connection of the host's server: an HTTP request through
/// `Endpoint::handle`, or a WebSocket through `Endpoint::socket`.
async fn serve(stream: TcpStream, endpoint: Arc<Endpoint>) {
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
        let seen = Arc::new(Mutex::new(None::<Value>));
        let upgrade = seen.clone();
        let Ok(socket) = tokio_tungstenite::accept_hdr_async(stream, move |request: &Upgrade, response: Upgraded| {
            let headers: Vec<_> = request
                .headers()
                .iter()
                .map(|(k, v)| json!([k.to_string(), v.to_str().unwrap_or_default()]))
                .collect();
            *upgrade.lock().unwrap() =
                Some(json!({ "method": "GET", "path": request.uri().to_string(), "headers": headers }));
            Ok(response)
        })
        .await
        else {
            return;
        };
        let request = seen.lock().unwrap().take().unwrap().to_string();
        let (mut sink, mut incoming) = socket.split();
        let (to_client, mut from_endpoint) = tokio::sync::mpsc::unbounded_channel();
        let served = {
            let endpoint = endpoint.clone();
            blocking(move || {
                endpoint
                    .socket(request, Arc::new(Pump { to_client }))
                    .unwrap()
            })
            .await
            .unwrap()
        };
        let pushing = served.clone();
        let read = async move {
            while let Some(Ok(message)) = incoming.next().await {
                match message {
                    Message::Binary(data) => {
                        if pushing.push(data.to_vec()).is_err() {
                            break;
                        }
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            pushing.end();
        };
        let write = async move {
            while let Some(Some(data)) = from_endpoint.recv().await {
                if sink.send(Message::Binary(data.into())).await.is_err() {
                    break;
                }
            }
            let _ = sink.close().await;
        };
        futures::future::join(read, write).await;
        return;
    }
    let mut stream = stream;
    let end = text.find("\r\n\r\n").unwrap() + 4;
    let mut consumed = vec![0; end];
    stream.read_exact(&mut consumed).await.unwrap();
    let mut lines = text[..end].lines();
    let mut start = lines.next().unwrap_or_default().split_whitespace();
    let (method, path) = (
        start.next().unwrap_or_default().to_owned(),
        start.next().unwrap_or_default().to_owned(),
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
    let request =
        json!({ "method": method, "path": path, "headers": headers, "body": BASE64.encode(&body) })
            .to_string();
    let response = blocking(move || parse(&endpoint.handle(request).unwrap()))
        .await
        .unwrap();
    let body = BASE64.decode(response["body"].as_str().unwrap()).unwrap();
    let mut out = format!("HTTP/1.1 {} X\r\n", response["status"]);
    for header in response["headers"].as_array().unwrap() {
        out.push_str(&format!(
            "{}: {}\r\n",
            header[0].as_str().unwrap(),
            header[1].as_str().unwrap()
        ));
    }
    out.push_str(&format!(
        "content-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    ));
    let _ = stream.write_all(out.as_bytes()).await;
    let _ = stream.write_all(&body).await;
}

/// alice signs in with her app bearer through the backend's own `/jazz`
/// endpoint (served by the host through jazz-ffi), writes a show she chiefs
/// and syncs it; the backend acting as her (`for_user`) reads it.
///
/// ```
/// alice ──bearer──► host HTTP/WS ──Endpoint (jazz-ffi)──► core
///                         └── Authenticator("token-alice") = alice
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backend_serves_its_users_through_its_own_endpoint() {
    let app_id = jazz_app::AppId::random();
    let server = jazz_server::JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .unwrap();
    let backend_config = backend_config(&server, &app_id.to_string(), "stage-plan");
    let (backend, endpoint) = blocking(move || {
        let backend = JazzHandle::open(backend_config).unwrap();
        let endpoint = backend
            .endpoint(Arc::new(Bearers), None, None, None)
            .unwrap();
        (backend, endpoint)
    })
    .await
    .unwrap();
    assert!(
        endpoint.is_socket("/jazz/apps/x/ws".into()) || !endpoint.is_socket("/jazz/nope".into())
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/jazz", listener.local_addr().unwrap());
    let serving = {
        let endpoint = endpoint.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(serve(stream, endpoint.clone()));
            }
        })
    };

    let config = json!({ "app": STAGE_PLAN_APP, "appId": app_id.to_string(), "serverUrl": url,
                         "auth": { "bearer": true }, "contract": 2 })
    .to_string();
    let result = blocking(move || {
        let alice = JazzHandle::open_with_tokens(config, Arc::new(Tokens("alice"))).unwrap();
        let state = parse(&alice.session_state().unwrap().unwrap());
        assert_eq!(state["status"], "signedIn", "{state}");
        assert_eq!(state["user"]["subject"], "alice");
        let account = state["user"]["account"].as_str().unwrap().to_owned();
        let show_id = parse(&alice.insert("shows".into(), show(&account)).unwrap())["id"]
            .as_str()
            .unwrap()
            .to_owned();
        alice.wait_for_sync("global".into()).unwrap();

        // The backend acting as alice, from a request bearing her token.
        let request = json!({ "method": "GET", "path": "/", "headers": [["authorization", "Bearer token-alice"]] })
            .to_string();
        let as_alice = endpoint.for_user(request).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let shows = loop {
            let shows = parse(&as_alice.all_at(json!({ "table": "shows" }).to_string(), "remote".into()).unwrap());
            if !shows.as_array().unwrap().is_empty() || std::time::Instant::now() > deadline {
                break shows;
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        assert_eq!(shows[0]["id"], show_id, "{shows}");

        // A request without a bearer has no user.
        let nobody = endpoint.for_user(json!({ "method": "GET", "path": "/", "headers": [] }).to_string());
        assert!(nobody.is_err());
        as_alice.shutdown().unwrap();
        endpoint.shutdown().unwrap();
        alice.shutdown().unwrap();
        backend.shutdown().unwrap();
    })
    .await;
    serving.abort();
    server.shutdown().await;
    result.unwrap();
}

/// The host's sign-in says alice, but her credential expired an hour ago.
struct Expired;
impl Authenticator for Expired {
    fn authenticate(&self, _request_json: String) -> Option<String> {
        let past = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 3600;
        Some(json!({ "kind": "user", "userId": "alice", "expiresAt": past }).to_string())
    }
}

/// An `authenticate` answer that has expired is no one on every path: the
/// sign-in route answers 401 without the account, and the backend can't act
/// as that user.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expired_answer_is_no_one() {
    let app_id = jazz_app::AppId::random();
    let server = jazz_server::JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .unwrap();
    let config = backend_config(&server, &app_id.to_string(), "stage-plan");
    let app = app_id.to_string();
    let result = blocking(move || {
        let backend = JazzHandle::open(config).unwrap();
        let endpoint = backend.endpoint(Arc::new(Expired), None, None, Some(8)).unwrap();
        let request = json!({
            "method": "POST",
            "path": format!("/jazz/apps/{app}/accounts/backend-sign-in"),
            "headers": [["authorization", "Bearer token-alice"], ["content-type", "application/json"]],
            "body": BASE64.encode(b"{}"),
        })
        .to_string();
        let response = parse(&endpoint.handle(request.clone()).unwrap());
        assert_eq!(response["status"], 401, "{response}");
        let body = String::from_utf8(BASE64.decode(response["body"].as_str().unwrap()).unwrap()).unwrap();
        assert!(!body.contains("account"), "{body}");
        assert!(endpoint.for_user(request).is_err());
        endpoint.shutdown().unwrap();
        backend.shutdown().unwrap();
    })
    .await;
    server.shutdown().await;
    result.unwrap();
}

/// A socket event: a message for the client, or its close.
type SocketEvent = (Option<Vec<u8>>, Option<(u16, String)>);

struct Recorder(std::sync::mpsc::Sender<SocketEvent>);
impl SocketListener for Recorder {
    fn on_message(&self, message: Vec<u8>) {
        let _ = self.0.send((Some(message), None));
    }
    fn on_close(&self, code: u16, reason: String) {
        let _ = self.0.send((None, Some((code, reason))));
    }
}

/// A socket whose opening message names credentials anywhere but its
/// `auth` (here: an empty `auth` beside a top-level token) gets no session:
/// the endpoint answers it as not signed in and closes it with 1008.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_prelude_with_credentials_outside_auth_gets_no_session() {
    let app_id = jazz_app::AppId::random();
    let server = jazz_server::JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .unwrap();
    let config = backend_config(&server, &app_id.to_string(), "stage-plan");
    let app = app_id.to_string();
    let result = blocking(move || {
        let backend = JazzHandle::open(config).unwrap();
        let endpoint = backend
            .endpoint(Arc::new(Bearers), None, None, None)
            .unwrap();
        let path = format!("/jazz/apps/{app}/ws");
        assert!(endpoint.is_socket(path.clone()));
        let (sender, events) = std::sync::mpsc::channel();
        let upgrade = json!({ "method": "GET", "path": path, "headers": [] }).to_string();
        let socket = endpoint
            .socket(upgrade, Arc::new(Recorder(sender)))
            .unwrap();
        let prelude = json!({ "peer_identity": "someone", "jwt_token": "a.b.c", "auth": {} });
        socket.push(serde_json::to_vec(&prelude).unwrap()).unwrap();
        let mut closed = None;
        while let Ok((_, close)) = events.recv_timeout(Duration::from_secs(10)) {
            if close.is_some() {
                closed = close;
                break;
            }
        }
        let (code, _) = closed.expect("the socket is closed");
        assert_eq!(code, 1008);
        socket.end();
        endpoint.shutdown().unwrap();
        backend.shutdown().unwrap();
    })
    .await;
    server.shutdown().await;
    result.unwrap();
}
