//! A stand-in for an app backend's Jazz endpoint (`createJazz`'s handler
//! at `/jazz`), as far as an end user's client meets it: `backend-sign-in`,
//! and the sync socket, whose prelude it admits and relays to the core the
//! way `admitPrelude` does (`packages/jazz-tools/src/backend/create-jazz.ts`).
//!
//! Its `authenticate` reads `Authorization: Bearer token-<user>`.

use std::sync::{Arc, Mutex};

use futures::{SinkExt, StreamExt};
use jazz_app::AppId;
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
    bundle: String,
}

pub struct Relay {
    /// The app's Jazz endpoint, as a client's `Config::server`.
    pub endpoint: String,
    /// Sockets admitted so far, by who they were for.
    pub admitted: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub async fn start(core: &str, app: AppId, backend_secret: &str, bundle: &str) -> Relay {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/jazz", listener.local_addr().unwrap());
    let backend = Backend {
        core: core.trim_end_matches('/').to_owned(),
        app,
        backend_secret: backend_secret.to_owned(),
        bundle: bundle.to_owned(),
    };
    let admitted = Arc::new(Mutex::new(Vec::new()));
    let log = admitted.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(serve(stream, backend.clone(), log.clone()));
        }
    });
    Relay {
        endpoint,
        admitted,
        task,
    }
}

/// `authenticate`: the user a bearer names.
fn authenticate(authorization: Option<&str>) -> Option<String> {
    let user = authorization?.strip_prefix("Bearer token-")?;
    (!user.is_empty()).then(|| user.to_owned())
}

async fn serve(stream: TcpStream, backend: Backend, admitted: Arc<Mutex<Vec<String>>>) {
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
        relay_socket(stream, backend, admitted).await;
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
    let (status, body) = if path.ends_with("/accounts/backend-sign-in") {
        match authenticate(header("authorization").as_deref()) {
            None => ("401 Unauthorized", "signed_out".to_owned()),
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
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
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

async fn relay_socket(stream: TcpStream, backend: Backend, admitted: Arc<Mutex<Vec<String>>>) {
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
    let Some((prelude, who)) = admit(&backend, &prelude, authorization.as_deref()).await else {
        let _ = client.close(None).await;
        return;
    };
    admitted.lock().unwrap().push(who);
    let url = format!(
        "{}/apps/{}/ws",
        backend.core.replacen("http", "ws", 1),
        backend.app
    );
    let Ok((mut core, _)) = tokio_tungstenite::connect_async(url).await else {
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
        () = up => {}
        () = down => {}
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
