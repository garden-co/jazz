//! A backend's `/jazz` endpoint (jazz-app's [`jazz_app::Endpoint`], TS
//! `createJazz`'s handler) for hosts whose own HTTP server serves it: the
//! host passes each request in and writes the response out, upgrades the
//! socket path itself and pumps its WebSocket's binary messages through a
//! [`SocketCore`], and answers "who is this request's user" from its own
//! sign-in through a callback.
//!
//! Requests and responses are JSON: `{ "method", "path", "headers": [[name,
//! value], ...], "body"?: "<base64>" }` and `{ "status", "headers", "body":
//! "<base64>" }`. The host's `authenticate` gets a request's JSON and returns
//! `{ "kind": "user", "userId", "claims"?, "expiresAt" }`, `{ "kind":
//! "deviceAccount" }` or nothing (no one).

use std::sync::{Arc, Mutex};

use base64::Engine as _;
use futures_executor::block_on;
use futures_util::StreamExt as _;
use jazz_app::endpoint::{Answer, Closed, Endpoint, Request, Response};
use serde_json::{Value as Json, json};

use crate::core::Core;
use crate::error::{FfiError, Result, refused};

const BASE64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

/// A host's `authenticate`: the request's JSON in, the answer's JSON (or
/// none) out. Called on a thread of its own: it may block.
pub(crate) type HostAuthFn = dyn Fn(&str) -> Option<String> + Send + Sync;

pub(crate) fn request(json: &str) -> Result<Request> {
    let json: Json = serde_json::from_str(json)
        .map_err(|e| refused(format!("the request is not valid JSON: {e}")))?;
    let text = |key: &str| {
        json.get(key)
            .and_then(Json::as_str)
            .map(str::to_owned)
            .ok_or_else(|| refused(format!("the request needs \"{key}\"")))
    };
    let mut request = Request::new(text("method")?, text("path")?);
    for header in json
        .get("headers")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
    {
        let pair = header.as_array().filter(|pair| pair.len() == 2);
        let (Some(name), Some(value)) = (
            pair.and_then(|p| p[0].as_str()),
            pair.and_then(|p| p[1].as_str()),
        ) else {
            return Err(refused("request.headers are [name, value] pairs"));
        };
        request = request.header(name, value);
    }
    if let Some(body) = json.get("body").and_then(Json::as_str) {
        request.body = BASE64
            .decode(body)
            .map_err(|_| refused("request.body must be base64"))?;
    }
    Ok(request)
}

fn request_json(request: &Request) -> String {
    json!({
        "method": request.method,
        "path": request.path,
        "headers": request.headers,
        "body": BASE64.encode(&request.body),
    })
    .to_string()
}

fn response_json(response: Response) -> String {
    json!({
        "status": response.status,
        "headers": response.headers,
        "body": BASE64.encode(&response.body),
    })
    .to_string()
}

pub(crate) struct EndpointCore {
    endpoint: Endpoint,
    jazz: jazz_app::Jazz,
    schema: Arc<jazz_app::jazz::tools::Schema>,
    relations: crate::convert::Relations,
    has_server: bool,
    contract: u64,
}

impl EndpointCore {
    /// The endpoint of `backend` (opened with `permissions`), at
    /// `base_path` (default `/jazz`).
    pub fn new(
        backend: &Core,
        authenticate: Arc<HostAuthFn>,
        base_path: Option<&str>,
        session_ttl_secs: Option<u64>,
    ) -> Result<Self> {
        let auth = move |request: Request| {
            let callback = authenticate.clone();
            let json = request_json(&request);
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let spawned = std::thread::Builder::new()
                .name("jazz-authenticate".into())
                .spawn(move || {
                    let answer =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback(&json)))
                            .unwrap_or(None);
                    let _ = sender.send(answer);
                });
            async move {
                spawned
                    .map_err(|e| jazz_app::Error::Config(format!("cannot authenticate: {e}")))?;
                let answer = receiver.await.map_err(|_| jazz_app::Error::Stopped)?;
                match answer {
                    None => Ok(None),
                    Some(text) => serde_json::from_str::<Answer>(&text)
                        .map(Some)
                        .map_err(|e| {
                            jazz_app::Error::Config(format!(
                                "authenticate's answer is not an answer: {e}"
                            ))
                        }),
                }
            }
        };
        let mut endpoint = Endpoint::new(backend.jazz()?, auth)?;
        if let Some(path) = base_path {
            endpoint = endpoint.base_path(path);
        }
        if let Some(secs) = session_ttl_secs {
            endpoint = endpoint.session_ttl(std::time::Duration::from_secs(secs));
        }
        let (jazz, schema, relations, has_server) = backend.parts();
        Ok(Self {
            endpoint,
            jazz,
            schema,
            relations,
            has_server,
            contract: backend.contract,
        })
    }

    /// Whether `path` is the socket path: the host upgrades it to a
    /// WebSocket and serves it with [`Self::socket`].
    pub fn is_socket(&self, path: &str) -> bool {
        self.endpoint.is_socket(path)
    }

    /// Answers an HTTP request (`request_json`) with a response's JSON.
    pub fn handle(&self, request_json: &str) -> Result<String> {
        let request = request(request_json)?;
        let endpoint = self.endpoint.clone();
        let response = block_on(
            self.jazz
                .with_client(move |_| async move { Ok(endpoint.handle(request).await) }),
        )?;
        Ok(response_json(response))
    }

    /// A handle acting as the request's user (TS `forReqUser`).
    pub fn for_user(&self, request_json: &str) -> Result<Core> {
        let request = request(request_json)?;
        let endpoint = self.endpoint.clone();
        let jazz = block_on(
            self.jazz
                .with_client(move |_| async move { endpoint.for_user(&request).await }),
        )?;
        Ok(Core::from_parts(
            jazz,
            self.schema.clone(),
            self.relations.clone(),
            self.has_server,
            self.contract,
        ))
    }

    /// Serves an upgraded socket: `on_message` gets each binary message for
    /// the client, `on_close` the code and reason to close it with (the
    /// last call). Feed the client's messages with [`SocketCore::push`].
    pub fn socket(
        &self,
        upgrade_json: &str,
        on_message: Box<dyn Fn(Vec<u8>) + Send>,
        on_close: Box<dyn FnOnce(u16, String) + Send>,
    ) -> Result<SocketCore> {
        let request = request(upgrade_json)?;
        let (incoming_sender, incoming) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        let (outgoing, mut outgoing_receiver) = futures_channel::mpsc::unbounded::<Vec<u8>>();
        let (closed_sender, closed) = tokio::sync::oneshot::channel::<Closed>();
        let endpoint = self.endpoint.clone();
        block_on(self.jazz.with_client(move |_| async move {
            tokio::task::spawn_local(async move {
                let incoming = futures_util::stream::unfold(incoming, |mut receiver| async move {
                    receiver.recv().await.map(|message| (message, receiver))
                });
                let closed = endpoint
                    .serve_socket(request, Box::pin(incoming), outgoing)
                    .await;
                let _ = closed_sender.send(closed);
            });
            Ok(())
        }))?;
        // Delivers on a thread of its own: messages, then the close.
        let thread = std::thread::Builder::new()
            .name("jazz-socket".into())
            .spawn(move || {
                block_on(async move {
                    while let Some(message) = outgoing_receiver.next().await {
                        on_message(message);
                    }
                    let closed = closed.await.unwrap_or(Closed {
                        code: 1011,
                        reason: "the app stopped".into(),
                    });
                    on_close(closed.code, closed.reason);
                })
            })
            .map_err(|e| FfiError::Io(format!("cannot start the socket's thread: {e}")))?;
        Ok(SocketCore {
            incoming: Mutex::new(Some(incoming_sender)),
            thread: Mutex::new(Some(thread)),
        })
    }
}

/// A socket being served: the host pushes the client's messages in.
pub(crate) struct SocketCore {
    incoming: Mutex<Option<tokio::sync::mpsc::UnboundedSender<Vec<u8>>>>,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl SocketCore {
    /// A binary message from the client.
    pub fn push(&self, message: Vec<u8>) -> Result<()> {
        match &*self.incoming.lock().unwrap_or_else(|e| e.into_inner()) {
            Some(sender) => sender
                .send(message)
                .map_err(|_| refused("the socket is closed")),
            None => Err(refused("the socket is closed")),
        }
    }

    /// The client's socket closed: the endpoint ends it; `on_close` follows.
    pub fn close(&self) {
        self.incoming
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
    }

    /// [`Self::close`], then waits for the last callback to return.
    pub fn close_and_wait(&self) {
        self.close();
        let thread = self.thread.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(thread) = thread
            && thread.thread().id() != std::thread::current().id()
        {
            let _ = thread.join();
        }
    }
}

impl Drop for SocketCore {
    fn drop(&mut self) {
        self.close();
    }
}
