//! The HTTP stream transport: a WebSocket-shaped session over plain
//! requests, for main backends (TS `createJazz`) on hosts that can't upgrade
//! a socket. The Rust side of `http-stream-socket.ts` in `jazz-tools`.
//!
//! - `POST <base>/apps/<appId>/stream` opens a session. Its response body is
//!   a stream of records; the first carries the session id and key.
//! - `POST <base>/apps/<appId>/stream/<session>` passes messages to the core,
//!   as records in the request body, in order, one send at a time.
//! - `DELETE <base>/apps/<appId>/stream/<session>` ends the session.
//!
//! A record is `[kind u8][length u32 BE][payload]`. Each message record
//! carries what a WebSocket message would, so the session runs the same
//! prelude, hellos and pump as a socket does (see [`super::Carrier`]).
//!
//! [`HttpStreamFallbackConnector`] picks the transport as TS's
//! `httpStreamFallbackWebSocket` does: a socket first, and the stream once a
//! socket failed before opening and the backend says it serves the stream
//! instead, or keeps failing upgrades it says it accepts.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::channel::mpsc as fmpsc;
use futures::{Sink, Stream, StreamExt as _};
use jazz::tools::native_transport_connector::{
    ConnectedNativeTransport, NativeTransportConnector, NativeTransportError,
    NativeTransportFuture, NativeTransportRequest,
};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;

use super::{
    LinkActivity, Liveness, LivenessPolicy, NativeConnectFailure, Session,
    WS_CLIENT_HANDSHAKE_TIMEOUT, WS_CLIENT_LIVENESS, WebSocketClientError, WebSocketTransport,
    native_connect_error, native_transport_error, open_client_websocket, ws_url,
};

type WsError = tokio_tungstenite::tungstenite::Error;

pub(crate) const RECORD_SESSION: u8 = 0;
pub(crate) const RECORD_MESSAGE: u8 = 1;
pub(crate) const RECORD_CLOSE: u8 = 2;
/// Header carrying the session's key, which the session record hands over.
const STREAM_KEY_HEADER: &str = "x-jazz-stream-key";
/// Header on createJazz's 426 for a socket path: the endpoint serves the stream.
const STREAM_AVAILABLE_HEADER: &str = "x-jazz-stream";
/// The most one send request carries, unless a single record is larger.
const MAX_SEND_BYTES: usize = 4 * 1024 * 1024;
/// The largest session record the client reads (an id and a key).
const MAX_SESSION_RECORD_BYTES: usize = 4 * 1024;
/// The largest record the client buffers: one wire batch.
const MAX_RECORD_BYTES: usize = jazz::protocol_limits::MAX_WIRE_FRAME_BYTES + 64;
/// How long one send, or the DELETE that ends a session, may take. A backend
/// answers a send once the core has taken its records; one that answers
/// nothing for this long is gone.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// TCP keepalive on the stream's connections: first probe after this idle time,
/// then every interval, giving up after the retries (about 60 s in all).
const TCP_KEEPALIVE_IDLE: Duration = Duration::from_secs(30);
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const TCP_KEEPALIVE_RETRIES: u32 = 3;
/// How long the probe of a socket path may take.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
/// Received messages buffered ahead of the pump; the response body is not
/// read further until it takes them.
const INCOMING_SLOTS: usize = 16;
/// Messages queued for sending ahead of the send requests.
const OUTGOING_SLOTS: usize = 16;

/// How many failed sockets in a row, to a backend that answers and says it
/// upgrades them, mean they never get through (as TS's
/// `UPGRADE_FAILURES_BEFORE_STREAM`): past a restart or a draining instance,
/// which fail a socket once or twice and then let it through.
pub const UPGRADE_FAILURES_BEFORE_STREAM: u32 = 3;
/// How many sockets in a row that got no answer at all, to such a backend,
/// mean upgrades never get through (TS's `UPGRADE_HANGS_BEFORE_STREAM`). Two,
/// not one: a scale-to-zero host's edge holds the first upgrade after idle.
pub const UPGRADE_HANGS_BEFORE_STREAM: u32 = 2;

/// The stream's silence check: none. A backend sends nothing on an idle
/// stream session (it relays the core's messages, not its pings), so the
/// socket's silence deadline would end every idle session. The session ends
/// when its response ends or a send fails, and every send has a deadline
/// ([`REQUEST_TIMEOUT`]).
const HTTP_STREAM_LIVENESS: LivenessPolicy = LivenessPolicy {
    silence_deadline: Duration::MAX,
    tick: WS_CLIENT_LIVENESS.tick,
};

pub(crate) fn encode_record(kind: u8, payload: &[u8]) -> Vec<u8> {
    let length = u32::try_from(payload.len()).expect("a record fits u32");
    let mut record = Vec::with_capacity(5 + payload.len());
    record.push(kind);
    record.extend_from_slice(&length.to_be_bytes());
    record.extend_from_slice(payload);
    record
}

/// Splits a byte stream into records as it arrives.
#[derive(Default)]
pub(crate) struct RecordReader {
    buffered: Vec<u8>,
}

impl RecordReader {
    pub(crate) fn push(&mut self, chunk: &[u8]) -> Vec<(u8, Vec<u8>)> {
        self.buffered.extend_from_slice(chunk);
        let mut records = Vec::new();
        let mut offset = 0;
        while self.buffered.len() - offset >= 5 {
            let header = &self.buffered[offset..offset + 5];
            let length = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
            if self.buffered.len() - offset - 5 < length {
                break;
            }
            records.push((
                header[0],
                self.buffered[offset + 5..offset + 5 + length].to_vec(),
            ));
            offset += 5 + length;
        }
        self.buffered.drain(..offset);
        records
    }

    /// The length the unfinished record says it has, once its header is in.
    fn pending_length(&self) -> Option<usize> {
        (self.buffered.len() >= 5).then(|| {
            u32::from_be_bytes([
                self.buffered[1],
                self.buffered[2],
                self.buffered[3],
                self.buffered[4],
            ]) as usize
        })
    }
}

/// `ws(s)://…/apps/<id>/ws` → `http(s)://…/apps/<id>/<path>`.
fn http_url(socket_url: &str, path: &str) -> String {
    let url = if let Some(rest) = socket_url.strip_prefix("wss://") {
        format!("https://{rest}")
    } else if let Some(rest) = socket_url.strip_prefix("ws://") {
        format!("http://{rest}")
    } else {
        socket_url.to_owned()
    };
    match url.strip_suffix("/ws") {
        Some(base) => format!("{base}/{path}"),
        None => url,
    }
}

fn io_error(kind: std::io::ErrorKind, message: impl Into<String>) -> WsError {
    WsError::Io(std::io::Error::new(kind, message.into()))
}

/// Why a stream session did not open.
#[derive(Debug)]
pub enum HttpStreamError {
    /// The request did not get through, or the response broke off.
    Request(String),
    /// The backend refused the session with this status.
    Refused { status: u16, body: String },
    /// The response was not a session.
    Malformed(String),
    /// No session record within the handshake deadline.
    Timeout,
}

impl std::fmt::Display for HttpStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Request(error) => write!(f, "HTTP stream request failed: {error}"),
            Self::Refused { status, body } => write!(f, "HTTP stream refused ({status}): {body}"),
            Self::Malformed(why) => write!(f, "HTTP stream sent no session: {why}"),
            Self::Timeout => write!(f, "HTTP stream session did not open in time"),
        }
    }
}

impl std::error::Error for HttpStreamError {}

impl HttpStreamError {
    fn into_transport_error(self) -> NativeTransportError {
        let message = self.to_string();
        match self {
            Self::Request(_) | Self::Timeout => NativeTransportError::Retryable(message),
            // A refused credential stays refused until the caller brings a
            // new one, as on a socket's upgrade.
            Self::Refused {
                status: 401 | 403, ..
            } => NativeTransportError::AuthRejected(message),
            // No stream here after all, or something else answering: fail
            // loudly rather than retry against it.
            Self::Refused { status: 404, .. } | Self::Malformed(_) => {
                NativeTransportError::Terminal(message)
            }
            // Any other refusal switched the endpoint back to sockets (see
            // `connect_stream`): the next session tries a socket, which
            // fails as a socket does if the backend has neither.
            Self::Refused { .. } => NativeTransportError::Retryable(message),
        }
    }
}

#[derive(serde::Deserialize)]
struct SessionRecord {
    id: String,
    key: String,
}

/// One open stream session, as a [`super::Carrier`]: received records come
/// out as messages (a close record as a Close), and messages sent go out in
/// send requests, in order. Dropping or closing it ends the session.
pub(crate) struct HttpStreamCarrier {
    incoming: mpsc::Receiver<Result<Message, WsError>>,
    outgoing: fmpsc::Sender<Message>,
}

impl HttpStreamCarrier {
    /// Opens a session at the stream endpoint beside `socket_url`, sending
    /// `authorization` on each of its requests. The task returned ends the
    /// session (its DELETE) once the carrier is closed or dropped, and then
    /// finishes.
    pub(crate) async fn open(
        http: &reqwest::Client,
        socket_url: &str,
        authorization: Option<String>,
    ) -> Result<(Self, tokio::task::JoinHandle<()>), HttpStreamError> {
        let stream_url = http_url(socket_url, "stream");
        let mut open = http.post(&stream_url);
        if let Some(authorization) = &authorization {
            open = open.header(reqwest::header::AUTHORIZATION, authorization);
        }
        let response = open
            .send()
            .await
            .map_err(|error| HttpStreamError::Request(error.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(HttpStreamError::Refused {
                status: status.as_u16(),
                body: body.chars().take(500).collect(),
            });
        }
        let mut body = response.bytes_stream();
        let mut reader = RecordReader::default();
        let (session, early) = loop {
            let Some(chunk) = body.next().await else {
                return Err(HttpStreamError::Malformed("the response ended".into()));
            };
            let chunk = chunk.map_err(|error| HttpStreamError::Request(error.to_string()))?;
            let mut records = reader.push(&chunk).into_iter();
            if let Some((kind, payload)) = records.next() {
                if kind != RECORD_SESSION {
                    return Err(HttpStreamError::Malformed(format!(
                        "the first record is of kind {kind}"
                    )));
                }
                let session: SessionRecord = serde_json::from_slice(&payload)
                    .map_err(|error| HttpStreamError::Malformed(error.to_string()))?;
                break (session, records.collect::<Vec<_>>());
            }
            if reader
                .pending_length()
                .is_some_and(|length| length > MAX_SESSION_RECORD_BYTES)
            {
                return Err(HttpStreamError::Malformed(
                    "the session record is too large".into(),
                ));
            }
        };

        let (incoming_tx, incoming) = mpsc::channel(INCOMING_SLOTS);
        let (outgoing, outgoing_rx) = fmpsc::channel(OUTGOING_SLOTS);
        let reader_task = tokio::spawn(read_records(body, reader, early, incoming_tx.clone()));
        let sender = tokio::spawn(send_records(
            http.clone(),
            format!("{stream_url}/{}", urlencode(&session.id)),
            session.key,
            authorization,
            outgoing_rx,
            incoming_tx,
            reader_task.abort_handle(),
        ));
        Ok((Self { incoming, outgoing }, sender))
    }
}

/// A session id is a UUID; escape anything else a backend might hand out.
fn urlencode(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

/// Hands the session's records to the carrier until the response ends.
async fn read_records<B>(
    mut body: B,
    mut reader: RecordReader,
    early: Vec<(u8, Vec<u8>)>,
    incoming: mpsc::Sender<Result<Message, WsError>>,
) where
    B: Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin,
{
    // `false` once the session is over (a close record, or the carrier went).
    let deliver = async |kind: u8, payload: Vec<u8>| -> bool {
        match kind {
            RECORD_MESSAGE => incoming
                .send(Ok(Message::Binary(payload.into())))
                .await
                .is_ok(),
            RECORD_CLOSE => {
                #[derive(serde::Deserialize)]
                struct Close {
                    code: u16,
                    #[serde(default)]
                    reason: String,
                }
                let close = serde_json::from_slice::<Close>(&payload).unwrap_or(Close {
                    code: 1005,
                    reason: String::new(),
                });
                let _ = incoming
                    .send(Ok(Message::Close(Some(CloseFrame {
                        code: close.code.into(),
                        reason: close.reason.into(),
                    }))))
                    .await;
                false
            }
            // Kinds this client doesn't know say nothing to it.
            _ => true,
        }
    };
    for (kind, payload) in early {
        if !deliver(kind, payload).await {
            return;
        }
    }
    loop {
        match body.next().await {
            Some(Ok(chunk)) => {
                for (kind, payload) in reader.push(&chunk) {
                    if !deliver(kind, payload).await {
                        return;
                    }
                }
                if reader
                    .pending_length()
                    .is_some_and(|length| length > MAX_RECORD_BYTES)
                {
                    let _ = incoming
                        .send(Err(io_error(
                            std::io::ErrorKind::InvalidData,
                            "HTTP stream record too large",
                        )))
                        .await;
                    return;
                }
            }
            Some(Err(error)) => {
                let _ = incoming
                    .send(Err(io_error(
                        std::io::ErrorKind::ConnectionReset,
                        format!("HTTP stream broke off: {error}"),
                    )))
                    .await;
                return;
            }
            // The session's response ended without a close record: as a
            // socket that dropped (1006). Said explicitly, since the sender
            // task's half keeps the channel open.
            None => {
                let _ = incoming
                    .send(Err(io_error(
                        std::io::ErrorKind::ConnectionReset,
                        "HTTP stream ended",
                    )))
                    .await;
                return;
            }
        }
    }
}

/// Sends what the carrier is given, a bounded body per request, one request
/// at a time, so the backend's pace (it answers a send once the core took
/// it) holds the next one back. Ends the session with a DELETE once the
/// carrier is closed or dropped, or a send failed.
async fn send_records(
    http: reqwest::Client,
    session_url: String,
    key: String,
    authorization: Option<String>,
    mut outgoing: fmpsc::Receiver<Message>,
    incoming: mpsc::Sender<Result<Message, WsError>>,
    reader: tokio::task::AbortHandle,
) {
    let request = |method: reqwest::Method| {
        let mut request = http
            .request(method, &session_url)
            .header(STREAM_KEY_HEADER, &key)
            .timeout(REQUEST_TIMEOUT);
        if let Some(authorization) = &authorization {
            request = request.header(reqwest::header::AUTHORIZATION, authorization);
        }
        request
    };
    let mut held: Option<Vec<u8>> = None;
    loop {
        let first = match held.take() {
            Some(record) => record,
            None => match outgoing.next().await {
                Some(Message::Binary(bytes)) => encode_record(RECORD_MESSAGE, &bytes),
                Some(Message::Text(text)) => encode_record(RECORD_MESSAGE, text.as_bytes()),
                // A socket's own control messages mean nothing here.
                Some(Message::Ping(_) | Message::Pong(_) | Message::Frame(_)) => continue,
                Some(Message::Close(_)) | None => break,
            },
        };
        let mut body = first;
        let mut closing = false;
        // Take what is already queued, up to the bound.
        loop {
            let record = match outgoing.try_recv() {
                Ok(Message::Binary(bytes)) => encode_record(RECORD_MESSAGE, &bytes),
                Ok(Message::Text(text)) => encode_record(RECORD_MESSAGE, text.as_bytes()),
                Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_)) => continue,
                Err(fmpsc::TryRecvError::Empty) => break,
                Ok(Message::Close(_)) | Err(fmpsc::TryRecvError::Closed) => {
                    closing = true;
                    break;
                }
            };
            if body.len() + record.len() > MAX_SEND_BYTES {
                held = Some(record);
                break;
            }
            body.extend_from_slice(&record);
        }
        let sent = request(reqwest::Method::POST)
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(body)
            .send()
            .await;
        let failure = match sent {
            Ok(response) if response.status().is_success() => None,
            Ok(response) => {
                let status = response.status().as_u16();
                let body = response.text().await.unwrap_or_default();
                Some(format!("HTTP stream send refused ({status}): {body}"))
            }
            Err(error) => Some(format!("HTTP stream send failed: {error}")),
        };
        if let Some(failure) = failure {
            let _ = incoming
                .send(Err(io_error(
                    std::io::ErrorKind::ConnectionAborted,
                    failure,
                )))
                .await;
            break;
        }
        if closing {
            break;
        }
    }
    // The DELETE ends the session on the backend, and only then does the
    // response go: dropped first, it would end the session on its own, and
    // the DELETE would find it gone (#5187).
    let _ = request(reqwest::Method::DELETE).send().await;
    reader.abort();
}

impl Stream for HttpStreamCarrier {
    type Item = Result<Message, WsError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().incoming.poll_recv(cx)
    }
}

fn closed_sink(_: fmpsc::SendError) -> WsError {
    io_error(std::io::ErrorKind::BrokenPipe, "HTTP stream session ended")
}

impl Sink<Message> for HttpStreamCarrier {
    type Error = WsError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        Pin::new(&mut self.get_mut().outgoing)
            .poll_ready(cx)
            .map_err(closed_sink)
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), WsError> {
        Pin::new(&mut self.get_mut().outgoing)
            .start_send(item)
            .map_err(closed_sink)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        Pin::new(&mut self.get_mut().outgoing)
            .poll_flush(cx)
            .map_err(closed_sink)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        Pin::new(&mut self.get_mut().outgoing)
            .poll_close(cx)
            .map_err(closed_sink)
    }
}

/// What a plain request for a socket path says about the endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Probe {
    /// A createJazz backend that can't upgrade sockets (426 + marker).
    Stream,
    /// A createJazz backend that does upgrade them (426, no marker).
    Upgrades,
    /// Anything else: another server, a proxy's error, a refused origin.
    Other,
    /// No answer.
    Silent,
}

async fn probe_socket_path(
    http: &reqwest::Client,
    socket_url: &str,
    authorization: Option<&str>,
) -> Probe {
    let mut request = http.post(http_url(socket_url, "ws")).timeout(PROBE_TIMEOUT);
    if let Some(authorization) = authorization {
        request = request.header(reqwest::header::AUTHORIZATION, authorization);
    }
    let Ok(response) = request.send().await else {
        return Probe::Silent;
    };
    if response.status().as_u16() != 426 {
        return Probe::Other;
    }
    let marked = response
        .headers()
        .get(STREAM_AVAILABLE_HEADER)
        .is_some_and(|value| value.as_bytes() == b"1");
    if marked {
        Probe::Stream
    } else {
        Probe::Upgrades
    }
}

/// What the connector remembers about one endpoint, for as long as it runs.
#[derive(Default)]
struct EndpointState {
    /// Syncs over the stream.
    stream: bool,
    /// Sockets in a row that failed at a backend that answers and upgrades.
    failed_upgrades: u32,
    /// Of those, the ones in a row that got no answer at all.
    hung_upgrades: u32,
}

/// Which transport a session used.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpStreamTransportKind {
    WebSocket,
    HttpStream,
}

/// A connector for a client of an app's main backend (TS `createJazz`): a
/// WebSocket when the host upgrades it, else the backend's HTTP stream.
///
/// A socket that fails before it opens asks the backend, with a plain POST
/// to the socket path, whether it serves the stream instead: a 426 with
/// `x-jazz-stream: 1` switches the endpoint to the stream at once. A 426
/// without it is a backend that upgrades sockets; when
/// [`UPGRADE_FAILURES_BEFORE_STREAM`] sockets in a row fail there anyway
/// ([`UPGRADE_HANGS_BEFORE_STREAM`] that got no answer), something in
/// between doesn't pass upgrades on, and the endpoint switches too. A
/// socket that fails for any other reason fails as a socket would.
///
/// The connector remembers the switch for as long as it lives (a client's
/// reconnects share it). A stream refused before it opens switches back,
/// so the next session tries a socket again; a 404 or a response that is
/// no session fails for good (`Terminal`), loudly.
#[derive(Clone)]
pub struct HttpStreamFallbackConnector {
    http: reqwest::Client,
    endpoints: Arc<Mutex<HashMap<String, EndpointState>>>,
    last: Arc<Mutex<Option<HttpStreamTransportKind>>>,
    /// The tasks that end this connector's stream sessions (see
    /// [`Self::settle`]).
    endings: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl Default for HttpStreamFallbackConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for HttpStreamFallbackConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpStreamFallbackConnector")
            .field("last", &self.last_transport())
            .finish_non_exhaustive()
    }
}

impl HttpStreamFallbackConnector {
    pub fn new() -> Self {
        Self {
            // Redirects would carry the credential elsewhere; a backend
            // never redirects its sync routes.
            http: crate::http_client_builder()
                .redirect(reqwest::redirect::Policy::none())
                // An idle stream session has no silence deadline (see
                // `HTTP_STREAM_LIVENESS`): TCP keepalive finds a dead one,
                // within about a minute, rather than the OS default's hours.
                .tcp_keepalive(TCP_KEEPALIVE_IDLE)
                .tcp_keepalive_interval(TCP_KEEPALIVE_INTERVAL)
                .tcp_keepalive_retries(TCP_KEEPALIVE_RETRIES)
                .build()
                .expect("an HTTP client with no redirects builds"),
            endpoints: Arc::default(),
            last: Arc::default(),
            endings: Arc::default(),
        }
    }

    /// Waits, for at most `timeout`, until the stream sessions this
    /// connector opened and its client has since dropped are ended on the
    /// backend (their DELETEs answered). A client calls it as it shuts
    /// down, before its runtime goes: the DELETE runs on that runtime, and
    /// a session whose DELETE never went out lingers on the backend until
    /// it notices the response is gone. Call it once the client has
    /// stopped: a session still in use holds it until `timeout`. Sockets
    /// need nothing: their close goes with the socket.
    ///
    /// A zero `timeout` returns at once without waiting for anything, as
    /// a client closing with no time to spare (`close(Duration::ZERO)`)
    /// asks: the backend then ends the sessions when their responses go.
    pub async fn settle(&self, timeout: Duration) {
        if timeout.is_zero() {
            return;
        }
        let endings = std::mem::take(&mut *self.endings.lock().expect("endings lock"));
        let _ = tokio::time::timeout(timeout, async {
            for ending in endings {
                let _ = ending.await;
            }
        })
        .await;
    }

    /// The transport the last session that opened used.
    pub fn last_transport(&self) -> Option<HttpStreamTransportKind> {
        *self.last.lock().expect("transport lock")
    }

    fn with_endpoint<T>(&self, url: &str, f: impl FnOnce(&mut EndpointState) -> T) -> T {
        let mut endpoints = self.endpoints.lock().expect("endpoint lock");
        f(endpoints.entry(url.to_owned()).or_default())
    }

    /// Whether a socket to `url` that just failed before opening gives way
    /// to the stream (as TS's `servesStreamInstead`).
    async fn serves_stream_instead(
        &self,
        url: &str,
        hung: bool,
        authorization: Option<&str>,
    ) -> bool {
        let answer = probe_socket_path(&self.http, url, authorization).await;
        self.with_endpoint(url, |state| match answer {
            Probe::Stream => {
                state.failed_upgrades = 0;
                state.hung_upgrades = 0;
                true
            }
            // Not a backend that answered and upgrades (down, restarting,
            // or refusing): the failures are no longer in a row.
            Probe::Other | Probe::Silent => {
                state.failed_upgrades = 0;
                state.hung_upgrades = 0;
                false
            }
            Probe::Upgrades => {
                state.failed_upgrades += 1;
                state.hung_upgrades = if hung { state.hung_upgrades + 1 } else { 0 };
                let switch = state.failed_upgrades >= UPGRADE_FAILURES_BEFORE_STREAM
                    || state.hung_upgrades >= UPGRADE_HANGS_BEFORE_STREAM;
                if switch {
                    state.failed_upgrades = 0;
                    state.hung_upgrades = 0;
                }
                switch
            }
        })
    }

    async fn connect_socket_or_stream(
        &self,
        request: NativeTransportRequest,
    ) -> Result<WebSocketTransport, NativeTransportError> {
        let url = ws_url(&request.server_url, request.app_id);
        let session = Session {
            peer_identity: request.peer_identity,
            auth: request.auth,
            bundle: request.bundle,
            requested_link: request.requested_link,
        };
        if self.with_endpoint(&url, |state| state.stream) {
            return self
                .connect_stream(&url, request.authorization, session, request.wake)
                .await;
        }
        let deadline = tokio::time::Instant::now() + WS_CLIENT_HANDSHAKE_TIMEOUT;
        let activity = LinkActivity::new();
        let opened = tokio::time::timeout_at(
            deadline,
            open_client_websocket(
                url.clone(),
                request.authorization.as_deref(),
                Arc::clone(&activity),
            ),
        )
        .await;
        let failure = match opened {
            Ok(Ok((ws, _))) => {
                self.with_endpoint(&url, |state| {
                    state.failed_upgrades = 0;
                    state.hung_upgrades = 0;
                });
                let transport = WebSocketTransport::establish(
                    ws,
                    deadline,
                    Liveness::new(activity, WS_CLIENT_LIVENESS),
                    session,
                    request.wake,
                )
                .await
                .map_err(native_transport_error)?;
                *self.last.lock().expect("transport lock") =
                    Some(HttpStreamTransportKind::WebSocket);
                return Ok(transport);
            }
            Ok(Err(failure)) => failure,
            Err(_) => NativeConnectFailure::WebSocket(WebSocketClientError::HandshakeTimeout),
        };
        let hung = matches!(
            failure,
            NativeConnectFailure::WebSocket(WebSocketClientError::HandshakeTimeout)
        );
        // A name that doesn't resolve has no backend to ask, and an upgrade
        // refused for its credential (401) was answered by the backend
        // itself. A 403 may be a WAF or CDN refusing upgrades: probe it, as
        // TS does.
        let probe = match &failure {
            NativeConnectFailure::Resolution(_) => false,
            NativeConnectFailure::WebSocket(WebSocketClientError::Connect(
                tokio_tungstenite::tungstenite::Error::Http(response),
            )) => response.status().as_u16() != 401,
            _ => true,
        };
        if probe
            && self
                .serves_stream_instead(&url, hung, request.authorization.as_deref())
                .await
        {
            self.with_endpoint(&url, |state| state.stream = true);
            return self
                .connect_stream(&url, request.authorization, session, request.wake)
                .await;
        }
        Err(native_connect_error(failure))
    }

    async fn connect_stream(
        &self,
        url: &str,
        authorization: Option<String>,
        session: Session,
        wake: Arc<dyn Fn() + Send + Sync>,
    ) -> Result<WebSocketTransport, NativeTransportError> {
        let deadline = tokio::time::Instant::now() + WS_CLIENT_HANDSHAKE_TIMEOUT;
        let opened = tokio::time::timeout_at(
            deadline,
            HttpStreamCarrier::open(&self.http, url, authorization),
        )
        .await
        .unwrap_or(Err(HttpStreamError::Timeout));
        let carrier = match opened {
            Ok((carrier, ending)) => {
                let mut endings = self.endings.lock().expect("endings lock");
                endings.retain(|ending| !ending.is_finished());
                endings.push(ending);
                carrier
            }
            Err(error) => {
                // Refused before it opened: the next session tries a socket.
                self.with_endpoint(url, |state| state.stream = false);
                return Err(error.into_transport_error());
            }
        };
        let transport = WebSocketTransport::establish(
            carrier,
            deadline,
            Liveness::new(LinkActivity::new(), HTTP_STREAM_LIVENESS),
            session,
            wake,
        )
        .await
        .map_err(native_transport_error)?;
        *self.last.lock().expect("transport lock") = Some(HttpStreamTransportKind::HttpStream);
        Ok(transport)
    }
}

impl NativeTransportConnector for HttpStreamFallbackConnector {
    fn connect(&self, request: NativeTransportRequest) -> NativeTransportFuture {
        let connector = self.clone();
        Box::pin(async move {
            let mut transport = connector.connect_socket_or_stream(request).await?;
            let (protocol_version, features) = transport.negotiated_transport_metadata();
            let terminal = transport.take_terminal_future();
            Ok(ConnectedNativeTransport {
                transport: Box::new(transport),
                protocol_version,
                features,
                terminal,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_split_across_chunks() {
        let mut bytes = encode_record(RECORD_SESSION, br#"{"id":"a","key":"b"}"#);
        bytes.extend(encode_record(RECORD_MESSAGE, &[1, 2, 3]));
        bytes.extend(encode_record(RECORD_CLOSE, b""));
        let mut reader = RecordReader::default();
        let mut records = Vec::new();
        for chunk in bytes.chunks(3) {
            records.extend(reader.push(chunk));
        }
        assert_eq!(
            records,
            vec![
                (RECORD_SESSION, br#"{"id":"a","key":"b"}"#.to_vec()),
                (RECORD_MESSAGE, vec![1, 2, 3]),
                (RECORD_CLOSE, Vec::new()),
            ]
        );
        assert_eq!(reader.pending_length(), None);
    }

    /// A backend sends nothing on an idle stream session: past the socket's
    /// silence deadline, the session is still up.
    #[tokio::test(start_paused = true)]
    async fn an_idle_stream_session_has_no_silence_deadline() {
        let mut stream = Liveness::new(LinkActivity::new(), HTTP_STREAM_LIVENESS);
        let mut socket = Liveness::new(LinkActivity::new(), WS_CLIENT_LIVENESS);
        let mut socket_gave_up = false;
        for _ in 0..(5 * 60 * 1000 / HTTP_STREAM_LIVENESS.tick.as_millis() as u64) {
            tokio::time::advance(HTTP_STREAM_LIVENESS.tick).await;
            assert_eq!(stream.check(), None);
            socket_gave_up |= socket.check().is_some();
        }
        assert!(socket_gave_up, "a socket that silent is given up");
    }

    #[test]
    fn stream_open_refusals_map_like_a_socket_upgrade() {
        let refused = |status| HttpStreamError::Refused {
            status,
            body: String::new(),
        };
        let kind = |error: HttpStreamError| match error.into_transport_error() {
            NativeTransportError::Retryable(_) => "retryable",
            NativeTransportError::Terminal(_) => "terminal",
            NativeTransportError::AuthRejected(_) => "auth",
            NativeTransportError::Wire(_) => "wire",
        };
        assert_eq!(kind(refused(401)), "auth");
        assert_eq!(kind(refused(403)), "auth");
        assert_eq!(kind(refused(404)), "terminal");
        assert_eq!(kind(HttpStreamError::Malformed("x".into())), "terminal");
        assert_eq!(kind(refused(503)), "retryable");
        assert_eq!(kind(refused(410)), "retryable");
        assert_eq!(kind(HttpStreamError::Timeout), "retryable");
    }

    #[test]
    fn stream_urls_sit_beside_the_socket_path() {
        assert_eq!(
            http_url("wss://example.com/jazz/apps/x/ws", "stream"),
            "https://example.com/jazz/apps/x/stream"
        );
        assert_eq!(
            http_url("ws://127.0.0.1:3000/jazz/apps/x/ws", "ws"),
            "http://127.0.0.1:3000/jazz/apps/x/ws"
        );
    }
}
