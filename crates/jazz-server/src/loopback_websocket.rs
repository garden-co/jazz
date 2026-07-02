//! Tiny loopback WebSocket bridge for exercising `InMemoryServerShell` byte frames.
//!
//! This is an alpha transport boundary for integration tests and local
//! loopback experiments. Each binary WebSocket message is a postcard-encoded
//! batch of encoded Jazz ABI wire frames, unchanged end to end inside the batch.

use std::fmt;
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use jazz::db::DbIdentity;
use jazz::ids::{AuthorId, NodeUuid};
use jazz::protocol_limits::{MAX_WIRE_FRAME_BYTES, validate_wire_frame_len};
use jazz::schema::JazzSchema;
use tokio::net::{TcpListener as TokioTcpListener, TcpStream};
use tokio::sync::Mutex as TokioMutex;
use tokio_tungstenite::{WebSocketStream, accept_hdr_async};
use tungstenite::handshake::server::{Request, Response};
use tungstenite::protocol::Message;

use crate::auth_admission::{
    AdmissionSource, AdmittedSession, AuthAdmissionConfig, AuthAdmissionError, AuthHandshake,
    LOCAL_FIRST_JWT_ISSUER, admit_bearer_jwt, admit_local_first_jwt, admit_static_bearer,
    admit_static_bearer_with_claims, bearer_from_authorization,
};
use crate::{
    InMemoryServerShell, InMemoryServerShellConfig, ListenerConfig, ShellError, StorageConfig,
};

/// Result type returned by loopback WebSocket helpers.
pub type LoopbackWebSocketResult<T> = std::result::Result<T, LoopbackWebSocketError>;

/// Running loopback WebSocket listener and shutdown handle.
#[derive(Debug)]
pub struct LoopbackWebSocketServer {
    addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

/// Alpha-shaped loopback WebSocket server configuration.
///
/// This mirrors the current alpha CLI/dev-server split between an explicit
/// in-memory mode and a durable `dataDir`-style storage path.
#[derive(Clone, Debug, PartialEq)]
pub struct LoopbackWebSocketServerConfig {
    /// Listener/socket settings for the loopback transport.
    pub listener: ListenerConfig,
    /// Schema served by the loopback server database.
    pub schema: JazzSchema,
    /// Database identity used by server-owned local writes.
    pub identity: DbIdentity,
    /// Optional deterministic row-id seed for ABI writes.
    pub row_id_seed: Option<u64>,
    /// Storage backend requested by the caller.
    pub storage: StorageConfig,
    /// Transport admission policy.
    pub auth_admission: AuthAdmissionConfig,
}

impl LoopbackWebSocketServerConfig {
    /// Construct an in-memory loopback WebSocket config.
    pub fn in_memory(schema: JazzSchema, identity: DbIdentity) -> Self {
        Self {
            listener: ListenerConfig::default(),
            schema,
            identity,
            row_id_seed: None,
            storage: StorageConfig::InMemory,
            auth_admission: AuthAdmissionConfig::default(),
        }
    }

    /// Construct a durable loopback WebSocket config rooted at a data dir.
    pub fn persistent_data_dir(
        schema: JazzSchema,
        identity: DbIdentity,
        data_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            listener: ListenerConfig::default(),
            schema,
            identity,
            row_id_seed: None,
            storage: StorageConfig::data_dir(data_dir),
            auth_admission: AuthAdmissionConfig::default(),
        }
    }

    /// Set a deterministic row-id seed for server-side ABI writes.
    pub fn with_row_id_seed(mut self, row_id_seed: u64) -> Self {
        self.row_id_seed = Some(row_id_seed);
        self
    }

    /// Set loopback WebSocket admission policy.
    pub fn with_auth_admission(mut self, auth_admission: AuthAdmissionConfig) -> Self {
        self.auth_admission = auth_admission;
        self
    }

    fn in_memory_shell_config(&self) -> InMemoryServerShellConfig {
        let mut config = InMemoryServerShellConfig::new(self.schema.clone(), self.identity);
        if let Some(row_id_seed) = self.row_id_seed {
            config = config.with_row_id_seed(row_id_seed);
        }
        config
    }
}

impl LoopbackWebSocketServer {
    /// Start a WebSocket listener around an in-memory shell constructed on the listener thread.
    pub fn start(
        bind_addr: SocketAddr,
        config: InMemoryServerShellConfig,
        websocket_path: impl Into<String>,
    ) -> LoopbackWebSocketResult<Self> {
        Self::start_with_admission(
            bind_addr,
            config,
            websocket_path,
            AuthAdmissionConfig::default(),
        )
    }

    /// Start a WebSocket listener with explicit auth/session admission policy.
    pub fn start_with_admission(
        bind_addr: SocketAddr,
        config: InMemoryServerShellConfig,
        websocket_path: impl Into<String>,
        auth_admission: AuthAdmissionConfig,
    ) -> LoopbackWebSocketResult<Self> {
        Self::start_with_storage_admission(
            bind_addr,
            config,
            StorageConfig::InMemory,
            websocket_path,
            auth_admission,
        )
    }

    fn start_with_storage_admission(
        bind_addr: SocketAddr,
        config: InMemoryServerShellConfig,
        storage: StorageConfig,
        websocket_path: impl Into<String>,
        auth_admission: AuthAdmissionConfig,
    ) -> LoopbackWebSocketResult<Self> {
        let listener = TcpListener::bind(bind_addr)?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let websocket_path = websocket_path.into();

        let thread_shutdown = Arc::clone(&shutdown);
        let (startup_tx, startup_rx) = mpsc::channel::<LoopbackWebSocketResult<()>>();
        let thread = thread::spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = startup_tx.send(Err(io::Error::other(error).into()));
                    return;
                }
            };
            let local = tokio::task::LocalSet::new();
            runtime.block_on(local.run_until(async move {
                let shell = match InMemoryServerShell::start_with_storage(config, storage) {
                    Ok(shell) => shell,
                    Err(error) => {
                        let _ = startup_tx.send(Err(error.into()));
                        return;
                    }
                };
                let listener = match TokioTcpListener::from_std(listener) {
                    Ok(listener) => listener,
                    Err(error) => {
                        let _ = startup_tx.send(Err(error.into()));
                        return;
                    }
                };
                let _ = startup_tx.send(Ok(()));
                #[allow(clippy::arc_with_non_send_sync)]
                let shell = Arc::new(TokioMutex::new(shell));
                accept_loop(
                    listener,
                    shell,
                    websocket_path,
                    auth_admission,
                    thread_shutdown,
                )
                .await;
            }));
        });
        match startup_rx.recv() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                let _ = thread.join();
                return Err(error);
            }
            Err(_) => {
                let _ = thread.join();
                return Err(io::Error::other("loopback WebSocket listener failed to start").into());
            }
        }

        Ok(Self {
            addr,
            shutdown,
            thread: Some(thread),
        })
    }

    /// Start a WebSocket listener around a default empty-schema in-memory shell.
    pub fn start_default(bind_addr: SocketAddr) -> LoopbackWebSocketResult<Self> {
        let listener = ListenerConfig::default();
        Self::start(bind_addr, default_config(), listener.websocket_path)
    }

    /// Start a WebSocket listener using the path from a listener config.
    pub fn start_with_listener_config(
        listener: &ListenerConfig,
        config: InMemoryServerShellConfig,
    ) -> LoopbackWebSocketResult<Self> {
        Self::start(listener.bind_addr, config, listener.websocket_path.clone())
    }

    /// Start a WebSocket listener from alpha-shaped storage config.
    pub fn start_with_config(
        config: LoopbackWebSocketServerConfig,
    ) -> LoopbackWebSocketResult<Self> {
        match &config.storage {
            StorageConfig::InMemory => Self::start_with_admission(
                config.listener.bind_addr,
                config.in_memory_shell_config(),
                config.listener.websocket_path.clone(),
                config.auth_admission,
            ),
            StorageConfig::RocksDb { .. } => Self::start_with_storage_admission(
                config.listener.bind_addr,
                config.in_memory_shell_config(),
                config.storage,
                config.listener.websocket_path.clone(),
                config.auth_admission,
            ),
            StorageConfig::SQLite { .. } => {
                Err(LoopbackWebSocketError::DurableStorageUnavailable {
                    storage: config.storage,
                })
            }
        }
    }

    /// Return the bound socket address.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Request listener shutdown and wait for the accept loop to exit.
    pub fn shutdown(mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for LoopbackWebSocketServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Error returned by loopback WebSocket startup and shell plumbing.
#[derive(Debug)]
pub enum LoopbackWebSocketError {
    /// Socket or stream operation failed.
    Io(io::Error),
    /// The in-memory shell failed while starting or handling frames.
    Shell(ShellError),
    /// Requested loopback storage backend is not supported by this shell.
    DurableStorageUnavailable {
        /// Requested storage config.
        storage: StorageConfig,
    },
}

impl fmt::Display for LoopbackWebSocketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "loopback WebSocket I/O error: {error}"),
            Self::Shell(error) => write!(f, "loopback WebSocket shell error: {error}"),
            Self::DurableStorageUnavailable { storage } => write!(
                f,
                "loopback WebSocket storage backend is not available for {storage:?}; this shell supports in-memory and RocksDB data-dir storage"
            ),
        }
    }
}

impl std::error::Error for LoopbackWebSocketError {}

impl From<io::Error> for LoopbackWebSocketError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<ShellError> for LoopbackWebSocketError {
    fn from(error: ShellError) -> Self {
        Self::Shell(error)
    }
}

struct UpgradeAdmission {
    bearer: Option<String>,
}

fn default_config() -> InMemoryServerShellConfig {
    InMemoryServerShellConfig::new(
        JazzSchema::new([]),
        DbIdentity {
            node: NodeUuid::from_bytes([0x5e; 16]),
            author: AuthorId::SYSTEM,
        },
    )
}

async fn accept_loop(
    listener: TokioTcpListener,
    shell: Arc<TokioMutex<InMemoryServerShell>>,
    websocket_path: String,
    auth_admission: AuthAdmissionConfig,
    shutdown: Arc<AtomicBool>,
) {
    let mut shutdown_tick = tokio::time::interval(Duration::from_millis(10));
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else {
                    break;
                };
                tokio::task::spawn_local(handle_connection(
                    stream,
                    Arc::clone(&shell),
                    websocket_path.clone(),
                    auth_admission.clone(),
                ));
            }
            _ = shutdown_tick.tick() => {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
            }
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    shell: Arc<TokioMutex<InMemoryServerShell>>,
    websocket_path: String,
    auth_admission: AuthAdmissionConfig,
) {
    let accepted_admission = Arc::new(Mutex::new(None));
    let callback_admission = Arc::clone(&accepted_admission);
    let expected_path = websocket_path;
    let accepted = accept_hdr_async(stream, move |request: &Request, response: Response| {
        if !websocket_path_matches(request.uri().path(), &expected_path) {
            return Err(tungstenite::handshake::server::ErrorResponse::new(Some(
                "not found".to_owned(),
            )));
        }
        if request
            .uri()
            .query()
            .is_some_and(query_has_identity_parameter)
        {
            return Err(tungstenite::handshake::server::ErrorResponse::new(Some(
                "URL identity auth is not supported".to_owned(),
            )));
        }
        let bearer = request
            .headers()
            .get("Authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(bearer_from_authorization)
            .map(str::to_owned);
        if let Ok(mut slot) = callback_admission.lock() {
            *slot = Some(UpgradeAdmission { bearer });
        }
        Ok(response)
    })
    .await;
    let Ok(mut socket) = accepted else {
        return;
    };

    let admission = accepted_admission
        .lock()
        .ok()
        .and_then(|mut slot| slot.take())
        .unwrap_or(UpgradeAdmission { bearer: None });
    let admitted = match admit_socket(&mut socket, &auth_admission, admission).await {
        Ok(admitted) => admitted,
        Err(_) => {
            let _ = socket.close(None).await;
            return;
        }
    };

    let session = {
        let mut shell = shell.lock().await;
        if admitted.claims.is_empty() {
            shell.accept_subscriber_session(admitted.author)
        } else {
            shell.accept_subscriber_session_with_claims(admitted.author, admitted.claims)
        }
    };
    let Ok(session) = session else {
        let _ = socket.close(None).await;
        return;
    };

    service_connection(socket, shell, session).await;
}

fn websocket_path_matches(request_path: &str, expected_path: &str) -> bool {
    request_path == expected_path
}

async fn admit_socket(
    socket: &mut WebSocketStream<TcpStream>,
    auth_admission: &AuthAdmissionConfig,
    admission: UpgradeAdmission,
) -> std::result::Result<AdmittedSession, AuthAdmissionError> {
    if admission.bearer.is_some() && auth_admission.jwt_verifier.is_some() {
        if is_local_first_bearer(auth_admission, admission.bearer.as_deref()) {
            return admit_local_first_jwt(auth_admission, admission.bearer.as_deref());
        }
        return admit_bearer_jwt(
            auth_admission,
            admission.bearer.as_deref(),
            AdmissionSource::AuthorizationHeader,
        );
    }

    if !auth_admission.requires_bearer() {
        return admit_static_bearer(
            auth_admission,
            None,
            &auth_admission.anonymous_subject,
            AdmissionSource::Anonymous,
        );
    }

    let message = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .map_err(|_| AuthAdmissionError::InvalidHandshake("timed out".to_owned()))?
        .ok_or_else(|| AuthAdmissionError::InvalidHandshake("socket closed".to_owned()))?
        .map_err(|error| AuthAdmissionError::InvalidHandshake(error.to_string()))?;
    let text = match message {
        Message::Text(text) => text.to_string(),
        _ => {
            return Err(AuthAdmissionError::InvalidHandshake(
                "expected text AuthHandshake".to_owned(),
            ));
        }
    };
    let handshake: AuthHandshake = serde_json::from_str(&text)
        .map_err(|error| AuthAdmissionError::InvalidHandshake(error.to_string()))?;
    validate_auth_handshake(&handshake)?;
    if auth_admission.jwt_verifier.is_some() {
        if is_local_first_bearer(auth_admission, handshake.bearer_jwt.as_deref()) {
            return admit_local_first_jwt(auth_admission, handshake.bearer_jwt.as_deref());
        }
        return admit_bearer_jwt(
            auth_admission,
            handshake.bearer_jwt.as_deref(),
            AdmissionSource::FirstFrameHandshake,
        );
    }
    admit_static_bearer_with_claims(
        auth_admission,
        admission
            .bearer
            .as_deref()
            .or(handshake.bearer_jwt.as_deref()),
        handshake.sub,
        handshake.claims,
        AdmissionSource::FirstFrameHandshake,
    )
}

fn query_has_identity_parameter(query: &str) -> bool {
    query
        .split('&')
        .filter_map(|part| part.split_once('=').map(|(name, _)| name))
        .any(|name| name == "identity")
}

fn is_local_first_bearer(config: &AuthAdmissionConfig, bearer: Option<&str>) -> bool {
    if !config.allow_local_first_auth {
        return false;
    }
    let Some(bearer) = bearer else {
        return false;
    };
    let Ok(decoded) = jsonwebtoken::dangerous::insecure_decode::<JwtIssuerClaims>(bearer) else {
        return false;
    };
    decoded.claims.iss.as_deref() == Some(LOCAL_FIRST_JWT_ISSUER)
}

#[derive(serde::Deserialize)]
struct JwtIssuerClaims {
    iss: Option<String>,
}

fn validate_auth_handshake(
    handshake: &AuthHandshake,
) -> std::result::Result<(), AuthAdmissionError> {
    if handshake.sub.trim().is_empty() {
        return Err(AuthAdmissionError::InvalidHandshake(
            "sub must be non-empty".to_owned(),
        ));
    }
    if matches!(handshake.bearer_jwt.as_deref(), Some("")) {
        return Err(AuthAdmissionError::InvalidHandshake(
            "bearerJwt must be non-empty when supplied".to_owned(),
        ));
    }
    Ok(())
}

async fn service_connection(
    mut socket: WebSocketStream<TcpStream>,
    shell: Arc<TokioMutex<InMemoryServerShell>>,
    session: crate::ServerSession,
) {
    let mut outbound_tick = tokio::time::interval(Duration::from_millis(5));
    loop {
        tokio::select! {
            message = socket.next() => {
                let Some(message) = message else {
                    break;
                };
                match message {
                    Ok(Message::Binary(batch)) => {
                        let Ok(frames) = decode_frame_batch(&batch) else {
                            break;
                        };
                        let mut shell = shell.lock().await;
                        if shell.receive_frames(session, frames).is_err() || shell.tick().is_err() {
                            break;
                        }
                        match shell.take_frames(session) {
                            Ok(frames) if !frames.is_empty() => {
                                let Ok(batches) = encode_frame_batches(&frames) else {
                                    break;
                                };
                                let mut send_failed = false;
                                for batch in batches {
                                    if socket.send(Message::Binary(batch.into())).await.is_err() {
                                        send_failed = true;
                                        break;
                                    }
                                }
                                if send_failed {
                                    break;
                                }
                            }
                            Ok(_) => {}
                            Err(_) => break,
                        }
                    }
                    Ok(Message::Close(_)) => break,
                    Ok(Message::Ping(payload)) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Ok(Message::Pong(_)) => {}
                    Ok(Message::Text(_)) | Ok(Message::Frame(_)) => break,
                    Err(_) => break,
                }
            }
            _ = outbound_tick.tick() => {
                let frames = {
                    let mut shell = shell.lock().await;
                    if shell.tick().is_err() {
                        break;
                    }
                    match shell.take_frames(session) {
                        Ok(frames) => frames,
                        Err(_) => break,
                    }
                };
                if !frames.is_empty() {
                    let Ok(batches) = encode_frame_batches(&frames) else {
                        break;
                    };
                    let mut send_failed = false;
                    for batch in batches {
                        if socket.send(Message::Binary(batch.into())).await.is_err() {
                            send_failed = true;
                            break;
                        }
                    }
                    if send_failed {
                        break;
                    }
                }
            }
        }
    }

    let _ = shell.lock().await.close_session(session);
    let _ = socket.close(None).await;
}

fn encode_frame_batches(frames: &[Vec<u8>]) -> Result<Vec<Vec<u8>>, postcard::Error> {
    let mut batches = Vec::new();
    let mut current = Vec::new();
    for frame in frames {
        if validate_wire_frame_len(frame.len()).is_err() {
            return Err(postcard::Error::SerializeBufferFull);
        }
        let mut candidate = current.clone();
        candidate.push(frame.clone());
        let encoded = postcard::to_allocvec(&candidate)?;
        if encoded.len() > MAX_WIRE_FRAME_BYTES && !current.is_empty() {
            batches.push(postcard::to_allocvec(&current)?);
            current.clear();
        } else if encoded.len() > MAX_WIRE_FRAME_BYTES {
            return Err(postcard::Error::SerializeBufferFull);
        }
        current.push(frame.clone());
    }
    if !current.is_empty() {
        batches.push(postcard::to_allocvec(&current)?);
    }
    Ok(batches)
}

fn decode_frame_batch(bytes: &[u8]) -> Result<Vec<Vec<u8>>, postcard::Error> {
    if bytes.len() > MAX_WIRE_FRAME_BYTES {
        return Err(postcard::Error::DeserializeUnexpectedEnd);
    }
    let frames: Vec<Vec<u8>> = postcard::from_bytes(bytes)?;
    if frames
        .iter()
        .any(|frame| validate_wire_frame_len(frame.len()).is_err())
    {
        return Err(postcard::Error::DeserializeUnexpectedEnd);
    }
    Ok(frames)
}
