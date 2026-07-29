//! Tiny loopback HTTP bridge for exercising `InMemoryServerShell` byte frames.
//!
//! This is an integrability slice, not the production sync transport. Frame
//! request and response bodies are newline-separated lowercase hex strings,
//! where each line is one encoded Jazz ABI wire frame.
//!
//! The alpha admin schema API under `/apps/{app_id}` stores and returns raw
//! schema JSON while publishing accepted schemas into the local runtime
//! catalogue, including durable startup reload from `admin-schemas.json`.

use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jazz::db::DbIdentity;
use jazz::ids::{AuthorId, NodeUuid};
use jazz::schema::JazzSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::admin_schema_convert::convert_admin_schema;
use crate::{InMemoryServerShell, InMemoryServerShellConfig, MetricsSnapshot, ShellError};

/// Result type returned by loopback HTTP helpers.
pub type LoopbackHttpResult<T> = std::result::Result<T, LoopbackHttpError>;

/// Load the latest admin-published schema for an app from `admin-schemas.json`.
///
/// Permissions-bearing schema publishes are intentionally not loaded here yet:
/// the current admin path rejects non-null permissions, and this helper keeps
/// durable server startup aligned with that supported surface.
pub fn load_latest_admin_schema_for_app(
    data_dir: impl AsRef<Path>,
    app_id: &str,
) -> io::Result<Option<JazzSchema>> {
    let schema_store_path = data_dir.as_ref().join("admin-schemas.json");
    let schemas = load_admin_schema_store(Some(&schema_store_path))?;
    let Some(schema) = schemas.get(app_id).and_then(|schemas| schemas.last()) else {
        return Ok(None);
    };
    if matches!(schema.permissions.as_ref(), Some(value) if !value.is_null()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "stored admin schema {} has unsupported permissions",
                schema.object_id
            ),
        ));
    }
    convert_admin_schema(&schema.schema)
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

/// Running loopback listener and shutdown handle.
#[derive(Debug)]
pub struct LoopbackHttpServer {
    addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl LoopbackHttpServer {
    /// Start an HTTP listener around an in-memory shell constructed on the listener thread.
    pub fn start(
        bind_addr: SocketAddr,
        config: InMemoryServerShellConfig,
    ) -> LoopbackHttpResult<Self> {
        Self::start_inner(bind_addr, config, None, None)
    }

    /// Start an HTTP listener around a default empty-schema in-memory shell.
    pub fn start_default(bind_addr: SocketAddr) -> LoopbackHttpResult<Self> {
        Self::start(bind_addr, default_config())
    }

    /// Start an HTTP listener with the alpha admin schema API enabled.
    pub fn start_with_admin_secret(
        bind_addr: SocketAddr,
        config: InMemoryServerShellConfig,
        admin_secret: impl Into<String>,
    ) -> LoopbackHttpResult<Self> {
        Self::start_inner(bind_addr, config, Some(admin_secret.into()), None)
    }

    /// Start an HTTP listener with schemas persisted as `admin-schemas.json` under a data dir.
    ///
    /// This mirrors the alpha server's data-dir-shaped local durability for
    /// schema catalogue metadata and reloads accepted schemas into the runtime
    /// catalogue before the listener reports ready.
    pub fn start_with_admin_secret_and_data_dir(
        bind_addr: SocketAddr,
        config: InMemoryServerShellConfig,
        admin_secret: impl Into<String>,
        data_dir: impl Into<PathBuf>,
    ) -> LoopbackHttpResult<Self> {
        let schema_store_path = data_dir.into().join("admin-schemas.json");
        Self::start_with_admin_secret_and_schema_store(
            bind_addr,
            config,
            admin_secret,
            schema_store_path,
        )
    }

    /// Start an HTTP listener with the alpha admin schema API backed by a JSON file.
    ///
    /// The stored schema catalogue is reloaded for API responses and the
    /// runtime catalogue before the listener reports ready.
    pub fn start_with_admin_secret_and_schema_store(
        bind_addr: SocketAddr,
        config: InMemoryServerShellConfig,
        admin_secret: impl Into<String>,
        schema_store_path: impl Into<PathBuf>,
    ) -> LoopbackHttpResult<Self> {
        Self::start_inner(
            bind_addr,
            config,
            Some(admin_secret.into()),
            Some(schema_store_path.into()),
        )
    }

    fn start_inner(
        bind_addr: SocketAddr,
        config: InMemoryServerShellConfig,
        admin_secret: Option<String>,
        schema_store_path: Option<PathBuf>,
    ) -> LoopbackHttpResult<Self> {
        let listener = TcpListener::bind(bind_addr)?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        let shutdown = Arc::new(AtomicBool::new(false));

        let thread_shutdown = Arc::clone(&shutdown);
        let (startup_tx, startup_rx) = mpsc::channel();
        let thread = thread::spawn(move || {
            let mut shell = match InMemoryServerShell::start(config) {
                Ok(shell) => shell,
                Err(error) => {
                    let _ = startup_tx.send(Err(error.into()));
                    return;
                }
            };
            let schemas = match load_admin_schema_store(schema_store_path.as_deref()) {
                Ok(schemas) => schemas,
                Err(error) => {
                    let _ = startup_tx.send(Err(error.into()));
                    return;
                }
            };
            if let Err(error) = reload_admin_schema_catalogue(&mut shell, &schemas) {
                let _ = startup_tx.send(Err(error));
                return;
            }
            let _ = startup_tx.send(Ok(()));
            accept_loop(
                listener,
                LoopbackState {
                    shell,
                    sessions: HashMap::new(),
                    next_session_id: 1,
                    admin_secret,
                    schema_store_path,
                    schemas,
                },
                thread_shutdown,
            );
        });
        match startup_rx.recv() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                let _ = thread.join();
                return Err(error);
            }
            Err(_) => {
                let _ = thread.join();
                return Err(io::Error::other("loopback HTTP listener failed to start").into());
            }
        }

        Ok(Self {
            addr,
            shutdown,
            thread: Some(thread),
        })
    }

    /// Return the bound socket address.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Request listener shutdown and wait for the accept loop to exit.
    pub fn shutdown(mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(self.addr);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for LoopbackHttpServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(self.addr);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Error returned by loopback HTTP startup and shell plumbing.
#[derive(Debug)]
pub enum LoopbackHttpError {
    /// Socket or stream operation failed.
    Io(io::Error),
    /// The in-memory shell failed while starting or handling a request.
    Shell(ShellError),
}

impl fmt::Display for LoopbackHttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "loopback HTTP I/O error: {error}"),
            Self::Shell(error) => write!(f, "loopback HTTP shell error: {error}"),
        }
    }
}

impl std::error::Error for LoopbackHttpError {}

impl From<io::Error> for LoopbackHttpError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<ShellError> for LoopbackHttpError {
    fn from(error: ShellError) -> Self {
        Self::Shell(error)
    }
}

#[derive(Debug)]
struct LoopbackState {
    shell: InMemoryServerShell,
    sessions: HashMap<u64, crate::ServerSession>,
    next_session_id: u64,
    admin_secret: Option<String>,
    schema_store_path: Option<PathBuf>,
    schemas: HashMap<String, Vec<StoredAdminSchema>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct StoredAdminSchema {
    hash: String,
    object_id: String,
    published_at: u64,
    schema: Value,
    permissions: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    local_schema_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PublishSchemaRequest {
    schema: Value,
    #[serde(default)]
    permissions: Option<Value>,
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

fn accept_loop(listener: TcpListener, mut state: LoopbackState, shutdown: Arc<AtomicBool>) {
    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((mut stream, _)) => {
                let response = match read_request(&mut stream) {
                    Ok(request) => handle_request(request, &mut state),
                    Err(error) => response(
                        400,
                        "Bad Request",
                        "text/plain; charset=utf-8",
                        error.into_bytes(),
                    ),
                };
                let _ = stream.write_all(&response);
                let _ = stream.flush();
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(_) => break,
        }
    }
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

fn read_request(stream: &mut TcpStream) -> std::result::Result<HttpRequest, String> {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|error| error.to_string())?;
    let mut bytes = Vec::new();
    let mut buffer = [0; 1024];
    let header_end;
    loop {
        let read = stream
            .read(&mut buffer)
            .map_err(|error| error.to_string())?;
        if read == 0 {
            return Err("connection closed before headers".to_owned());
        }
        bytes.extend_from_slice(&buffer[..read]);
        if let Some(position) = find_header_end(&bytes) {
            header_end = position;
            break;
        }
        if bytes.len() > 64 * 1024 {
            return Err("request headers too large".to_owned());
        }
    }

    let headers = String::from_utf8(bytes[..header_end].to_vec())
        .map_err(|_| "request headers are not utf-8".to_owned())?;
    let mut lines = headers.split("\r\n");
    let request_line = lines.next().ok_or("missing request line")?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().ok_or("missing method")?.to_owned();
    let path = request_parts.next().ok_or("missing path")?.to_owned();
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
        .collect::<HashMap<_, _>>();
    let content_length = headers
        .get("content-length")
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|_| "invalid content-length".to_owned())?
        .unwrap_or(0);

    let body_start = header_end + 4;
    while bytes.len() < body_start + content_length {
        let read = stream
            .read(&mut buffer)
            .map_err(|error| error.to_string())?;
        if read == 0 {
            return Err("connection closed before body".to_owned());
        }
        bytes.extend_from_slice(&buffer[..read]);
    }

    Ok(HttpRequest {
        method,
        path,
        headers,
        body: bytes[body_start..body_start + content_length].to_vec(),
    })
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn handle_request(request: HttpRequest, state: &mut LoopbackState) -> Vec<u8> {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/healthz") => {
            let health = state.shell.health_snapshot();
            response(
                200,
                "OK",
                "text/plain; charset=utf-8",
                format!(
                    "status={}\nmessage={}\n",
                    health_status_name(health.status),
                    health.message
                )
                .into_bytes(),
            )
        }
        ("GET", "/metrics") => response(
            200,
            "OK",
            "text/plain; charset=utf-8",
            render_metrics(&state.shell.metrics_snapshot()).into_bytes(),
        ),
        ("POST", "/sessions") => {
            let session = match state
                .shell
                .accept_subscriber_session(AuthorId::from_bytes([0xc1; 16]))
            {
                Ok(session) => session,
                Err(error) => return shell_error_response(error),
            };
            let session_id = state.next_session_id;
            state.next_session_id += 1;
            state.sessions.insert(session_id, session);
            response(
                201,
                "Created",
                "text/plain; charset=utf-8",
                format!("id={session_id}\n").into_bytes(),
            )
        }
        ("POST", path) if path.starts_with("/sessions/") && path.ends_with("/frames") => {
            handle_frames(path, request.body, state)
        }
        ("POST", path) if path.starts_with("/apps/") && path.ends_with("/admin/schemas") => {
            handle_admin_publish_schema(path, &request, state)
        }
        ("GET", path) if path.starts_with("/apps/") && path.ends_with("/schemas") => {
            handle_list_schemas(path, &request, state)
        }
        ("GET", path) if path.starts_with("/apps/") && path.contains("/schema/") => {
            handle_get_schema(path, &request, state)
        }
        _ => response(
            404,
            "Not Found",
            "text/plain; charset=utf-8",
            b"not found\n".to_vec(),
        ),
    }
}

fn handle_admin_publish_schema(
    path: &str,
    request: &HttpRequest,
    state: &mut LoopbackState,
) -> Vec<u8> {
    let Some(app_id) = path
        .strip_prefix("/apps/")
        .and_then(|tail| tail.strip_suffix("/admin/schemas"))
    else {
        return not_found_response();
    };
    if !admin_secret_matches(request, state.admin_secret.as_deref()) {
        return json_response(401, "Unauthorized", json!({ "error": "unauthorized" }));
    }
    let publish = match serde_json::from_slice::<PublishSchemaRequest>(&request.body) {
        Ok(publish) => publish,
        Err(error) => {
            return json_response(
                400,
                "Bad Request",
                json!({ "error": "invalid_schema_publish_request", "message": error.to_string() }),
            );
        }
    };
    if matches!(publish.permissions.as_ref(), Some(value) if !value.is_null()) {
        return json_response(
            400,
            "Bad Request",
            json!({
                "error": "unsupported_permissions",
                "message": "admin schema permissions must be null until runtime schema loading is wired"
            }),
        );
    }

    let local_schema = match convert_admin_schema(&publish.schema) {
        Ok(schema) => schema,
        Err(error) => {
            return json_response(
                400,
                "Bad Request",
                json!({
                    "error": "unsupported_admin_schema",
                    "path": error.path(),
                    "message": error.to_string()
                }),
            );
        }
    };
    let local_schema_id = local_schema.version_id();
    if let Err(error) = state.shell.publish_runtime_schema(local_schema) {
        return shell_error_response(error);
    }

    let canonical = match serde_json::to_vec(&publish.schema) {
        Ok(bytes) => bytes,
        Err(error) => {
            return json_response(
                400,
                "Bad Request",
                json!({ "error": "invalid_schema_json", "message": error.to_string() }),
            );
        }
    };
    let hash = hex_digest(&canonical);
    let schema = StoredAdminSchema {
        object_id: format!("schema:{app_id}:{hash}"),
        published_at: unix_timestamp(),
        hash,
        schema: publish.schema,
        permissions: None,
        local_schema_id: Some(local_schema_id.0.to_string()),
    };
    let app_schemas = state.schemas.entry(app_id.to_owned()).or_default();
    if let Some(existing) = app_schemas
        .iter_mut()
        .find(|existing| existing.hash == schema.hash)
    {
        *existing = schema.clone();
    } else {
        app_schemas.push(schema.clone());
    }
    if let Err(error) = save_admin_schema_store(state.schema_store_path.as_deref(), &state.schemas)
    {
        return json_response(
            500,
            "Internal Server Error",
            json!({ "error": "schema_store_write_failed", "message": error.to_string() }),
        );
    }

    eprintln!(
        "admin_schema_api=partial runtime_schema_loading=local_catalogue_wired app_id={app_id} hash={}",
        schema.hash
    );
    json_response(
        201,
        "Created",
        json!({ "objectId": schema.object_id, "hash": schema.hash }),
    )
}

fn handle_list_schemas(path: &str, request: &HttpRequest, state: &LoopbackState) -> Vec<u8> {
    let Some(app_id) = path
        .strip_prefix("/apps/")
        .and_then(|tail| tail.strip_suffix("/schemas"))
    else {
        return not_found_response();
    };
    if !admin_secret_matches(request, state.admin_secret.as_deref()) {
        return json_response(401, "Unauthorized", json!({ "error": "unauthorized" }));
    }
    let schemas = state.schemas.get(app_id).cloned().unwrap_or_default();
    let hashes = schemas
        .iter()
        .map(|schema| &schema.hash)
        .collect::<Vec<_>>();
    json_response(200, "OK", json!({ "hashes": hashes }))
}

fn handle_get_schema(path: &str, request: &HttpRequest, state: &LoopbackState) -> Vec<u8> {
    let Some((app_id, hash)) = path.strip_prefix("/apps/").and_then(|tail| {
        let (app_id, rest) = tail.split_once("/schema/")?;
        Some((app_id, rest))
    }) else {
        return not_found_response();
    };
    if !admin_secret_matches(request, state.admin_secret.as_deref()) {
        return json_response(401, "Unauthorized", json!({ "error": "unauthorized" }));
    }
    let Some(schema) = state
        .schemas
        .get(app_id)
        .and_then(|schemas| schemas.iter().find(|schema| schema.hash == hash))
    else {
        return json_response(404, "Not Found", json!({ "error": "schema_not_found" }));
    };
    json_response(
        200,
        "OK",
        json!({ "schema": schema.schema, "publishedAt": schema.published_at }),
    )
}

fn admin_secret_matches(request: &HttpRequest, expected: Option<&str>) -> bool {
    matches!(
        (expected, request.headers.get("x-jazz-admin-secret")),
        (Some(expected), Some(actual)) if actual == expected
    )
}

fn hex_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut text = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut text, "{byte:02x}");
    }
    text
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn load_admin_schema_store(
    path: Option<&Path>,
) -> io::Result<HashMap<String, Vec<StoredAdminSchema>>> {
    let Some(path) = path else {
        return Ok(HashMap::new());
    };
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(HashMap::new()),
        Err(error) => Err(error),
    }
}

fn reload_admin_schema_catalogue(
    shell: &mut InMemoryServerShell,
    schemas: &HashMap<String, Vec<StoredAdminSchema>>,
) -> LoopbackHttpResult<()> {
    for schema in schemas.values().flat_map(|schemas| schemas.iter()) {
        let local_schema = convert_admin_schema(&schema.schema).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "stored admin schema {} cannot be loaded into runtime catalogue: {error}",
                    schema.object_id
                ),
            )
        })?;
        shell.publish_runtime_schema(local_schema)?;
    }
    Ok(())
}

fn save_admin_schema_store(
    path: Option<&Path>,
    schemas: &HashMap<String, Vec<StoredAdminSchema>>,
) -> io::Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec_pretty(schemas)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    fs::write(path, bytes)?;
    Ok(())
}

fn handle_frames(path: &str, body: Vec<u8>, state: &mut LoopbackState) -> Vec<u8> {
    let Some(id_part) = path
        .strip_prefix("/sessions/")
        .and_then(|tail| tail.strip_suffix("/frames"))
    else {
        return response(
            404,
            "Not Found",
            "text/plain; charset=utf-8",
            b"not found\n".to_vec(),
        );
    };
    let Ok(session_id) = id_part.parse::<u64>() else {
        return response(
            400,
            "Bad Request",
            "text/plain; charset=utf-8",
            b"invalid session id\n".to_vec(),
        );
    };
    let frames = match parse_hex_frames(&body) {
        Ok(frames) => frames,
        Err(error) => {
            return response(
                400,
                "Bad Request",
                "text/plain; charset=utf-8",
                format!("{error}\n").into_bytes(),
            );
        }
    };

    let Some(session) = state.sessions.get(&session_id).copied() else {
        return response(
            404,
            "Not Found",
            "text/plain; charset=utf-8",
            b"unknown session\n".to_vec(),
        );
    };
    if let Err(error) = state.shell.receive_frames(session, frames) {
        return shell_error_response(error);
    }
    if let Err(error) = state.shell.tick() {
        return shell_error_response(error);
    }
    let outbound = match state.shell.take_frames(session) {
        Ok(frames) => frames,
        Err(error) => return shell_error_response(error),
    };
    response(
        200,
        "OK",
        "text/plain; charset=utf-8",
        render_hex_frames(&outbound).into_bytes(),
    )
}

fn parse_hex_frames(body: &[u8]) -> std::result::Result<Vec<Vec<u8>>, String> {
    let text = std::str::from_utf8(body).map_err(|_| "frame body is not utf-8".to_owned())?;
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(decode_hex)
        .collect()
}

fn decode_hex(text: &str) -> std::result::Result<Vec<u8>, String> {
    if !text.len().is_multiple_of(2) {
        return Err("hex frame has odd length".to_owned());
    }
    let mut bytes = Vec::with_capacity(text.len() / 2);
    for pair in text.as_bytes().chunks_exact(2) {
        let high = hex_value(pair[0]).ok_or("hex frame contains non-hex digit")?;
        let low = hex_value(pair[1]).ok_or("hex frame contains non-hex digit")?;
        bytes.push(high << 4 | low);
    }
    Ok(bytes)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn render_hex_frames(frames: &[Vec<u8>]) -> String {
    let mut body = String::new();
    for frame in frames {
        for byte in frame {
            use std::fmt::Write as _;
            let _ = write!(&mut body, "{byte:02x}");
        }
        body.push('\n');
    }
    body
}

fn render_metrics(metrics: &MetricsSnapshot) -> String {
    format!(
        "active_sessions={}\ntotal_sessions={}\nframes_received={}\nframes_sent={}\nbytes_received={}\nbytes_sent={}\nticks={}\n",
        metrics.active_sessions,
        metrics.total_sessions,
        metrics.frames_received,
        metrics.frames_sent,
        metrics.bytes_received,
        metrics.bytes_sent,
        metrics.ticks
    )
}

fn health_status_name(status: crate::HealthStatus) -> &'static str {
    match status {
        crate::HealthStatus::Ready => "ready",
        crate::HealthStatus::Draining => "draining",
        crate::HealthStatus::Unhealthy => "unhealthy",
    }
}

fn shell_error_response(error: ShellError) -> Vec<u8> {
    response(
        500,
        "Internal Server Error",
        "text/plain; charset=utf-8",
        format!("{error}\n").into_bytes(),
    )
}

fn not_found_response() -> Vec<u8> {
    response(
        404,
        "Not Found",
        "text/plain; charset=utf-8",
        b"not found\n".to_vec(),
    )
}

fn json_response(status: u16, reason: &str, body: Value) -> Vec<u8> {
    response(
        status,
        reason,
        "application/json",
        serde_json::to_vec(&body).expect("admin schema response is valid json"),
    )
}

fn response(status: u16, reason: &str, content_type: &str, body: Vec<u8>) -> Vec<u8> {
    let mut response = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(&body);
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_publish_schema_wires_local_runtime_catalogue_and_keeps_raw_get_response() {
        let mut headers = HashMap::new();
        headers.insert("x-jazz-admin-secret".to_owned(), "secret".to_owned());
        let body = json!({
            "schema": {
                "tables": [
                    {
                        "name": "todos",
                        "columns": [
                            { "name": "title", "type": "string" }
                        ]
                    }
                ]
            }
        });
        let mut state = LoopbackState {
            shell: InMemoryServerShell::start(default_config()).expect("start shell"),
            sessions: HashMap::new(),
            next_session_id: 1,
            admin_secret: Some("secret".to_owned()),
            schema_store_path: None,
            schemas: HashMap::new(),
        };
        let request = HttpRequest {
            method: "POST".to_owned(),
            path: "/apps/app-a/admin/schemas".to_owned(),
            headers: headers.clone(),
            body: serde_json::to_vec(&body).expect("request json"),
        };

        let response = handle_admin_publish_schema(&request.path, &request, &mut state);
        assert_eq!(response_status(&response), 201);
        let published = response_json(&response);
        let local_schema_id = state
            .shell
            .last_published_runtime_schema()
            .expect("schema was published into the runtime catalogue");
        assert_eq!(state.shell.runtime_write_schema_revision(), 1);

        let hash = published["hash"].as_str().expect("hash");
        let get_request = HttpRequest {
            method: "GET".to_owned(),
            path: format!("/apps/app-a/schema/{hash}"),
            headers,
            body: Vec::new(),
        };
        let fetched = handle_get_schema(&get_request.path, &get_request, &state);
        assert_eq!(response_status(&fetched), 200);
        let fetched = response_json(&fetched);
        assert_eq!(fetched["schema"], body["schema"]);
        assert!(fetched.get("localSchemaId").is_none());

        let stored_id = state.schemas["app-a"][0]
            .local_schema_id
            .as_deref()
            .expect("local schema id stored internally");
        assert_eq!(stored_id, local_schema_id.0.to_string());
    }

    #[test]
    fn admin_schema_store_reload_publishes_stored_schemas_into_runtime_catalogue() {
        let schemas = HashMap::from([(
            "app-a".to_owned(),
            vec![
                StoredAdminSchema {
                    hash: "hash-a".to_owned(),
                    object_id: "schema:app-a:hash-a".to_owned(),
                    published_at: 1,
                    schema: json!({
                        "tables": [
                            {
                                "name": "todos",
                                "columns": [
                                    { "name": "title", "type": "string" }
                                ]
                            }
                        ]
                    }),
                    permissions: None,
                    local_schema_id: None,
                },
                StoredAdminSchema {
                    hash: "hash-b".to_owned(),
                    object_id: "schema:app-a:hash-b".to_owned(),
                    published_at: 2,
                    schema: json!({
                        "tables": [
                            {
                                "name": "notes",
                                "columns": [
                                    { "name": "body", "type": "string" }
                                ]
                            }
                        ]
                    }),
                    permissions: None,
                    local_schema_id: None,
                },
            ],
        )]);
        let mut shell = InMemoryServerShell::start(default_config()).expect("start shell");

        reload_admin_schema_catalogue(&mut shell, &schemas).expect("reload stored schemas");

        assert!(shell.last_published_runtime_schema().is_some());
        assert_eq!(shell.runtime_write_schema_revision(), 2);
    }

    fn response_status(response: &[u8]) -> u16 {
        std::str::from_utf8(response)
            .expect("response utf8")
            .split_whitespace()
            .nth(1)
            .expect("status code")
            .parse()
            .expect("numeric status")
    }

    fn response_json(response: &[u8]) -> Value {
        let body = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|position| &response[position + 4..])
            .expect("response body");
        serde_json::from_slice(body).expect("json response")
    }
}
