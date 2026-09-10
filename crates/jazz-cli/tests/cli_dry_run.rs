use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Output, Stdio};
use std::rc::Rc;
use std::thread;
use std::time::{Duration, Instant};

use futures_util::{FutureExt, StreamExt};
use jazz::account_registry::AccountId;
use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, SeededRowIdSource, SubscriptionEvent, WireTransportAdapter,
    block_on,
};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, Query};
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType as PublicColumnType, SchemaBuilder as PublicSchemaBuilder,
    TableSchemaBuilder as PublicTableSchemaBuilder,
};
use jazz::tx::DurabilityTier;
use jazz::wire::{TransportError, WireTransport};
use serde_json::json;
use tungstenite::protocol::Message;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{WebSocket, connect};

mod support;

use support::cargo_binary;

const LOOPBACK_ADMITTED_ACCOUNT: &str = "7c5fd0da-4bd1-4ba9-9203-41e1f0da142c";

fn loopback_admitted_account() -> AccountId {
    AccountId(
        uuid::Uuid::parse_str(LOOPBACK_ADMITTED_ACCOUNT)
            .expect("parse loopback admitted test account"),
    )
}

fn jazz_server_command() -> Command {
    let mut command = Command::new(cargo_binary("jazz-server"));
    command
        .env_remove("JAZZ_SERVER_LISTEN")
        .env_remove("JAZZ_SERVER_PORT")
        .env_remove("JAZZ_SERVER_DATA_DIR")
        .env_remove("JAZZ_SERVER_IN_MEMORY")
        .env_remove("JAZZ_SERVER_WEBSOCKET_PATH")
        .env_remove("JAZZ_SERVER_AUTH_STATIC_BEARER")
        .env_remove("JAZZ_ADMIN_SECRET")
        .env_remove("JAZZ_BACKEND_SECRET")
        .env_remove("JAZZ_SERVER_AUTH_JWT_ED_PUBLIC_KEY_PEM")
        .env_remove("JAZZ_JWT_ISSUER")
        .env_remove("JAZZ_JWT_AUDIENCE")
        .env_remove("JAZZ_ALLOW_LOCAL_FIRST_AUTH")
        .env_remove("JAZZ_UPSTREAM_URL")
        .env_remove("JAZZ_SERVER_ANONYMOUS_SUBJECT");
    command
}

fn server_command_output(command: &mut Command) -> Output {
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn jazz-server server");

    // EOF requests clean shutdown after startup; reap before checking the report.
    drop(child.stdin.take());
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(20));
            }
            result => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("server did not exit cleanly before the deadline: {result:?}");
            }
        }
    }
    child.wait_with_output().expect("collect server report")
}

fn server_command_report(command: &mut Command) -> String {
    let output = server_command_output(command);
    assert!(
        output.status.success(),
        "server failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("server stdout is utf-8")
}

fn jazz_tools_command() -> Command {
    let mut command = Command::new(cargo_binary("jazz-tools"));
    command
        .env_remove("JAZZ_SERVER_PORT")
        .env_remove("JAZZ_SERVER_DATA_DIR")
        .env_remove("JAZZ_SERVER_IN_MEMORY")
        .env_remove("JAZZ_ADMIN_SECRET")
        .env_remove("JAZZ_JWT_ISSUER")
        .env_remove("JAZZ_JWT_AUDIENCE")
        .env_remove("JAZZ_TRUST_PROXY")
        .env_remove("JAZZ_UPSTREAM_URL")
        .env_remove("JAZZ_BOUND_PORT_FILE");
    command
}

#[cfg(unix)]
fn wait_for_successful_exit(child: &mut Child, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().expect("poll jazz-tools server") {
            assert!(status.success(), "jazz-tools server exited with {status}");
            return;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("jazz-tools server did not exit within {timeout:?}");
        }
        thread::sleep(Duration::from_millis(20));
    }
}

#[cfg(unix)]
fn start_jazz_tools_server(data_dir: &Path, bound_port_file: &Path) -> (Child, u16) {
    let mut child = jazz_tools_command()
        .args([
            "server",
            "00000000-0000-0000-0000-000000000001",
            "--port",
            "0",
            "--data-dir",
            data_dir.to_str().expect("temp path is utf-8"),
            "--bound-port-file",
            bound_port_file.to_str().expect("temp path is utf-8"),
            "--shutdown-timeout-secs",
            "1",
            "--admin-secret",
            "sigterm-test-secret",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn jazz-tools server");

    let deadline = Instant::now() + Duration::from_secs(10);
    let port = loop {
        if let Some(status) = child.try_wait().expect("poll jazz-tools startup") {
            panic!("jazz-tools server exited before binding: {status}");
        }
        if let Ok(contents) = std::fs::read_to_string(bound_port_file)
            && let Some(port) = parse_bound_port_record(&contents)
        {
            break port;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("jazz-tools server did not publish a complete bound-port record within 10s");
        }
        thread::sleep(Duration::from_millis(20));
    };
    (child, port)
}

#[cfg(unix)]
fn parse_bound_port_record(contents: &str) -> Option<u16> {
    let record = contents.strip_suffix('\n')?;
    if record.is_empty() || record.contains('\n') {
        return None;
    }
    record.parse::<u16>().ok().filter(|port| *port != 0)
}

#[cfg(unix)]
fn publish_empty_schema_and_wait_for_live_core(port: u16, data_dir: &Path) {
    let body = r#"{"schema":{"tables":{}}}"#;
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect admin schema API");
    write!(
        stream,
        "POST /apps/00000000-0000-0000-0000-000000000001/admin/schemas HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nX-Jazz-Admin-Secret: sigterm-test-secret\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
    .expect("publish schema request");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read schema publish response");
    assert!(
        response.starts_with("HTTP/1.1 201"),
        "schema publication failed: {response}"
    );
    assert!(
        data_dir.join("server-shell.rocksdb").is_dir(),
        "published schema must start a live core backed by RocksDB"
    );
}

fn schema_hex(schema: &JazzSchema) -> String {
    serde_json::to_vec(schema.public_schema())
        .expect("encode public schema")
        .into_iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn structured_schema() -> JazzSchema {
    let source = PublicSchemaBuilder::new()
        .table(PublicTableSchemaBuilder::new("users").column("name", PublicColumnType::Text))
        .table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("owner_id", PublicColumnType::Uuid),
        )
        .build();
    jazz::schema::JazzSchema::new(&source).unwrap()
}

fn empty_schema() -> JazzSchema {
    jazz::schema::JazzSchema::new(&PublicSchemaBuilder::new().build()).unwrap()
}

fn identity_for_subject(node: u8, subject: &str) -> DbIdentity {
    let account = loopback_admitted_account();
    DbIdentity {
        node: NodeUuid::from_bytes([node; 16]),
        // The loopback server authenticates this handshake using the configured
        // static bearer, so the local runtime must use the exact same reserved
        // issuer-and-subject identity as the authority.
        author: AuthorSubject::from_canonical(
            &serde_json::to_string(&(
                account.0.to_string(),
                jazz::serving::auth_admission::STATIC_BEARER_ISSUER,
                subject,
            ))
            .expect("serialize canonical admitted static-bearer test identity"),
        )
        .expect("parse canonical admitted static-bearer test identity"),
    }
}

fn connect_server_ws(ws_url: &str, subject: &str) -> WebSocket<MaybeTlsStream<TcpStream>> {
    let mut last_error = None;
    let (mut socket, response) = {
        let mut connected = None;
        for _ in 0..20 {
            match connect(ws_url) {
                Ok(result) => {
                    connected = Some(result);
                    break;
                }
                Err(error) => {
                    last_error = Some(error);
                    thread::sleep(Duration::from_millis(10));
                }
            }
        }
        connected.unwrap_or_else(|| {
            panic!(
                "connect jazz-server WebSocket listener: {:?}",
                last_error.expect("connection error")
            )
        })
    };
    assert_eq!(response.status().as_u16(), 101);
    if let MaybeTlsStream::Plain(stream) = socket.get_mut() {
        stream
            .set_read_timeout(Some(Duration::from_millis(20)))
            .expect("set read timeout");
    }
    socket
        .send(Message::Text(
            json!({
                "bearerJwt": "test-admin-secret",
                "sub": subject,
                "claims": {}
            })
            .to_string()
            .into(),
        ))
        .expect("send auth handshake");
    socket
}

#[derive(Clone, Default)]
struct QueuedWireTransport {
    queues: Rc<RefCell<WireQueues>>,
}

#[derive(Default)]
struct WireQueues {
    inbound: VecDeque<Vec<u8>>,
    outbound: VecDeque<Vec<u8>>,
}

impl QueuedWireTransport {
    fn drain_outbound(&self) -> Vec<Vec<u8>> {
        self.queues.borrow_mut().outbound.drain(..).collect()
    }

    fn push_inbound(&self, frame: Vec<u8>) {
        self.queues.borrow_mut().inbound.push_back(frame);
    }
}

impl WireTransport for QueuedWireTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.queues.borrow_mut().outbound.push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.queues.borrow_mut().inbound.pop_front()
    }
}

struct ConnectedClient {
    db: Db<MemoryStorage>,
    wire: QueuedWireTransport,
    socket: WebSocket<MaybeTlsStream<TcpStream>>,
}

fn open_connected_client(
    schema: JazzSchema,
    ws_url: &str,
    subject: &str,
    client: DbIdentity,
) -> ConnectedClient {
    let refs = schema.column_families();
    let cf_refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    let db = block_on(Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&cf_refs).expect("valid memory storage families"),
            client,
        )
        .with_id_source(SeededRowIdSource::new(0xc1)),
    ))
    .expect("open client db");
    let wire = QueuedWireTransport::default();
    block_on(db.connect_upstream(Box::new(WireTransportAdapter::current(wire.clone()))));
    let socket = connect_server_ws(ws_url, subject);
    ConnectedClient { db, wire, socket }
}

fn pump_websocket(
    socket: &mut WebSocket<MaybeTlsStream<TcpStream>>,
    db: &Db<MemoryStorage>,
    wire: &QueuedWireTransport,
) -> bool {
    let mut saw_server_frames = false;
    for _ in 0..64 {
        block_on(db.tick()).expect("drive client db");
        let frames = wire.drain_outbound();
        if !frames.is_empty() {
            socket
                .send(Message::Binary(
                    postcard::to_allocvec(&frames).unwrap().into(),
                ))
                .expect("send binary wire frame batch");
        }

        for frame in read_available_binary_frames(socket) {
            saw_server_frames = true;
            wire.push_inbound(frame);
        }

        block_on(db.tick()).expect("apply server frames");
    }
    saw_server_frames
}

fn pump_websocket_once(
    socket: &mut WebSocket<MaybeTlsStream<TcpStream>>,
    db: &Db<MemoryStorage>,
    wire: &QueuedWireTransport,
) -> bool {
    block_on(db.tick()).expect("drive client db");
    let frames = wire.drain_outbound();
    if !frames.is_empty() {
        socket
            .send(Message::Binary(
                postcard::to_allocvec(&frames)
                    .expect("encode wire frame batch")
                    .into(),
            ))
            .expect("send binary wire frame batch");
    }

    let mut saw_server_frames = false;
    for frame in read_available_binary_frames(socket) {
        saw_server_frames = true;
        wire.push_inbound(frame);
    }
    block_on(db.tick()).expect("apply server frames");
    saw_server_frames
}
fn pump_websocket_once_allow_close(
    socket: &mut WebSocket<MaybeTlsStream<TcpStream>>,
    db: &Db<MemoryStorage>,
    wire: &QueuedWireTransport,
) {
    block_on(db.tick()).expect("drive client db");
    let frames = wire.drain_outbound();
    if !frames.is_empty() {
        socket
            .send(Message::Binary(
                postcard::to_allocvec(&frames)
                    .expect("encode wire frame batch")
                    .into(),
            ))
            .expect("send binary wire frame batch");
    }
    for frame in read_available_binary_frames_allow_close(socket) {
        wire.push_inbound(frame);
    }
    block_on(db.tick()).expect("apply server frames");
}

#[cfg(unix)]
fn constrain_receive_buffer(socket: &mut WebSocket<MaybeTlsStream<TcpStream>>, bytes: libc::c_int) {
    let MaybeTlsStream::Plain(stream) = socket.get_mut() else {
        panic!("loopback test must use a plain TCP stream");
    };
    let result = unsafe {
        libc::setsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            (&bytes as *const libc::c_int).cast(),
            std::mem::size_of_val(&bytes) as libc::socklen_t,
        )
    };
    assert_eq!(result, 0, "set loopback receive buffer");
}

fn wait_for_settled_reset(
    client: &mut ConnectedClient,
    subscription: &mut jazz::db::SubscriptionStream,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        pump_websocket_once_allow_close(&mut client.socket, &client.db, &client.wire);
        while let Some(event) = subscription.next().now_or_never().flatten() {
            if matches!(
                event,
                SubscriptionEvent::Delta {
                    reset: true,
                    settled: true,
                    ..
                }
            ) {
                return true;
            }
        }
        if Instant::now() >= deadline {
            return false;
        }
    }
}

fn read_available_binary_frames(socket: &mut WebSocket<MaybeTlsStream<TcpStream>>) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    loop {
        match socket.read() {
            Ok(Message::Binary(batch)) => {
                frames.extend(postcard::from_bytes::<Vec<Vec<u8>>>(&batch).unwrap());
            }
            Ok(Message::Ping(payload)) => socket.send(Message::Pong(payload)).unwrap(),
            Ok(Message::Pong(_)) => {}
            Ok(message) => panic!("unexpected websocket message: {message:?}"),
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                break;
            }
            Err(error) => panic!("read websocket frame: {error}"),
        }
    }
    frames
}
fn read_available_binary_frames_allow_close(
    socket: &mut WebSocket<MaybeTlsStream<TcpStream>>,
) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    loop {
        match socket.read() {
            Ok(Message::Binary(batch)) => {
                frames.extend(postcard::from_bytes::<Vec<Vec<u8>>>(&batch).unwrap());
            }
            Ok(Message::Ping(payload)) => socket.send(Message::Pong(payload)).unwrap(),
            Ok(Message::Pong(_)) => {}
            Ok(Message::Close(_)) => break,
            Ok(message) => panic!("unexpected websocket message: {message:?}"),
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                break;
            }
            Err(error) => panic!("read websocket frame: {error}"),
        }
    }
    frames
}

struct RunningServer {
    child: Child,
    stdin: Option<ChildStdin>,
    ws_url: String,
}

impl RunningServer {
    fn start_schema(schema: &JazzSchema) -> Self {
        let mut child = jazz_server_command()
            .args([
                "serve-loopback-websocket-schema",
                &schema_hex(schema),
                "--in-memory",
                "--auth-static-bearer",
                "test-admin-secret",
                "--loopback-admitted-account",
                LOOPBACK_ADMITTED_ACCOUNT,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn schema websocket server");
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().expect("child stdout");
        let mut reader = BufReader::new(stdout);
        let mut lines = Vec::new();
        let ws_url = loop {
            let mut line = String::new();
            assert_ne!(
                reader.read_line(&mut line).expect("read server stdout"),
                0,
                "server exited before reporting ws_url"
            );
            let line = line.trim_end().to_owned();
            let ws_url = line.strip_prefix("ws_url=").map(str::to_owned);
            lines.push(line);
            if let Some(ws_url) = ws_url {
                break ws_url;
            }
        };
        Self {
            child,
            stdin,
            ws_url,
        }
    }

    fn shutdown(mut self) {
        drop(self.stdin.take());
        let status = self.child.wait().expect("wait for server command");
        assert!(status.success());
    }
}

#[test]
fn help_lists_dev_server_commands() {
    let output = jazz_server_command()
        .arg("--help")
        .output()
        .expect("run jazz-server --help");

    assert!(output.status.success());
    assert!(output.stderr.is_empty());

    let stdout = String::from_utf8(output.stdout).expect("help stdout is utf-8");
    let lines: Vec<&str> = stdout.lines().collect();

    assert!(lines.iter().any(|line| {
        line.contains(" dry-run ")
            && line.contains("--listen <addr>")
            && line.contains("--bind <addr>")
            && line.contains("--port <port>")
            && line.contains("--data-dir <dir>")
            && line.contains("--dataDir <dir>")
            && line.contains("--in-memory")
            && line.contains("--memory")
            && line.contains("--auth-static-bearer <token>")
            && line.contains("--auth-jwt-ed-public-key-pem <pem>")
            && line.contains("--jwt-issuer <issuer>")
            && line.contains("--jwt-audience <audience>")
    }));
    assert!(lines.iter().any(|line| {
        line.contains(" server <APP_ID>")
            && line.contains("--port <port>")
            && line.contains("--data-dir <dir>")
            && line.contains("--in-memory")
            && line.contains("--auth-static-bearer <token>")
    }));
    assert!(lines.iter().any(|line| {
        line.contains(" serve <schema-source-json-hex>")
            && line.contains("--websocket-path <path>")
            && line.contains("--ws-path <path>")
    }));
    assert!(lines.iter().any(|line| {
        line.contains(" dev-server <schema-source-json-hex>")
            && line.contains("same options as serve")
    }));
    assert!(lines.iter().any(|line| {
        line.contains(" serve-loopback-websocket-schema <schema-source-json-hex>")
            && line.contains("--websocket-path <path>")
    }));
    assert!(lines.iter().any(|line| {
        line.contains(
            " serve-loopback-websocket-schema-data-dir <schema-source-json-hex> <data-dir>",
        )
    }));
    assert!(
        lines
            .iter()
            .any(|line| line.contains("JAZZ_SERVER_DATA_DIR"))
    );
    assert!(lines.iter().any(|line| line.contains("JAZZ_SERVER_PORT")));
    assert!(
        lines
            .iter()
            .any(|line| line.contains("JAZZ_SERVER_AUTH_STATIC_BEARER"))
    );
    assert!(!lines.iter().any(|line| line.contains("JAZZ_ADMIN_SECRET")));
    assert!(
        !lines
            .iter()
            .any(|line| line.contains("JAZZ_BACKEND_SECRET"))
    );
    assert!(lines.iter().any(|line| line.contains("JAZZ_JWT_ISSUER")));
    assert!(lines.iter().any(|line| line.contains("JAZZ_JWT_AUDIENCE")));
    assert!(
        lines
            .iter()
            .any(|line| line.contains("JAZZ_ALLOW_LOCAL_FIRST_AUTH"))
    );
    assert!(lines.iter().any(|line| line.contains("JAZZ_UPSTREAM_URL")));
}

#[test]
fn dry_run_accepts_local_first_jwt_public_key() {
    let output = jazz_server_command()
        .args([
            "dry-run",
            "--auth-jwt-ed-public-key-pem",
            "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEA67pupk4AEbEBWrKNQvXpW72yVVQwzh7l86pCW9YzP8I=\n-----END PUBLIC KEY-----\n",
            "--allow-local-first-auth",
            "true",
        ])
        .output()
        .expect("run jazz-server dry-run with local-first jwt public key");

    assert!(output.status.success());
    assert!(output.stderr.is_empty());

    let stdout = String::from_utf8(output.stdout).expect("dry-run stdout is utf-8");
    let lines: Vec<&str> = stdout.lines().collect();
    assert!(lines.contains(&"auth.mode=jwt"));
    assert!(lines.contains(&"auth.allow_local_first_auth=true"));
}

#[test]
fn server_command_reports_missing_app_id_with_usage() {
    let output = jazz_server_command()
        .arg("server")
        .output()
        .expect("run jazz-server server");

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());

    let stderr = String::from_utf8(output.stderr).expect("server stderr is utf-8");
    assert!(stderr.contains("error=missing_app_id"));
    assert!(stderr.contains(" server <APP_ID>"));
}

#[test]
fn server_command_reports_wired_loopback_shape() {
    let mut child = jazz_server_command()
        .args([
            "server",
            "app-a",
            "--in-memory",
            "--auth-static-bearer",
            "secret",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn jazz-server server");

    let stdout = child.stdout.take().expect("child stdout");
    let mut reader = BufReader::new(stdout);
    let mut lines = Vec::new();
    loop {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line).expect("read server stdout");
        assert_ne!(bytes, 0, "server exited before reporting ws_url");
        let line = line.trim_end().to_owned();
        let saw_ws_url = line.starts_with("ws_url=ws://127.0.0.1:");
        lines.push(line);
        if saw_ws_url {
            break;
        }
    }

    drop(child.stdin.take());
    let status = child.wait().expect("wait for server command");
    assert!(status.success());

    assert!(lines.contains(&"command=server".to_owned()));
    assert!(lines.contains(&"app_id=app-a".to_owned()));
    assert!(lines.contains(&"websocket_path=/apps/app-a/ws".to_owned()));
    assert!(lines.contains(&"storage=in-memory".to_owned()));
    assert!(lines.contains(&"auth.mode=static-bearer".to_owned()));
    assert!(lines.contains(&"schema_catalogue=empty".to_owned()));
    assert!(lines.contains(&"runtime_schema_loading=static_empty_schema".to_owned()));
    assert!(!lines.iter().any(|line| line.contains("unimplemented")));
    assert!(
        lines
            .iter()
            .any(|line| line == "ws_url=ws://127.0.0.1:0/apps/app-a/ws"
                || line.starts_with("ws_url=ws://127.0.0.1:") && line.ends_with("/apps/app-a/ws"))
    );
}

#[cfg(unix)]
#[test]
fn jazz_tools_server_sigterm_exits_cleanly_and_releases_storage() {
    let temp_dir = tempfile::tempdir().expect("create server temp dir");
    let data_dir = temp_dir.path().join("data");
    let first_port_file = temp_dir.path().join("first-port");
    let (mut first, first_port) = start_jazz_tools_server(&data_dir, &first_port_file);
    publish_empty_schema_and_wait_for_live_core(first_port, &data_dir);

    // SAFETY: `first.id()` names the live child process spawned above.
    let result = unsafe { libc::kill(first.id() as libc::pid_t, libc::SIGTERM) };
    assert_eq!(result, 0, "send SIGTERM to jazz-tools server");
    wait_for_successful_exit(&mut first, Duration::from_secs(10));

    // Reopening the same RocksDB directory proves controlled shutdown released
    // the process-local storage lock rather than merely stopping the listener.
    let second_port_file = temp_dir.path().join("second-port");
    let (mut second, _second_port) = start_jazz_tools_server(&data_dir, &second_port_file);
    assert!(data_dir.join("server-shell.rocksdb").is_dir());
    // SAFETY: `second.id()` names the live child process spawned above.
    let result = unsafe { libc::kill(second.id() as libc::pid_t, libc::SIGTERM) };
    assert_eq!(result, 0, "send SIGTERM to restarted jazz-tools server");
    wait_for_successful_exit(&mut second, Duration::from_secs(10));
}

/// Bound-port readiness accepts only one complete newline-terminated numeric record.
///
/// This internal parser test is necessary because a process test can observe the
/// readiness file between creation and completion, before any public network API
/// can be contacted.
#[cfg(unix)]
#[test]
fn bound_port_record_waits_for_a_complete_line() {
    assert_eq!(parse_bound_port_record(""), None);
    assert_eq!(parse_bound_port_record("42"), None);
    assert_eq!(parse_bound_port_record("\n"), None);
    assert_eq!(parse_bound_port_record("not-a-port\n"), None);
    assert_eq!(parse_bound_port_record("0\n"), None);
    assert_eq!(parse_bound_port_record("42000\n"), Some(42000));
}

#[test]
fn server_command_defaults_to_data_dir_and_accepts_aliases() {
    let data_dir = std::env::temp_dir().join(format!(
        "jazz-server-command-data-dir-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&data_dir);

    let mut child = jazz_server_command()
        .args([
            "server",
            "app-b",
            "--dataDir",
            data_dir.to_str().expect("temp path is utf-8"),
            "--ws-path",
            "/custom-ws",
            "--auth-static-bearer",
            "secret",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn jazz-server server");

    let stdout = child.stdout.take().expect("child stdout");
    let mut reader = BufReader::new(stdout);
    let mut lines = Vec::new();
    loop {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line).expect("read server stdout");
        assert_ne!(bytes, 0, "server exited before reporting ws_url");
        let line = line.trim_end().to_owned();
        let saw_ws_url = line.starts_with("ws_url=ws://127.0.0.1:");
        lines.push(line);
        if saw_ws_url {
            break;
        }
    }

    drop(child.stdin.take());
    let status = child.wait().expect("wait for server command");
    let _ = std::fs::remove_dir_all(&data_dir);
    assert!(status.success());

    assert!(lines.contains(&"websocket_path=/custom-ws".to_owned()));
    assert!(lines.contains(&"storage=rocksdb".to_owned()));
    assert!(lines.contains(&format!("data_dir={}", data_dir.display())));
    assert!(lines.contains(&"auth.mode=static-bearer".to_owned()));
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("ws_url=ws://127.0.0.1:") && line.ends_with("/custom-ws"))
    );
}

#[test]
fn server_command_isolates_implicit_data_directories_between_apps() {
    let temp_dir = tempfile::tempdir().expect("create server temp dir");
    let data_dirs = ["app-a", "app-b"].map(|app_id| {
        let stdout = server_command_report(
            jazz_server_command()
                .args([
                    "server",
                    app_id,
                    "--listen",
                    "127.0.0.1:0",
                    "--auth-static-bearer",
                    "secret",
                ])
                .current_dir(temp_dir.path()),
        );
        let reported_path = stdout
            .lines()
            .find_map(|line| line.strip_prefix("data_dir="))
            .unwrap_or_else(|| panic!("{app_id} must report durable storage:\n{stdout}"));
        let data_dir = temp_dir.path().join(reported_path);
        assert!(
            data_dir.join("CURRENT").is_file(),
            "{app_id} must initialise the reported store root: {}\n{stdout}",
            data_dir.display()
        );
        data_dir
            .canonicalize()
            .expect("canonicalise initialised store root")
    });

    assert_ne!(
        data_dirs[0], data_dirs[1],
        "different apps must not reuse the same implicit store in a shared working directory"
    );
}

fn app_storage_command(cwd: &Path, app_id: &str) -> Command {
    let mut command = jazz_server_command();
    command
        .args([
            "server",
            app_id,
            "--listen",
            "127.0.0.1:0",
            "--auth-static-bearer",
            "secret",
        ])
        .current_dir(cwd);
    command
}

fn reported_data_directory(stdout: &str) -> &str {
    stdout
        .lines()
        .find_map(|line| line.strip_prefix("data_dir="))
        .unwrap_or_else(|| panic!("server must report durable storage:\n{stdout}"))
}

fn storage_snapshot(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    fn visit(root: &Path, path: &Path, entries: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
        for entry in std::fs::read_dir(path).expect("read storage directory") {
            let path = entry.expect("read storage entry").path();
            let key = path.strip_prefix(root).unwrap().to_owned();
            if path.is_dir() {
                entries.insert(key, None);
                visit(root, &path, entries);
            } else {
                entries.insert(key, Some(std::fs::read(path).expect("read storage bytes")));
            }
        }
    }
    let mut entries = BTreeMap::new();
    visit(root, root, &mut entries);
    entries
}

#[test]
fn server_command_reopens_canonical_app_storage_without_changing_raw_app_identity() {
    let temp_dir = tempfile::tempdir().expect("create server temp dir");
    let named = server_command_report(&mut app_storage_command(temp_dir.path(), "stable-app"));
    let named_again =
        server_command_report(&mut app_storage_command(temp_dir.path(), "stable-app"));
    assert_eq!(
        reported_data_directory(&named),
        reported_data_directory(&named_again)
    );
    let named_path = Path::new(reported_data_directory(&named));
    assert_eq!(named_path.parent(), Some(Path::new("./data/apps")));
    assert!(temp_dir.path().join(named_path).join("CURRENT").is_file());

    // Reopening by the reported canonical identity must address the named app's store.
    let canonical_name = named_path.file_name().unwrap().to_str().unwrap();
    let by_id = server_command_report(&mut app_storage_command(temp_dir.path(), canonical_name));
    assert_eq!(
        reported_data_directory(&named),
        reported_data_directory(&by_id)
    );

    let uuid = "7c5fd0da-4bd1-4ba9-9203-41e1f0da142c";
    for spelling in [uuid, "7C5FD0DA4BD14BA9920341E1F0DA142C"] {
        let stdout = server_command_report(&mut app_storage_command(temp_dir.path(), spelling));
        assert_eq!(
            reported_data_directory(&stdout),
            format!("./data/apps/{uuid}")
        );
        assert!(
            stdout
                .lines()
                .any(|line| line == format!("app_id={spelling}"))
        );
        assert!(
            stdout
                .lines()
                .any(|line| line == format!("websocket_path=/apps/{spelling}/ws"))
        );
    }
    assert!(
        temp_dir
            .path()
            .join(format!("data/apps/{uuid}/CURRENT"))
            .is_file()
    );
    assert_eq!(
        std::fs::read_dir(temp_dir.path().join("data/apps"))
            .unwrap()
            .count(),
        2
    );
}

#[test]
fn server_command_keeps_path_separator_app_names_inside_canonical_storage_root() {
    let temp_dir = tempfile::tempdir().expect("create server temp dir");
    let stdout = server_command_report(&mut app_storage_command(temp_dir.path(), "../nested/app"));
    let relative = Path::new(reported_data_directory(&stdout));
    assert_eq!(relative.parent(), Some(Path::new("./data/apps")));
    let child = relative.file_name().unwrap().to_str().unwrap();
    let id = uuid::Uuid::parse_str(child).expect("storage child is a UUID, not a raw app name");
    assert_eq!(child, id.to_string());
    assert!(temp_dir.path().join(relative).join("CURRENT").is_file());
    assert!(!temp_dir.path().join("data/nested").exists());
}

#[test]
fn server_command_refuses_legacy_default_without_mutation_but_allows_explicit_reopen() {
    let temp_dir = tempfile::tempdir().expect("create server temp dir");
    server_command_report(
        app_storage_command(temp_dir.path(), "legacy-app").args(["--data-dir", "./data"]),
    );
    let before = storage_snapshot(temp_dir.path());
    let output = server_command_output(
        app_storage_command(temp_dir.path(), "new-app").env("JAZZ_SERVER_IN_MEMORY", "false"),
    );
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("ws_url="));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("legacy_default_storage"), "{stderr}");
    assert!(stderr.contains("--data-dir ./data"), "{stderr}");
    assert!(stderr.contains("JAZZ_SERVER_DATA_DIR=./data"), "{stderr}");
    assert_eq!(storage_snapshot(temp_dir.path()), before);
    assert!(!temp_dir.path().join("data/apps").exists());

    let cli = server_command_report(
        app_storage_command(temp_dir.path(), "new-app")
            .env("JAZZ_SERVER_IN_MEMORY", "true")
            .args(["--memory", "--data-dir", "./data"]),
    );
    assert_eq!(reported_data_directory(&cli), "./data");
    let env = server_command_report(
        app_storage_command(temp_dir.path(), "new-app").env("JAZZ_SERVER_DATA_DIR", "./data"),
    );
    assert_eq!(reported_data_directory(&env), "./data");
    assert!(!temp_dir.path().join("data/apps").exists());
}

#[test]
fn server_command_explicit_storage_bypasses_legacy_guard_without_touching_legacy_data() {
    let temp_dir = tempfile::tempdir().expect("create server temp dir");
    let data = temp_dir.path().join("data");
    // Any existing CURRENT entry is legacy evidence, even a directory.
    std::fs::create_dir_all(data.join("CURRENT")).unwrap();
    std::fs::write(data.join("CURRENT/keep"), b"do not adopt").unwrap();
    let before = storage_snapshot(&data);
    let refused = server_command_output(&mut app_storage_command(temp_dir.path(), "new-app"));
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("legacy_default_storage"));
    assert_eq!(storage_snapshot(&data), before);

    let env = server_command_report(
        app_storage_command(temp_dir.path(), "new-app").env("JAZZ_SERVER_DATA_DIR", "./env-data"),
    );
    assert_eq!(reported_data_directory(&env), "./env-data");
    assert!(temp_dir.path().join("env-data/CURRENT").is_file());
    let cli = server_command_report(
        app_storage_command(temp_dir.path(), "new-app")
            .env("JAZZ_SERVER_DATA_DIR", "./unused-env")
            .args(["--in-memory", "--dataDir=./cli-data"]),
    );
    assert_eq!(reported_data_directory(&cli), "./cli-data");
    assert!(temp_dir.path().join("cli-data/CURRENT").is_file());
    let env_memory = server_command_report(
        app_storage_command(temp_dir.path(), "new-app")
            .env("JAZZ_SERVER_IN_MEMORY", "true")
            .env("JAZZ_SERVER_DATA_DIR", "./unused-env"),
    );
    let cli_memory = server_command_report(
        app_storage_command(temp_dir.path(), "new-app")
            .env("JAZZ_SERVER_DATA_DIR", "./unused-env")
            .args(["--dataDir", "./unused-cli", "--memory"]),
    );
    for stdout in [env_memory, cli_memory] {
        assert!(stdout.lines().any(|line| line == "storage=in-memory"));
        assert!(!stdout.lines().any(|line| line.starts_with("data_dir=")));
    }
    assert!(!temp_dir.path().join("unused-env").exists());
    assert!(!temp_dir.path().join("unused-cli").exists());
    assert_eq!(storage_snapshot(&data), before);
    assert!(!data.join("apps").exists());
}

#[cfg(unix)]
#[test]
fn server_command_refuses_dangling_legacy_marker_without_mutation() {
    let temp_dir = tempfile::tempdir().expect("create server temp dir");
    let data = temp_dir.path().join("data");
    std::fs::create_dir(&data).unwrap();
    std::os::unix::fs::symlink("missing-manifest", data.join("CURRENT")).unwrap();
    let output = server_command_output(&mut app_storage_command(temp_dir.path(), "new-app"));
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("legacy_default_storage"));
    assert_eq!(
        std::fs::read_link(data.join("CURRENT")).unwrap(),
        Path::new("missing-manifest")
    );
    assert_eq!(std::fs::read_dir(&data).unwrap().count(), 1);
    assert!(!data.join("apps").exists());
}

#[test]
fn server_command_fails_closed_when_legacy_probe_parent_is_not_a_directory() {
    let temp_dir = tempfile::tempdir().expect("create server temp dir");
    std::fs::write(temp_dir.path().join("data"), b"not a directory").unwrap();
    let before = storage_snapshot(temp_dir.path());
    let output = server_command_output(&mut app_storage_command(temp_dir.path(), "new-app"));
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("ws_url="));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("legacy_default_storage_probe_failed"),
        "{stderr}"
    );
    assert!(stderr.contains("./data/CURRENT"), "{stderr}");
    assert_eq!(storage_snapshot(temp_dir.path()), before);
}

#[test]
fn server_command_honours_environment_storage_and_websocket_route() {
    let temp_dir = tempfile::tempdir().expect("create server temp dir");
    let stdout = server_command_report(
        jazz_server_command()
            .args([
                "server",
                "environment-app",
                "--listen",
                "127.0.0.1:0",
                "--auth-static-bearer",
                "secret",
            ])
            .env("JAZZ_SERVER_IN_MEMORY", "true")
            .env("JAZZ_SERVER_DATA_DIR", "./unused-environment-data")
            .env("JAZZ_SERVER_WEBSOCKET_PATH", "/environment-route")
            .current_dir(temp_dir.path()),
    );
    let lines: Vec<&str> = stdout.lines().collect();
    assert!(
        lines.contains(&"storage=in-memory"),
        "environment must select memory storage:\n{stdout}"
    );
    assert!(
        !lines.iter().any(|line| line.starts_with("data_dir=")),
        "memory storage must not report a data directory:\n{stdout}"
    );
    assert!(
        lines.contains(&"websocket_path=/environment-route"),
        "environment must select the websocket route:\n{stdout}"
    );
    assert!(
        lines.iter().any(|line| {
            line.starts_with("ws_url=ws://127.0.0.1:") && line.ends_with("/environment-route")
        }),
        "server URL must use the selected websocket route:\n{stdout}"
    );
}

#[test]
fn server_command_honours_environment_data_directory_when_memory_is_false() {
    let temp_dir = tempfile::tempdir().expect("create server temp dir");
    let stdout = server_command_report(
        jazz_server_command()
            .args([
                "server",
                "durable-environment-app",
                "--listen",
                "127.0.0.1:0",
                "--auth-static-bearer",
                "secret",
            ])
            .env("JAZZ_SERVER_IN_MEMORY", "false")
            .env("JAZZ_SERVER_DATA_DIR", "./environment-data")
            .env("JAZZ_SERVER_WEBSOCKET_PATH", "/durable-environment")
            .current_dir(temp_dir.path()),
    );
    let lines: Vec<&str> = stdout.lines().collect();
    assert!(lines.contains(&"storage=rocksdb"), "{stdout}");
    assert!(lines.contains(&"data_dir=./environment-data"), "{stdout}");
    assert!(
        lines.contains(&"websocket_path=/durable-environment"),
        "{stdout}"
    );
    assert!(
        lines.iter().any(|line| {
            line.starts_with("ws_url=ws://127.0.0.1:") && line.ends_with("/durable-environment")
        }),
        "{stdout}"
    );
    assert!(temp_dir.path().join("environment-data").is_dir());
    assert!(!temp_dir.path().join("data").exists());
}

#[test]
fn server_command_explicit_data_directory_overrides_environment_and_earlier_memory_flag() {
    let temp_dir = tempfile::tempdir().expect("create server temp dir");
    let stdout = server_command_report(
        jazz_server_command()
            .args([
                "server",
                "explicit-durable-app",
                "--bind=127.0.0.1:0",
                "--memory",
                "--dataDir=./cli-data",
                "--ws-path=/cli-durable",
                "--auth-static-bearer",
                "secret",
            ])
            .env("JAZZ_SERVER_IN_MEMORY", "true")
            .env("JAZZ_SERVER_DATA_DIR", "./unused-environment-data")
            .env("JAZZ_SERVER_WEBSOCKET_PATH", "/unused-environment-route")
            .current_dir(temp_dir.path()),
    );
    let lines: Vec<&str> = stdout.lines().collect();
    assert!(lines.contains(&"storage=rocksdb"), "{stdout}");
    assert!(lines.contains(&"data_dir=./cli-data"), "{stdout}");
    assert!(lines.contains(&"websocket_path=/cli-durable"), "{stdout}");
    assert!(
        lines.iter().any(|line| {
            line.starts_with("ws_url=ws://127.0.0.1:") && line.ends_with("/cli-durable")
        }),
        "{stdout}"
    );
    assert!(temp_dir.path().join("cli-data").is_dir());
    assert!(!temp_dir.path().join("unused-environment-data").exists());
    assert!(!temp_dir.path().join("data").exists());
}

#[test]
fn server_command_explicit_memory_overrides_environment_and_earlier_data_directory_flag() {
    let temp_dir = tempfile::tempdir().expect("create server temp dir");
    let stdout = server_command_report(
        jazz_server_command()
            .args([
                "server",
                "explicit-memory-app",
                "--listen",
                "127.0.0.1:0",
                "--data-dir",
                "./unused-cli-data",
                "--in-memory",
                "--websocket-path",
                "/cli-memory",
                "--auth-static-bearer",
                "secret",
            ])
            .env("JAZZ_SERVER_DATA_DIR", "./unused-environment-data")
            .env("JAZZ_SERVER_WEBSOCKET_PATH", "/unused-environment-route")
            .current_dir(temp_dir.path()),
    );
    let lines: Vec<&str> = stdout.lines().collect();
    assert!(lines.contains(&"storage=in-memory"), "{stdout}");
    assert!(
        !lines.iter().any(|line| line.starts_with("data_dir=")),
        "{stdout}"
    );
    assert!(lines.contains(&"websocket_path=/cli-memory"), "{stdout}");
    assert!(
        lines.iter().any(|line| {
            line.starts_with("ws_url=ws://127.0.0.1:") && line.ends_with("/cli-memory")
        }),
        "{stdout}"
    );
    assert!(!temp_dir.path().join("unused-cli-data").exists());
    assert!(!temp_dir.path().join("unused-environment-data").exists());
    assert!(!temp_dir.path().join("data").exists());
}

fn websocket_reconnect_preserves_local_structured_terminal_patches() {
    let schema = structured_schema();
    let server = RunningServer::start_schema(&schema);
    let subject = "structured-reconnect-user";
    let mut writer = open_connected_client(
        schema.clone(),
        &server.ws_url,
        subject,
        identity_for_subject(0xd1, subject),
    );
    block_on(writer.db.insert(
        "users",
        BTreeMap::from([("name".to_owned(), Value::String("owner".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(RowUuid::from_bytes([0xa1; 16])),
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(writer.db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("first".to_owned())),
            (
                "owner_id".to_owned(),
                Value::Uuid(RowUuid::from_bytes([0xa1; 16]).0),
            ),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(RowUuid::from_bytes([0xb1; 16])),
            ..Default::default()
        },
    ))
    .unwrap();
    assert!(pump_websocket(&mut writer.socket, &writer.db, &writer.wire));

    let mut reader = open_connected_client(
        schema.clone(),
        &server.ws_url,
        subject,
        identity_for_subject(0xd2, subject),
    );
    let query = Query::from("users").array_subquery(ArraySubquery::new(
        "todosViaOwner",
        "todos",
        "owner_id",
        "id",
    ));
    let prepared = reader.db.prepare_query(&query).unwrap();
    let mut subscription = block_on(reader.db.subscribe(
        &prepared,
        ReadOpts {
            tier: DurabilityTier::Global,
            ..Default::default()
        },
    ))
    .unwrap();
    assert!(pump_websocket(&mut reader.socket, &reader.db, &reader.wire));
    let reset = block_on(subscription.next()).expect("structured reset event");
    assert!(matches!(
        reset,
        SubscriptionEvent::Delta {
            reset: true,
            terminal_operations,
            ..
        } if terminal_operations.is_empty()
    ));
    while subscription.next().now_or_never().flatten().is_some() {}

    // Break the actual socket while retaining this Db, its terminal cache,
    // and the same SubscriptionStream.
    drop(reader.socket);

    block_on(writer.db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("second".to_owned())),
            (
                "owner_id".to_owned(),
                Value::Uuid(RowUuid::from_bytes([0xa1; 16]).0),
            ),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(RowUuid::from_bytes([0xb2; 16])),
            ..Default::default()
        },
    ))
    .unwrap();
    assert!(pump_websocket(&mut writer.socket, &writer.db, &writer.wire));

    let reconnected_wire = QueuedWireTransport::default();
    block_on(
        reader
            .db
            .connect_upstream(Box::new(WireTransportAdapter::current(
                reconnected_wire.clone(),
            ))),
    );
    reader.wire = reconnected_wire;
    reader.socket = connect_server_ws(&server.ws_url, subject);
    assert!(pump_websocket(&mut reader.socket, &reader.db, &reader.wire));

    // Replacing the upstream invalidates the old authority receipt before the
    // new link can speak. Preserve the cached structured value, but publish
    // that it is no longer settled before accepting the new supporting set.
    let reconnect_demoted = block_on(subscription.next()).expect("reconnect authority demotion");
    assert!(matches!(
        reconnect_demoted,
        SubscriptionEvent::Delta {
            reset: false,
            added,
            updated,
            removed,
            terminal_operations,
            settled: false,
            ..
        } if added.is_empty()
            && updated.is_empty()
            && removed.is_empty()
            && terminal_operations.is_empty()
    ));

    let reconnect_reset = block_on(subscription.next()).expect("authoritative reconnect delta");
    let SubscriptionEvent::Delta {
        reset,
        added,
        terminal_operations,
        ..
    } = reconnect_reset
    else {
        panic!("expected reconnect reset delta")
    };
    // The local evaluator and its terminal cache survived the socket change.
    // A complete supporting snapshot produces a local patch against that cache;
    // no upstream terminal instructions or new root reset are needed.
    assert!(!reset);
    assert!(added.is_empty());
    assert!(!terminal_operations.is_empty());
    while subscription.next().now_or_never().flatten().is_some() {}

    block_on(writer.db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("third".to_owned())),
            (
                "owner_id".to_owned(),
                Value::Uuid(RowUuid::from_bytes([0xa1; 16]).0),
            ),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(RowUuid::from_bytes([0xb3; 16])),
            ..Default::default()
        },
    ))
    .unwrap();
    assert!(pump_websocket(&mut writer.socket, &writer.db, &writer.wire));
    assert!(pump_websocket(&mut reader.socket, &reader.db, &reader.wire));
    let mut patch = None;
    while let Some(event) = subscription.next().now_or_never().flatten() {
        if matches!(
            &event,
            SubscriptionEvent::Delta {
                reset: false,
                terminal_operations,
                ..
            } if !terminal_operations.is_empty()
        ) {
            patch = Some(event);
            break;
        }
    }
    let patch = patch.expect("structured patch event");
    assert!(
        matches!(
            &patch,
            SubscriptionEvent::Delta {
                reset: false,
                added,
                updated,
                removed,
                terminal_operations,
                ..
            } if added.is_empty()
                && updated.is_empty()
                && removed.is_empty()
                && !terminal_operations.is_empty()
        ),
        "unexpected post-reconnect structured event: {patch:?}"
    );
    let SubscriptionEvent::Delta {
        terminal_operations,
        ..
    } = patch
    else {
        unreachable!()
    };
    assert!(matches!(
        terminal_operations.as_slice(),
        [jazz::groove::ivm::TerminalOperation {
            path,
            edit: jazz::groove::ivm::TerminalEdit::Insert { index: 2, .. },
            ..
        }] if path == &[jazz::groove::ivm::TerminalPathSegment::Collection(
            "todosViaOwner".to_owned()
        )]
    ));

    drop(reader.socket);
    drop(writer.socket);
    server.shutdown();
}

/// The first client receives a multi-megabyte subscription response while it
/// deliberately advertises a tiny TCP receive window and never reads it.
/// Client B's independent reset must still complete before A is drained, and
/// A's rows must arrive in their original order once the window is opened.
#[cfg(unix)]
#[test]
fn bug_196_backpressured_client_does_not_block_independent_client_and_preserves_fifo() {
    const ROW_COUNT: usize = 64;
    const PAYLOAD_BYTES: usize = 48 * 1024;
    let make_payload = |index: usize| {
        let mut payload = format!("{index:03}:");
        let mut state = index as u64 + 0x9e37_79b9_7f4a_7c15;
        for _ in 0..(PAYLOAD_BYTES - 4) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            payload.push(char::from(b'a' + state as u8 % 26));
        }
        payload
    };

    let schema = structured_schema();
    let server = RunningServer::start_schema(&schema);

    let mut seed = open_connected_client(
        schema.clone(),
        &server.ws_url,
        "bug-196-stalled",
        identity_for_subject(0xa0, "bug-196-stalled"),
    );
    let mut seeded_writes = Vec::with_capacity(ROW_COUNT);
    for index in 0..ROW_COUNT {
        let payload = make_payload(index);
        let write = block_on(seed.db.insert(
            "users",
            BTreeMap::from([("name".to_owned(), Value::String(payload))]),
            jazz::db::InsertOptions {
                row_id: Some(RowUuid::from_bytes([index as u8; 16])),
                ..Default::default()
            },
        ))
        .expect("stage backpressure fixture row");
        seeded_writes.push(write);
    }
    let mut sent_seed_update = false;
    for _ in 0..8 {
        block_on(seed.db.tick()).expect("queue backpressure fixture rows");
        let frames = seed.wire.drain_outbound();
        if !frames.is_empty() {
            seed.socket
                .send(Message::Binary(
                    postcard::to_allocvec(&frames)
                        .expect("encode backpressure fixture rows")
                        .into(),
                ))
                .expect("send backpressure fixture rows");
            sent_seed_update = true;
        }
    }
    assert!(sent_seed_update, "seed client must send its fixture rows");

    // Settle the fixture before opening A, so A's protocol handshake cannot
    // contend with seed admission.
    let settlement_deadline = Instant::now() + Duration::from_secs(3);
    let mut seed_settled = false;
    while Instant::now() < settlement_deadline {
        pump_websocket_once_allow_close(&mut seed.socket, &seed.db, &seed.wire);
        if seeded_writes
            .iter()
            .all(|write| block_on(write.wait(DurabilityTier::Global)).is_ok())
        {
            seed_settled = true;
            break;
        }
    }
    assert!(
        seed_settled,
        "seed fixture writes must settle globally before client A opens"
    );

    let mut stalled = open_connected_client(
        schema.clone(),
        &server.ws_url,
        "bug-196-stalled",
        identity_for_subject(0xa2, "bug-196-stalled"),
    );
    block_on(stalled.db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("handshake".to_owned())),
            (
                "owner_id".to_owned(),
                Value::Uuid(RowUuid::from_bytes([0xfe; 16]).0),
            ),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(RowUuid::from_bytes([0xfe; 16])),
            ..Default::default()
        },
    ))
    .expect("stage stalled client's handshake mutation");
    assert!(pump_websocket(
        &mut stalled.socket,
        &stalled.db,
        &stalled.wire
    ));
    constrain_receive_buffer(&mut stalled.socket, 1024);

    let data_query = stalled.db.prepare_query(&Query::from("users")).unwrap();
    let mut stalled_data_subscription = block_on(stalled.db.subscribe(
        &data_query,
        ReadOpts {
            tier: DurabilityTier::Global,
            ..Default::default()
        },
    ))
    .unwrap();
    let auxiliary_query = stalled
        .db
        .prepare_query(&Query::from("users").limit(ROW_COUNT + 1))
        .unwrap();
    let _auxiliary_subscription = block_on(stalled.db.subscribe(
        &auxiliary_query,
        ReadOpts {
            tier: DurabilityTier::Global,
            ..Default::default()
        },
    ))
    .unwrap();
    let mut sent_data_subscription = false;
    for _ in 0..8 {
        block_on(stalled.db.tick()).expect("queue stalled data subscription");
        let frames = stalled.wire.drain_outbound();
        if !frames.is_empty() {
            stalled
                .socket
                .send(Message::Binary(
                    postcard::to_allocvec(&frames)
                        .expect("encode stalled data subscription")
                        .into(),
                ))
                .expect("send stalled data subscription");
            sent_data_subscription = true;
        }
    }
    assert!(
        sent_data_subscription,
        "stalled client must announce its data subscription"
    );
    let _trigger_write = block_on(stalled.db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("trigger".to_owned())),
            (
                "owner_id".to_owned(),
                Value::Uuid(RowUuid::from_bytes([0xfd; 16]).0),
            ),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(RowUuid::from_bytes([0xfd; 16])),
            ..Default::default()
        },
    ))
    .expect("stage A's unread response trigger");
    let mut sent_trigger = false;
    for _ in 0..8 {
        block_on(stalled.db.tick()).expect("queue A's unread response trigger");
        let frames = stalled.wire.drain_outbound();
        if !frames.is_empty() {
            stalled
                .socket
                .send(Message::Binary(
                    postcard::to_allocvec(&frames)
                        .expect("encode A's unread response trigger")
                        .into(),
                ))
                .expect("send A's unread response trigger");
            sent_trigger = true;
        }
    }
    assert!(sent_trigger, "A must send its response trigger");

    // Let the server reach A's unread large reset before B is introduced.
    thread::sleep(Duration::from_millis(250));

    let mut independent = open_connected_client(
        schema.clone(),
        &server.ws_url,
        "bug-196-independent",
        identity_for_subject(0xa2, "bug-196-independent"),
    );
    let control_query = independent.db.prepare_query(&Query::from("todos")).unwrap();
    let mut independent_subscription = block_on(independent.db.subscribe(
        &control_query,
        ReadOpts {
            tier: DurabilityTier::Global,
            ..Default::default()
        },
    ))
    .unwrap();

    // This must complete while A remains unread. On the buggy server,
    // service_connection keeps the shell mutex across A's blocked send, so
    // B's subscription cannot reach its independent reset.
    assert!(
        wait_for_settled_reset(
            &mut independent,
            &mut independent_subscription,
            Duration::from_secs(3),
        ),
        "client B must complete while client A remains backpressured"
    );

    // Reopen A's receive window only after B has completed. This releases the
    // blocked WebSocket frame so the strict FIFO assertion can consume it.
    constrain_receive_buffer(&mut stalled.socket, 8 * 1024 * 1024);
    if let MaybeTlsStream::Plain(stream) = stalled.socket.get_mut() {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("increase A read timeout before draining its large frame");
    }

    let users_table = schema
        .tables()
        .iter()
        .find(|table| table.name == "users")
        .expect("users table");
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut saw_server_frames = false;
    let mut observed_payloads = Vec::new();
    while observed_payloads.len() < ROW_COUNT && Instant::now() < deadline {
        saw_server_frames |= pump_websocket_once(&mut stalled.socket, &stalled.db, &stalled.wire);
        while let Some(event) = stalled_data_subscription.next().now_or_never().flatten() {
            let SubscriptionEvent::Delta { added, .. } = event else {
                continue;
            };
            for row in added {
                let Some(Value::String(payload)) = row.cell(users_table, "name") else {
                    panic!("FIFO reset row must contain its payload");
                };
                observed_payloads.push(payload);
            }
        }
    }
    assert!(
        saw_server_frames,
        "A must receive server frames after reopening"
    );

    let observed_indices = observed_payloads
        .iter()
        .map(|payload| {
            payload
                .get(..3)
                .and_then(|prefix| prefix.parse::<usize>().ok())
        })
        .collect::<Option<Vec<_>>>();
    let expected_indices = Some((0..ROW_COUNT).collect::<Vec<_>>());
    assert_eq!(
        observed_indices, expected_indices,
        "client A's eventual subscription batches must stay FIFO"
    );
    drop(stalled.socket);
    drop(independent.socket);
    drop(seed.socket);
    server.shutdown();
}

#[test]
fn dry_run_prints_stable_report() {
    let output = jazz_server_command()
        .arg("dry-run")
        .output()
        .expect("run jazz-server dry-run");

    assert!(output.status.success());
    assert!(output.stderr.is_empty());

    let stdout = String::from_utf8(output.stdout).expect("dry-run stdout is utf-8");
    let lines: Vec<&str> = stdout.lines().collect();

    assert!(lines.contains(&"command=dry-run"));
    assert!(lines.contains(&"role=core"));
    assert!(lines.contains(&"profile=local"));
    assert!(lines.contains(&"listener=127.0.0.1:0"));
    assert!(lines.contains(&"storage=in-memory"));
    assert!(lines.contains(&"runtime_plan.core_role=core"));
    assert!(lines.contains(&"runtime_plan.profile=local"));
    assert!(lines.contains(&"runtime_plan.storage_kind=in-memory"));
    assert!(lines.contains(&"runtime_plan.schema_column_family_count=0"));
    assert!(lines.contains(&"health.status=ready"));
    assert!(lines.contains(&"health.role=core"));
    assert!(lines.contains(&"health.profile=local"));
    assert!(lines.contains(&"health.drain_state=running"));
    assert!(lines.contains(&"health.message=ready"));
    assert!(lines.contains(&"metrics.active_sessions=0"));
    assert!(lines.contains(&"metrics.total_sessions=0"));
    assert!(lines.contains(&"metrics.rejected_sessions=0"));
    assert!(lines.contains(&"sockets_bound=false"));
    assert!(lines.contains(&"storage_opened=false"));
    assert!(lines.contains(&"runtime_started=false"));
    assert!(lines.contains(&"auth.mode=anonymous"));
    assert!(lines.contains(&"auth.allow_local_first_auth=false"));
    assert!(lines.contains(&"auth.anonymous_subject=anonymous"));
}

#[test]
fn dry_run_accepts_alpha_cli_flags_without_opening_storage() {
    let data_dir = std::env::temp_dir().join(format!(
        "jazz-server-dry-run-data-dir-{}",
        std::process::id()
    ));
    let output = jazz_server_command()
        .args([
            "dry-run",
            "--listen",
            "127.0.0.1:1625",
            "--data-dir",
            data_dir.to_str().expect("temp path is utf-8"),
            "--websocket-path",
            "/sync-alpha",
            "--auth-static-bearer",
            "secret",
            "--anonymous-subject",
            "dev-user",
        ])
        .output()
        .expect("run jazz-server dry-run with alpha flags");

    assert!(output.status.success());
    assert!(output.stderr.is_empty());

    let stdout = String::from_utf8(output.stdout).expect("dry-run stdout is utf-8");
    let lines: Vec<&str> = stdout.lines().collect();
    assert!(lines.contains(&"listener=127.0.0.1:1625"));
    assert!(lines.contains(&"storage=rocksdb"));
    assert!(lines.contains(&"runtime_plan.storage_kind=rocksdb"));
    assert!(lines.contains(&"storage_opened=false"));
    assert!(lines.contains(&"auth.mode=static-bearer"));
    assert!(lines.contains(&"auth.allow_local_first_auth=false"));
    assert!(lines.contains(&"auth.anonymous_subject=dev-user"));
}

#[test]
fn dry_run_reads_alpha_env_and_cli_can_override_storage() {
    let output = jazz_server_command()
        .arg("dry-run")
        .arg("--in-memory")
        .env("JAZZ_SERVER_LISTEN", "127.0.0.1:1626")
        .env("JAZZ_SERVER_DATA_DIR", "/tmp/jazz-server-env-data")
        .env("JAZZ_SERVER_WEBSOCKET_PATH", "/env-sync")
        .env("JAZZ_SERVER_AUTH_STATIC_BEARER", "env-secret")
        .env("JAZZ_ALLOW_LOCAL_FIRST_AUTH", "true")
        .output()
        .expect("run jazz-server dry-run with env");

    assert!(output.status.success());
    assert!(output.stderr.is_empty());

    let stdout = String::from_utf8(output.stdout).expect("dry-run stdout is utf-8");
    let lines: Vec<&str> = stdout.lines().collect();
    assert!(lines.contains(&"listener=127.0.0.1:1626"));
    assert!(lines.contains(&"storage=in-memory"));
    assert!(lines.contains(&"auth.mode=static-bearer"));
    assert!(lines.contains(&"auth.allow_local_first_auth=true"));
}

#[test]
fn bug_306_rejects_privileged_secret_aliases_with_actionable_replacements() {
    let cases = [
        ("JAZZ_ADMIN_SECRET", "JAZZ_SERVER_AUTH_STATIC_BEARER"),
        ("JAZZ_BACKEND_SECRET", "JAZZ_SERVER_AUTH_STATIC_BEARER"),
    ];

    for (secret_env, replacement) in cases {
        let output = jazz_server_command()
            .arg("dry-run")
            .env(secret_env, "privileged-secret")
            .output()
            .expect("run jazz-server dry-run with privileged secret env");

        assert_eq!(
            output.status.code(),
            Some(2),
            "{secret_env} must not become an ordinary bearer credential"
        );
        assert!(output.stdout.is_empty());
        let stderr = String::from_utf8(output.stderr).expect("dry-run stderr is utf-8");
        assert!(stderr.contains(secret_env), "{stderr}");
        assert!(stderr.contains(replacement), "{stderr}");
    }

    let output = jazz_server_command()
        .args(["dry-run", "--admin-secret", "privileged-secret"])
        .output()
        .expect("run jazz-server dry-run with admin secret flag");
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).expect("dry-run stderr is utf-8");
    assert!(stderr.contains("--admin-secret"), "{stderr}");
    assert!(stderr.contains("--auth-static-bearer"), "{stderr}");
}

#[test]
fn dry_run_rejects_upstream_url_for_local_server_mode() {
    let output = jazz_server_command()
        .args(["dry-run", "--upstream-url", "wss://example.invalid/sync"])
        .output()
        .expect("run jazz-server dry-run with upstream url");

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());

    let stderr = String::from_utf8(output.stderr).expect("dry-run stderr is utf-8");
    assert!(stderr.contains("error=unsupported_upstream_url=wss://example.invalid/sync"));
    assert!(stderr.contains("local-only"));
}

#[test]
fn dry_run_accepts_non_privileged_alpha_aliases() {
    let output = jazz_server_command()
        .args([
            "dry-run",
            "--bind=127.0.0.1:0",
            "--port=1627",
            "--dataDir=/tmp/jazz-server-alias-data",
            "--memory",
            "--ws-path=/alias-sync",
            "--static-bearer=alias-secret",
            "--allow-local-first-auth=true",
            "--anonymous-subject=alias-user",
        ])
        .output()
        .expect("run jazz-server dry-run with alpha aliases");

    assert!(output.status.success());
    assert!(output.stderr.is_empty());

    let stdout = String::from_utf8(output.stdout).expect("dry-run stdout is utf-8");
    let lines: Vec<&str> = stdout.lines().collect();
    assert!(lines.contains(&"listener=127.0.0.1:1627"));
    assert!(lines.contains(&"storage=in-memory"));
    assert!(lines.contains(&"auth.mode=static-bearer"));
    assert!(lines.contains(&"auth.allow_local_first_auth=true"));
    assert!(lines.contains(&"auth.anonymous_subject=alias-user"));
}

#[test]
fn loopback_websocket_schema_rejects_bad_hex_without_serving() {
    let output = jazz_server_command()
        .args(["serve-loopback-websocket-schema", "xx"])
        .output()
        .expect("run jazz-server serve-loopback-websocket-schema");

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());

    let stderr = String::from_utf8(output.stderr).expect("schema stderr is utf-8");
    assert!(stderr.contains("error=hex input contains non-hex digit"));
    assert!(stderr.contains(" serve-loopback-websocket-schema <schema-source-json-hex>"));
}

#[test]
fn dev_server_alias_rejects_bad_hex_without_serving() {
    let output = jazz_server_command()
        .args(["dev-server", "xx"])
        .output()
        .expect("run jazz-server dev-server");

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());

    let stderr = String::from_utf8(output.stderr).expect("schema stderr is utf-8");
    assert!(stderr.contains("error=hex input contains non-hex digit"));
    assert!(stderr.contains(" dev-server <schema-source-json-hex>"));
}

#[test]
fn serve_aliases_report_missing_schema_with_command_usage() {
    for command in ["serve", "dev-server", "serve-loopback-websocket-schema"] {
        let output = jazz_server_command()
            .arg(command)
            .output()
            .unwrap_or_else(|error| panic!("run jazz-server {command}: {error}"));

        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());

        let stderr = String::from_utf8(output.stderr).expect("missing schema stderr is utf-8");
        assert!(stderr.contains("error=missing_schema"));
        assert!(stderr.contains(&format!(" {command} <schema-source-json-hex>")));
    }
}

#[test]
fn durable_loopback_websocket_command_reports_missing_arguments() {
    let missing_schema = jazz_server_command()
        .arg("serve-loopback-websocket-schema-data-dir")
        .output()
        .expect("run durable command without schema");

    assert_eq!(missing_schema.status.code(), Some(2));
    assert!(missing_schema.stdout.is_empty());

    let stderr = String::from_utf8(missing_schema.stderr).expect("missing schema stderr is utf-8");
    assert!(stderr.contains("error=missing_schema"));
    assert!(
        stderr.contains(
            " serve-loopback-websocket-schema-data-dir <schema-source-json-hex> <data-dir>"
        )
    );

    let missing_data_dir = jazz_server_command()
        .args(["serve-loopback-websocket-schema-data-dir", "00"])
        .output()
        .expect("run durable command without data-dir");

    assert_eq!(missing_data_dir.status.code(), Some(2));
    assert!(missing_data_dir.stdout.is_empty());

    let stderr =
        String::from_utf8(missing_data_dir.stderr).expect("missing data-dir stderr is utf-8");
    assert!(stderr.contains("error=missing_data_dir"));
    assert!(
        stderr.contains(
            " serve-loopback-websocket-schema-data-dir <schema-source-json-hex> <data-dir>"
        )
    );
}

#[test]
fn durable_loopback_websocket_command_rejects_unopenable_data_dir() {
    let schema_hex = schema_hex(&empty_schema());
    let data_dir = std::env::temp_dir().join(format!(
        "jazz-server-unopenable-data-dir-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&data_dir);
    std::fs::write(&data_dir, b"not a directory").expect("create unopenable data-dir file");
    let output = jazz_server_command()
        .args([
            "serve-loopback-websocket-schema-data-dir",
            &schema_hex,
            data_dir.to_str().expect("temp path is utf-8"),
        ])
        .output()
        .expect("run jazz-server serve-loopback-websocket-schema-data-dir");
    let _ = std::fs::remove_file(&data_dir);

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());

    let stderr = String::from_utf8(output.stderr).expect("durable command stderr is utf-8");
    assert!(stderr.contains("error=loopback WebSocket shell error"));
}
