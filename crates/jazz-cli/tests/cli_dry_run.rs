use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::rc::Rc;
use std::thread;
use std::time::{Duration, Instant};

use futures_util::{FutureExt, StreamExt};
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

fn jazz_server_command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_jazz-server"));
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
        .env_remove("JAZZ_ALLOW_LOCAL_FIRST_AUTH")
        .env_remove("JAZZ_UPSTREAM_URL")
        .env_remove("JAZZ_SERVER_ANONYMOUS_SUBJECT");
    command
}

fn jazz_tools_command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_jazz-tools"));
    command
        .env_remove("JAZZ_SERVER_PORT")
        .env_remove("JAZZ_SERVER_DATA_DIR")
        .env_remove("JAZZ_SERVER_IN_MEMORY")
        .env_remove("JAZZ_ADMIN_SECRET")
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
fn start_jazz_tools_server(data_dir: &Path, bound_port_file: &Path) -> Child {
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
    while !bound_port_file.exists() {
        if let Some(status) = child.try_wait().expect("poll jazz-tools startup") {
            panic!("jazz-tools server exited before binding: {status}");
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("jazz-tools server did not bind within 10s");
        }
        thread::sleep(Duration::from_millis(20));
    }
    child
}

#[cfg(unix)]
fn publish_empty_schema_and_wait_for_live_core(bound_port_file: &Path, data_dir: &Path) {
    let port = std::fs::read_to_string(bound_port_file)
        .expect("read bound port")
        .parse::<u16>()
        .expect("bound port is numeric");
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
    DbIdentity {
        node: NodeUuid::from_bytes([node; 16]),
        // The loopback server authenticates this handshake using the configured
        // static bearer, so the local runtime must use the exact same reserved
        // issuer-and-subject identity as the authority.
        author: AuthorSubject::from_canonical(
            &serde_json::to_string(&(jazz::serving::auth_admission::STATIC_BEARER_ISSUER, subject))
                .expect("serialize canonical static-bearer test identity"),
        )
        .expect("parse canonical static-bearer test identity"),
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
                "--admin-secret",
                "test-admin-secret",
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
            && line.contains("--admin-secret <token>")
            && line.contains("--auth-jwt-ed-public-key-pem <pem>")
    }));
    assert!(lines.iter().any(|line| {
        line.contains(" server <APP_ID>")
            && line.contains("--port <port>")
            && line.contains("--data-dir <dir>")
            && line.contains("--in-memory")
            && line.contains("--admin-secret <token>")
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
    assert!(lines.iter().any(|line| line.contains("JAZZ_ADMIN_SECRET")));
    assert!(
        lines
            .iter()
            .any(|line| line.contains("JAZZ_BACKEND_SECRET"))
    );
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
        .args(["server", "app-a", "--in-memory", "--admin-secret", "secret"])
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
    let mut first = start_jazz_tools_server(&data_dir, &first_port_file);
    publish_empty_schema_and_wait_for_live_core(&first_port_file, &data_dir);

    // SAFETY: `first.id()` names the live child process spawned above.
    let result = unsafe { libc::kill(first.id() as libc::pid_t, libc::SIGTERM) };
    assert_eq!(result, 0, "send SIGTERM to jazz-tools server");
    wait_for_successful_exit(&mut first, Duration::from_secs(10));

    // Reopening the same RocksDB directory proves controlled shutdown released
    // the process-local storage lock rather than merely stopping the listener.
    let second_port_file = temp_dir.path().join("second-port");
    let mut second = start_jazz_tools_server(&data_dir, &second_port_file);
    assert!(data_dir.join("server-shell.rocksdb").is_dir());
    // SAFETY: `second.id()` names the live child process spawned above.
    let result = unsafe { libc::kill(second.id() as libc::pid_t, libc::SIGTERM) };
    assert_eq!(result, 0, "send SIGTERM to restarted jazz-tools server");
    wait_for_successful_exit(&mut second, Duration::from_secs(10));
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
            "--admin-secret",
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
fn websocket_reconnect_resets_structured_terminal_before_live_patches() {
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
    // that it is no longer settled before accepting the reconnect reset.
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

    let reconnect_reset = block_on(subscription.next()).expect("authoritative reconnect reset");
    let SubscriptionEvent::Delta {
        reset,
        added,
        terminal_operations,
        ..
    } = reconnect_reset
    else {
        panic!("expected reconnect reset delta")
    };
    assert!(reset);
    assert!(
        !added.is_empty(),
        "a structured reset publishes its authoritative root relation"
    );
    assert!(terminal_operations.is_empty());
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
fn dry_run_accepts_alpha_aliases() {
    let output = jazz_server_command()
        .args([
            "dry-run",
            "--bind=127.0.0.1:0",
            "--port=1627",
            "--dataDir=/tmp/jazz-server-alias-data",
            "--memory",
            "--ws-path=/alias-sync",
            "--admin-secret=alias-secret",
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
