use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::io::{self, BufRead, BufReader};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::rc::Rc;
use std::thread;
use std::time::Duration;

use futures_util::{FutureExt, StreamExt};
use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, RowCells, SeededRowIdSource, SubscriptionEvent,
    SubscriptionStream, WireTransportAdapter, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::schema::ColumnType;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::schema::JazzSchema;
use jazz::schema::{ColumnSchema, TableSchema};
use jazz::tx::{DurabilityTier, Fate};
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

fn schema_hex(schema: &JazzSchema) -> String {
    postcard::to_allocvec(schema)
        .expect("encode schema")
        .into_iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn todos_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
        ],
    )])
}

fn identity(node: u8, author: u8) -> DbIdentity {
    DbIdentity {
        node: NodeUuid::from_bytes([node; 16]),
        author: AuthorId::from_bytes([author; 16]),
    }
}

fn todo_cells(title: &str, done: bool) -> RowCells {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("done".to_owned(), Value::Bool(done)),
    ])
}

fn author_hex(author: AuthorId) -> String {
    author
        .0
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

fn connect_server_ws(ws_url: &str, author: AuthorId) -> WebSocket<MaybeTlsStream<TcpStream>> {
    let url = format!("{ws_url}?identity={}", author_hex(author));
    let mut last_error = None;
    let (mut socket, response) = 'connect: loop {
        for _ in 0..20 {
            match connect(&url) {
                Ok(connected) => break 'connect connected,
                Err(error) => {
                    last_error = Some(error);
                    thread::sleep(Duration::from_millis(10));
                }
            }
        }
        panic!(
            "connect jazz-server WebSocket listener: {:?}",
            last_error.expect("connection error")
        );
    };
    assert_eq!(response.status().as_u16(), 101);
    if let MaybeTlsStream::Plain(stream) = socket.get_mut() {
        stream
            .set_read_timeout(Some(Duration::from_millis(20)))
            .expect("set read timeout");
    }
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

fn open_connected_client(schema: JazzSchema, ws_url: &str, client: DbIdentity) -> ConnectedClient {
    let refs = schema.column_families();
    let cf_refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    let db = block_on(Db::open(
        DbConfig::new(schema, MemoryStorage::new(&cf_refs), client)
            .with_id_source(SeededRowIdSource::new(0xc1)),
    ))
    .expect("open client db");
    let wire = QueuedWireTransport::default();
    db.connect_upstream(Box::new(WireTransportAdapter::current(wire.clone())));
    let socket = connect_server_ws(ws_url, client.author);
    ConnectedClient { db, wire, socket }
}

fn pump_websocket(
    socket: &mut WebSocket<MaybeTlsStream<TcpStream>>,
    db: &Db<MemoryStorage>,
    wire: &QueuedWireTransport,
) -> bool {
    let mut saw_server_frames = false;
    for _ in 0..64 {
        db.tick().expect("drive client db");
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

        db.tick().expect("apply server frames");
    }
    saw_server_frames
}

fn subscription_fields(
    subscription: &mut SubscriptionStream,
    table: &TableSchema,
) -> Vec<(String, bool)> {
    let mut rows = BTreeMap::<RowUuid, (String, bool)>::new();
    while let Some(event) = subscription.next().now_or_never() {
        let Some(event) = event else {
            break;
        };
        match event {
            SubscriptionEvent::Opened { current, .. }
            | SubscriptionEvent::Reset { current, .. } => {
                rows.clear();
                ingest_current_rows(&mut rows, table, current);
            }
            SubscriptionEvent::Delta {
                added,
                updated,
                removed,
                ..
            } => {
                for removed in removed {
                    rows.remove(&removed.row_uuid);
                }
                ingest_current_rows(&mut rows, table, added);
                ingest_current_rows(&mut rows, table, updated);
            }
            SubscriptionEvent::Closed => break,
        }
    }
    let mut fields = rows.into_values().collect::<Vec<_>>();
    fields.sort();
    fields
}

fn ingest_current_rows(
    rows: &mut BTreeMap<RowUuid, (String, bool)>,
    table: &TableSchema,
    current: Vec<jazz::node::CurrentRow>,
) {
    for row in current {
        let Some(Value::String(title)) = row.cell(table, "title") else {
            panic!("expected title");
        };
        let Some(Value::Bool(done)) = row.cell(table, "done") else {
            panic!("expected done");
        };
        rows.insert(row.row_uuid(), (title, done));
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

struct RunningServer {
    child: Child,
    stdin: Option<ChildStdin>,
    ws_url: String,
    lines: Vec<String>,
}

impl RunningServer {
    fn start(app_id: &str, data_dir: &Path) -> Self {
        let mut child = jazz_server_command()
            .args([
                "server",
                app_id,
                "--data-dir",
                data_dir.to_str().expect("temp path is utf-8"),
                "--allow-legacy-query-identity",
                "true",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn jazz-server server");
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().expect("child stdout");
        let mut reader = BufReader::new(stdout);
        let mut lines = Vec::new();
        let ws_url = loop {
            let mut line = String::new();
            let bytes = reader.read_line(&mut line).expect("read server stdout");
            assert_ne!(bytes, 0, "server exited before reporting ws_url");
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
            lines,
        }
    }

    fn shutdown(mut self) {
        drop(self.stdin.take());
        let status = self.child.wait().expect("wait for server command");
        assert!(status.success());
    }
}

fn publish_schema_to_data_dir(app_id: &str, data_dir: &Path) {
    std::fs::create_dir_all(data_dir).expect("create durable data dir");
    let schema = json!({
        "todos": {
            "columns": [
                { "name": "title", "column_type": "Text" },
                { "name": "done", "column_type": "Boolean" }
            ]
        }
    });
    let store = json!({
        app_id: [
            {
                "hash": "seeded-lifecycle-schema",
                "objectId": format!("schema:{app_id}:seeded-lifecycle-schema"),
                "publishedAt": 1,
                "schema": schema,
                "permissions": null
            }
        ]
    });
    std::fs::write(
        data_dir.join("admin-schemas.json"),
        serde_json::to_vec_pretty(&store).expect("encode schema store"),
    )
    .expect("write durable schema store");
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
        line.contains(" serve <schema-postcard-hex>")
            && line.contains("--websocket-path <path>")
            && line.contains("--ws-path <path>")
    }));
    assert!(lines.iter().any(|line| {
        line.contains(" dev-server <schema-postcard-hex>") && line.contains("same options as serve")
    }));
    assert!(lines.iter().any(|line| {
        line.contains(" serve-loopback-websocket-schema <schema-postcard-hex>")
            && line.contains("--websocket-path <path>")
    }));
    assert!(lines.iter().any(|line| {
        line.contains(" serve-loopback-websocket-schema-data-dir <schema-postcard-hex> <data-dir>")
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
    assert!(lines.contains(&"admin_schema_api=not_started".to_owned()));
    assert!(lines.contains(&"admin_schema_store=not_opened".to_owned()));
    assert!(lines.contains(&"admin_schema_owner=loopback_http_only".to_owned()));
    assert!(!lines.iter().any(|line| line.contains("unimplemented")));
    assert!(
        lines
            .iter()
            .any(|line| line == "ws_url=ws://127.0.0.1:0/apps/app-a/ws"
                || line.starts_with("ws_url=ws://127.0.0.1:") && line.ends_with("/apps/app-a/ws"))
    );
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
    assert!(lines.contains(&"admin_schema_api=not_started".to_owned()));
    assert!(lines.contains(&"admin_schema_store=not_opened".to_owned()));
    assert!(lines.contains(&"admin_schema_owner=loopback_http_only".to_owned()));
    assert!(lines.contains(&"auth.mode=static-bearer".to_owned()));
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("ws_url=ws://127.0.0.1:") && line.ends_with("/custom-ws"))
    );
}

#[test]
fn server_command_loads_published_schema_and_persists_ws_data_across_restart() {
    let app_id = "app-lifecycle";
    let data_dir = std::env::temp_dir().join(format!(
        "jazz-server-command-lifecycle-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&data_dir);
    publish_schema_to_data_dir(app_id, &data_dir);

    let schema = todos_schema();
    let server = RunningServer::start(app_id, &data_dir);
    assert!(server.lines.contains(&"command=server".to_owned()));
    assert!(server.lines.contains(&format!("app_id={app_id}")));
    assert!(
        server
            .lines
            .contains(&format!("websocket_path=/apps/{app_id}/ws"))
    );
    assert!(server.lines.contains(&"storage=rocksdb".to_owned()));
    assert!(
        server
            .lines
            .contains(&"schema_catalogue=admin_schema_store".to_owned())
    );
    assert!(
        server
            .lines
            .contains(&"runtime_schema_loading=admin_schema_store_latest".to_owned())
    );
    assert!(
        server
            .lines
            .contains(&"admin_schema_store=opened".to_owned())
    );
    assert!(server.ws_url.ends_with(&format!("/apps/{app_id}/ws")));

    let mut writer = open_connected_client(schema.clone(), &server.ws_url, identity(0xc1, 0xc1));
    let write = writer
        .db
        .insert_with_id(
            "todos",
            RowUuid::from_bytes([0x41; 16]),
            todo_cells("durable cli row", true),
        )
        .expect("write todo through client db");
    assert!(pump_websocket(&mut writer.socket, &writer.db, &writer.wire,));
    let state = write.write_state().expect("write state");
    assert_eq!(state.fate, Fate::Accepted);
    drop(writer.socket);
    server.shutdown();

    let restarted = RunningServer::start(app_id, &data_dir);
    assert!(
        restarted
            .lines
            .contains(&"schema_catalogue=admin_schema_store".to_owned())
    );

    let mut reader = open_connected_client(schema.clone(), &restarted.ws_url, identity(0xc2, 0xc1));
    let prepared = reader
        .db
        .prepare_query(&Query::from("todos"))
        .expect("prepare todos query");
    let mut subscription = block_on(reader.db.subscribe(
        &prepared,
        ReadOpts {
            tier: DurabilityTier::Global,
            ..Default::default()
        },
    ))
    .expect("subscribe to todos");
    assert!(pump_websocket(&mut reader.socket, &reader.db, &reader.wire,));
    assert_eq!(
        subscription_fields(&mut subscription, &schema.tables[0]),
        vec![("durable cli row".to_owned(), true)]
    );

    drop(reader.socket);
    restarted.shutdown();
    let store = std::fs::read_to_string(data_dir.join("admin-schemas.json"))
        .expect("schema catalogue survives beside data dir");
    assert!(store.contains(app_id));
    let _ = std::fs::remove_dir_all(&data_dir);
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
    assert!(lines.contains(&"admin_schema_api=not_started"));
    assert!(lines.contains(&"admin_schema_store=not_opened"));
    assert!(lines.contains(&"admin_schema_owner=loopback_http_only"));
    assert!(lines.contains(&"sockets_bound=false"));
    assert!(lines.contains(&"storage_opened=false"));
    assert!(lines.contains(&"runtime_started=false"));
    assert!(lines.contains(&"auth.mode=anonymous"));
    assert!(lines.contains(&"auth.legacy_query_identity=false"));
    assert!(lines.contains(&"auth.allow_local_first_auth=false"));
    assert!(lines.contains(&"auth.anonymous_subject=anonymous"));
}

#[test]
fn dry_run_accepts_legacy_query_identity_opt_in() {
    let output = jazz_server_command()
        .args(["dry-run", "--allow-legacy-query-identity", "true"])
        .output()
        .expect("run jazz-server dry-run with legacy identity opt-in");

    assert!(output.status.success());
    assert!(output.stderr.is_empty());

    let stdout = String::from_utf8(output.stdout).expect("dry-run stdout is utf-8");
    let lines: Vec<&str> = stdout.lines().collect();
    assert!(lines.contains(&"auth.mode=anonymous"));
    assert!(lines.contains(&"auth.legacy_query_identity=true"));
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
    assert!(lines.contains(&"admin_schema_api=not_started"));
    assert!(lines.contains(&"admin_schema_store=not_opened"));
    assert!(lines.contains(&"admin_schema_owner=loopback_http_only"));
    assert!(lines.contains(&"storage_opened=false"));
    assert!(lines.contains(&"auth.mode=static-bearer"));
    assert!(lines.contains(&"auth.legacy_query_identity=false"));
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
fn dry_run_accepts_backend_secret_env_alias() {
    let output = jazz_server_command()
        .arg("dry-run")
        .env("JAZZ_BACKEND_SECRET", "backend-secret")
        .output()
        .expect("run jazz-server dry-run with backend secret env");

    assert!(output.status.success());
    assert!(output.stderr.is_empty());

    let stdout = String::from_utf8(output.stdout).expect("dry-run stdout is utf-8");
    let lines: Vec<&str> = stdout.lines().collect();
    assert!(lines.contains(&"auth.mode=static-bearer"));
    assert!(lines.contains(&"auth.legacy_query_identity=false"));
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
    assert!(lines.contains(&"auth.legacy_query_identity=false"));
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
    assert!(stderr.contains(" serve-loopback-websocket-schema <schema-postcard-hex>"));
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
    assert!(stderr.contains(" dev-server <schema-postcard-hex>"));
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
        assert!(stderr.contains(&format!(" {command} <schema-postcard-hex>")));
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
        stderr
            .contains(" serve-loopback-websocket-schema-data-dir <schema-postcard-hex> <data-dir>")
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
        stderr
            .contains(" serve-loopback-websocket-schema-data-dir <schema-postcard-hex> <data-dir>")
    );
}

#[test]
fn durable_loopback_websocket_command_rejects_unopenable_data_dir() {
    let schema_hex = schema_hex(&JazzSchema::new([]));
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
    assert!(stderr.contains("failed to open RocksDB storage"));
}
