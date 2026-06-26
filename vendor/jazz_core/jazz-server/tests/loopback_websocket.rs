use std::collections::BTreeMap;
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::thread;
use std::time::Duration;

use futures_util::{FutureExt, StreamExt};
use jazz::abi::{
    ABI_FEATURE_NONE, ABI_VERSION, AbiRowBatch, AbiSubscriptionStreamChunk, DbIdentityPayload,
    Event, EventBudget, EventKind, FrameBudget, Handle, MemoryStorageConfig, OpenDbConfig,
    TickStats, TransportDirection, WriteStateTarget,
};
use jazz::abi_runtime::{AbiCallResult, AbiRuntime, AbiSubscriptionStream};
use jazz::db::{DbIdentity, RowCells};
use jazz::groove::records::Value;
use jazz::groove::schema::ColumnType;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::query::{Query, claim, col, eq};
use jazz::schema::{ColumnSchema, JazzSchema, Policy, TableSchema};
use jazz::tx::{Fate, RejectionReason};
use jazz_server::{
    InMemoryServerShell, InMemoryServerShellConfig, StorageConfig,
    auth_admission::{AuthAdmissionConfig, JwtVerifierConfig, author_id_from_subject},
    auth_admission::{AuthHandshake, LOCAL_FIRST_JWT_ISSUER},
    loopback_websocket::{LoopbackWebSocketServer, LoopbackWebSocketServerConfig},
};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde_json::json;
use tungstenite::client::IntoClientRequest;
use tungstenite::protocol::Message;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{WebSocket, connect};

fn todos_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
        ],
    )])
}

fn owner_write_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_write_policy(Policy::owner_only("todos", "owner"))])
}

fn team_read_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("team", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::shape(
        Query::from("todos").filter(eq(col("team"), claim("team"))),
    ))
    .with_write_policy(Policy::shape(
        Query::from("todos").filter(eq(col("team"), claim("team"))),
    ))])
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

fn owned_todo_cells(title: &str, done: bool, owner: AuthorId) -> RowCells {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("done".to_owned(), Value::Bool(done)),
        ("owner".to_owned(), Value::Uuid(owner.0)),
    ])
}

fn team_todo_cells(title: &str, done: bool, team: AuthorId) -> RowCells {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("done".to_owned(), Value::Bool(done)),
        ("team".to_owned(), Value::Uuid(team.0)),
    ])
}

fn expect_ack(result: AbiCallResult) {
    assert_eq!(result, AbiCallResult::Ack);
}

fn drain_events(runtime: &mut AbiRuntime, runtime_handle: Handle, request_id: u64) -> Vec<Event> {
    let AbiCallResult::Events { events } = runtime.events_poll(
        Some(request_id),
        runtime_handle,
        EventBudget {
            max_events: 128,
            max_bytes: u64::MAX,
        },
    ) else {
        panic!("expected event poll");
    };
    events
}

fn open_client_runtime(schema: JazzSchema, client: DbIdentity) -> (AbiRuntime, Handle, Handle) {
    let mut runtime = AbiRuntime::new();
    let AbiCallResult::RuntimeInitialized {
        runtime: runtime_handle,
    } = runtime.runtime_init(Some(10), ABI_VERSION, ABI_FEATURE_NONE)
    else {
        panic!("expected runtime init");
    };
    drain_events(&mut runtime, runtime_handle, 11);

    expect_ack(runtime.storage_open_memory(
        Some(12),
        postcard::to_allocvec(&MemoryStorageConfig::default()).unwrap(),
    ));
    let storage = drain_events(&mut runtime, runtime_handle, 13)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::StorageOpened { storage, .. } => Some(storage),
            _ => None,
        })
        .unwrap();

    expect_ack(
        runtime.db_open_memory(
            Some(14),
            storage,
            postcard::to_allocvec(&schema).unwrap(),
            postcard::to_allocvec(&OpenDbConfig {
                identity: DbIdentityPayload::from(client),
                row_id_seed: Some(0xc1),
                history_complete: false,
            })
            .unwrap(),
        ),
    );
    let db = drain_events(&mut runtime, runtime_handle, 15)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::DbOpened { db, .. } => Some(db),
            _ => None,
        })
        .unwrap();

    (runtime, runtime_handle, db)
}

struct TodosSubscription {
    stream: AbiSubscriptionStream,
    rows: BTreeMap<RowUuid, (String, bool)>,
}

impl TodosSubscription {
    fn ingest_available(&mut self) {
        while let Some(chunk) = self.stream.next().now_or_never() {
            let Some(chunk) = chunk else {
                break;
            };
            match chunk.unwrap() {
                AbiSubscriptionStreamChunk::Snapshot(snapshot) => {
                    self.rows.clear();
                    ingest_batches(&mut self.rows, &snapshot.rows);
                }
                AbiSubscriptionStreamChunk::Delta(chunk) => {
                    for removed in chunk.delta.removed {
                        self.rows.remove(&removed.row_id);
                    }
                    ingest_batches(&mut self.rows, &chunk.delta.added);
                    ingest_batches(&mut self.rows, &chunk.delta.updated);
                }
            }
        }
    }

    fn fields(&mut self) -> Vec<(String, bool)> {
        self.ingest_available();
        let mut fields = self.rows.values().cloned().collect::<Vec<_>>();
        fields.sort();
        fields
    }
}

fn ingest_batches(rows: &mut BTreeMap<RowUuid, (String, bool)>, batches: &[AbiRowBatch]) {
    for batch in batches {
        assert_eq!(batch.table, "todos");
        for row in &batch.rows {
            let record = batch.descriptor.bind(&row.raw);
            let Value::Nullable(Some(title)) = record.get("user_title").unwrap() else {
                panic!("expected nullable title");
            };
            let Value::String(title) = title.as_ref() else {
                panic!("expected string title");
            };
            let Value::Nullable(Some(done)) = record.get("user_done").unwrap() else {
                panic!("expected nullable done");
            };
            let Value::Bool(done) = done.as_ref() else {
                panic!("expected bool done");
            };
            rows.insert(row.row_id, (title.clone(), *done));
        }
    }
}

fn prepare_todos_subscription(
    runtime: &mut AbiRuntime,
    runtime_handle: Handle,
    db: Handle,
) -> TodosSubscription {
    expect_ack(runtime.db_prepare_query(
        Some(20),
        db,
        postcard::to_allocvec(&Query::from("todos")).unwrap(),
    ));
    let query = drain_events(runtime, runtime_handle, 21)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::QueryPrepared { query, .. } => Some(query),
            _ => None,
        })
        .unwrap();

    let mut subscription = TodosSubscription {
        stream: runtime
            .db_subscribe_stream(Some(22), db, query, Default::default())
            .unwrap(),
        rows: BTreeMap::new(),
    };
    subscription.ingest_available();
    subscription
}

fn attach_upstream(runtime: &mut AbiRuntime, runtime_handle: Handle, db: Handle) -> Handle {
    expect_ack(runtime.transport_attach(Some(30), db, TransportDirection::Upstream, Vec::new()));
    drain_events(runtime, runtime_handle, 31)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::TransportAttached { transport, .. } => Some(transport),
            _ => None,
        })
        .unwrap()
}

fn take_client_frames(
    runtime: &mut AbiRuntime,
    runtime_handle: Handle,
    transport: Handle,
    request_id: u64,
) -> Vec<Vec<u8>> {
    expect_ack(runtime.transport_recv_wire_frame(
        Some(request_id),
        transport,
        FrameBudget {
            max_frames: 16,
            max_bytes: u64::MAX,
        },
    ));
    drain_events(runtime, runtime_handle, request_id + 1)
        .into_iter()
        .flat_map(|event| match event.kind {
            EventKind::WireFrames { frames, .. } => frames,
            _ => Vec::new(),
        })
        .collect()
}

fn author_hex(author: AuthorId) -> String {
    author
        .0
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

fn connect_path(addr: SocketAddr, path_and_query: &str) -> WebSocket<MaybeTlsStream<TcpStream>> {
    let url = format!("ws://{addr}{path_and_query}");
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
            "connect loopback WebSocket listener: {:?}",
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

fn connect_sync(addr: SocketAddr) -> WebSocket<MaybeTlsStream<TcpStream>> {
    connect_path(addr, "/sync")
}

fn connect_sync_with_bearer(
    addr: SocketAddr,
    bearer: &str,
) -> WebSocket<MaybeTlsStream<TcpStream>> {
    let mut request = format!("ws://{addr}/sync")
        .into_client_request()
        .expect("build websocket request");
    request
        .headers_mut()
        .insert("Authorization", format!("Bearer {bearer}").parse().unwrap());
    let (mut socket, response) = connect(request).expect("connect with bearer");
    assert_eq!(response.status().as_u16(), 101);
    if let MaybeTlsStream::Plain(stream) = socket.get_mut() {
        stream
            .set_read_timeout(Some(Duration::from_millis(20)))
            .expect("set read timeout");
    }
    socket
}

fn connect_sync_as(addr: SocketAddr, author: AuthorId) -> WebSocket<MaybeTlsStream<TcpStream>> {
    connect_path(addr, &format!("/sync?identity={}", author_hex(author)))
}

fn send_auth_handshake(
    socket: &mut WebSocket<MaybeTlsStream<TcpStream>>,
    bearer: &str,
    sub: &str,
    claims: BTreeMap<String, Value>,
) {
    socket
        .send(Message::Text(
            serde_json::to_string(&AuthHandshake {
                bearer_jwt: Some(bearer.to_owned()),
                sub: sub.to_owned(),
                claims,
            })
            .unwrap()
            .into(),
        ))
        .expect("send auth handshake");
}

fn signed_hs256_jwt(secret: &[u8], sub: &str, exp: u64, claims: serde_json::Value) -> String {
    let mut payload = claims.as_object().cloned().unwrap_or_default();
    payload.insert("sub".to_owned(), json!(sub));
    payload.insert("exp".to_owned(), json!(exp));
    encode(
        &Header::new(Algorithm::HS256),
        &payload,
        &EncodingKey::from_secret(secret),
    )
    .expect("sign JWT")
}

fn signed_local_first_eddsa_jwt(sub: &str, exp: u64) -> String {
    signed_local_first_eddsa_jwt_for_app(sub, exp, None, None)
}

fn signed_local_first_eddsa_jwt_for_app(
    sub: &str,
    exp: u64,
    aud: Option<&str>,
    app_id: Option<&str>,
) -> String {
    let claims = json!({
        "iss": LOCAL_FIRST_JWT_ISSUER,
        "sub": sub,
        "exp": exp,
        "aud": aud,
        "appId": app_id,
    });
    encode(
        &Header::new(Algorithm::EdDSA),
        &claims,
        &EncodingKey::from_ed_pem(ED25519_PRIVATE_KEY_PEM.as_bytes()).unwrap(),
    )
    .expect("sign local-first JWT")
}

fn pump_websocket(
    socket: &mut WebSocket<MaybeTlsStream<TcpStream>>,
    runtime: &mut AbiRuntime,
    runtime_handle: Handle,
    db: Handle,
    transport: Handle,
    request_id_base: u64,
) -> bool {
    let mut saw_client_inbound = false;
    for offset in 0..64 {
        let request_id = request_id_base + offset * 10;
        expect_ack(runtime.db_drive_tick(Some(request_id), db));
        let frames = take_client_frames(runtime, runtime_handle, transport, request_id + 1);
        if !frames.is_empty() {
            socket
                .send(Message::Binary(encode_frame_batch(&frames).into()))
                .expect("send binary wire frame batch");
        }

        for frame in read_available_binary_frames(socket) {
            expect_ack(runtime.transport_send_wire_frame(None, transport, frame));
        }

        expect_ack(runtime.db_drive_tick(Some(request_id + 3), db));
        saw_client_inbound |= drain_events(runtime, runtime_handle, request_id + 4)
            .into_iter()
            .any(|event| {
                matches!(
                    event.kind,
                    EventKind::TickDone {
                        stats: TickStats { inbound, .. },
                    } if inbound > 0
                )
            });
    }
    saw_client_inbound
}

fn read_available_binary_frames(socket: &mut WebSocket<MaybeTlsStream<TcpStream>>) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    loop {
        match socket.read() {
            Ok(Message::Binary(batch)) => {
                frames.extend(decode_frame_batch(&batch).expect("decode websocket frame batch"));
            }
            Ok(Message::Ping(payload)) => socket
                .send(Message::Pong(payload))
                .expect("send pong for server ping"),
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

fn encode_frame_batch(frames: &[Vec<u8>]) -> Vec<u8> {
    postcard::to_allocvec(frames).expect("encode websocket frame batch")
}

fn decode_frame_batch(bytes: &[u8]) -> Result<Vec<Vec<u8>>, postcard::Error> {
    postcard::from_bytes(bytes)
}

fn start_legacy_query_identity_server(
    bind_addr: SocketAddr,
    config: InMemoryServerShellConfig,
    websocket_path: &str,
) -> jazz_server::loopback_websocket::LoopbackWebSocketResult<LoopbackWebSocketServer> {
    LoopbackWebSocketServer::start_with_admission(
        bind_addr,
        config,
        websocket_path,
        AuthAdmissionConfig::legacy_query_identity(),
    )
}

#[test]
fn loopback_websocket_starts_and_syncs_real_abi_frames_between_clients() {
    let schema = todos_schema();
    let server = start_legacy_query_identity_server(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        InMemoryServerShellConfig::new(schema.clone(), identity(0x5e, 0x01)).with_row_id_seed(0x5e),
        "/sync",
    )
    .expect("start loopback WebSocket listener");
    let addr = server.local_addr();

    let writer = identity(0xc1, 0xc1);
    let (mut writer_runtime, writer_runtime_handle, writer_db) =
        open_client_runtime(schema.clone(), writer);
    let writer_transport = attach_upstream(&mut writer_runtime, writer_runtime_handle, writer_db);
    let mut writer_socket = connect_sync_as(addr, writer.author);

    expect_ack(writer_runtime.db_insert_with_id(
        Some(40),
        writer_db,
        "todos".to_owned(),
        RowUuid::from_bytes([0x41; 16]),
        postcard::to_allocvec(&todo_cells("over websocket", true)).unwrap(),
    ));
    let write = drain_events(&mut writer_runtime, writer_runtime_handle, 41)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteStarted { write, .. } => Some(write),
            _ => None,
        })
        .unwrap();

    assert!(pump_websocket(
        &mut writer_socket,
        &mut writer_runtime,
        writer_runtime_handle,
        writer_db,
        writer_transport,
        100,
    ));
    expect_ack(writer_runtime.db_get_write_state(Some(9500), WriteStateTarget::Handle(write)));
    let state = drain_events(&mut writer_runtime, writer_runtime_handle, 9501)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteState { state, .. } => Some(state),
            _ => None,
        })
        .unwrap();
    assert_eq!(state.fate, Fate::Accepted);

    let reader = identity(0xc2, 0xc1);
    let (mut reader_runtime, reader_runtime_handle, reader_db) =
        open_client_runtime(schema, reader);
    let mut reader_subscription =
        prepare_todos_subscription(&mut reader_runtime, reader_runtime_handle, reader_db);
    let reader_transport = attach_upstream(&mut reader_runtime, reader_runtime_handle, reader_db);
    let mut reader_socket = connect_sync_as(addr, reader.author);

    assert!(pump_websocket(
        &mut reader_socket,
        &mut reader_runtime,
        reader_runtime_handle,
        reader_db,
        reader_transport,
        1000,
    ));
    assert_eq!(
        reader_subscription.fields(),
        vec![("over websocket".to_owned(), true)]
    );

    server.shutdown();
}

#[test]
fn verified_jwt_claim_gates_websocket_policy_reads() {
    let schema = team_read_schema();
    let data_dir =
        std::env::temp_dir().join(format!("jazz-server-claim-ws-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);
    let team_a = AuthorId::from_bytes([0xa1; 16]);
    let team_b = AuthorId::from_bytes([0xb2; 16]);

    {
        let mut shell = InMemoryServerShell::start_with_storage(
            InMemoryServerShellConfig::new(schema.clone(), identity(0x5e, 0x01))
                .with_row_id_seed(0x5e),
            StorageConfig::data_dir(&data_dir),
        )
        .expect("start seeding shell");
        shell
            .seed_row_with_id(
                "todos",
                RowUuid::from_bytes([0x41; 16]),
                team_todo_cells("team a", false, team_a),
            )
            .expect("seed team a row");
        shell
            .seed_row_with_id(
                "todos",
                RowUuid::from_bytes([0x42; 16]),
                team_todo_cells("team b", true, team_b),
            )
            .expect("seed team b row");
    }

    let mut config = LoopbackWebSocketServerConfig::persistent_data_dir(
        schema.clone(),
        identity(0x5e, 0x01),
        &data_dir,
    )
    .with_auth_admission(AuthAdmissionConfig::jwt(
        jazz_server::auth_admission::JwtVerifierConfig::hmac_secret(
            Algorithm::HS256,
            b"claim-test-secret".to_vec(),
        ),
    ))
    .with_row_id_seed(0x5e);
    config.listener.bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
    config.listener.websocket_path = "/sync".to_owned();
    let server =
        LoopbackWebSocketServer::start_with_config(config).expect("start auth WebSocket listener");
    let addr = server.local_addr();

    let reader = DbIdentity {
        node: NodeUuid::from_bytes([0xc2; 16]),
        author: author_id_from_subject("reader"),
    };
    let (mut reader_runtime, reader_runtime_handle, reader_db) =
        open_client_runtime(schema, reader);
    let mut reader_subscription =
        prepare_todos_subscription(&mut reader_runtime, reader_runtime_handle, reader_db);
    let reader_transport = attach_upstream(&mut reader_runtime, reader_runtime_handle, reader_db);
    let mut reader_socket = connect_sync(addr);
    let reader_jwt = signed_hs256_jwt(
        b"claim-test-secret",
        "reader",
        4_102_444_800,
        json!({ "team": team_a.0.to_string() }),
    );
    send_auth_handshake(
        &mut reader_socket,
        &reader_jwt,
        "ignored-handshake-reader",
        BTreeMap::from([("team".to_owned(), Value::Uuid(team_b.0))]),
    );

    assert!(pump_websocket(
        &mut reader_socket,
        &mut reader_runtime,
        reader_runtime_handle,
        reader_db,
        reader_transport,
        1000,
    ));
    assert_eq!(
        reader_subscription.fields(),
        vec![("team a".to_owned(), false)]
    );

    let other_reader = DbIdentity {
        node: NodeUuid::from_bytes([0xc3; 16]),
        author: author_id_from_subject("other-reader"),
    };
    let (mut other_runtime, other_runtime_handle, other_db) =
        open_client_runtime(team_read_schema(), other_reader);
    let mut other_subscription =
        prepare_todos_subscription(&mut other_runtime, other_runtime_handle, other_db);
    let other_transport = attach_upstream(&mut other_runtime, other_runtime_handle, other_db);
    let mut other_socket = connect_sync(addr);
    let other_jwt = signed_hs256_jwt(
        b"claim-test-secret",
        "other-reader",
        4_102_444_800,
        json!({ "team": team_b.0.to_string() }),
    );
    send_auth_handshake(
        &mut other_socket,
        &other_jwt,
        "ignored-other-reader",
        BTreeMap::from([("team".to_owned(), Value::Uuid(team_a.0))]),
    );

    assert!(pump_websocket(
        &mut other_socket,
        &mut other_runtime,
        other_runtime_handle,
        other_db,
        other_transport,
        2000,
    ));
    assert_eq!(
        other_subscription.fields(),
        vec![("team b".to_owned(), true)]
    );

    server.shutdown();
}

#[test]
fn durable_loopback_websocket_survives_restart_and_accepts_new_writes() {
    let schema = todos_schema();
    let data_dir =
        std::env::temp_dir().join(format!("jazz-server-durable-ws-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);

    let mut first_config = LoopbackWebSocketServerConfig::persistent_data_dir(
        schema.clone(),
        identity(0x5e, 0x01),
        &data_dir,
    )
    .with_row_id_seed(0x5e)
    .with_auth_admission(AuthAdmissionConfig::legacy_query_identity());
    first_config.listener.bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let first_server = LoopbackWebSocketServer::start_with_config(first_config)
        .expect("start durable loopback WebSocket listener");
    let first_addr = first_server.local_addr();

    let writer = identity(0xc1, 0xc1);
    let (mut writer_runtime, writer_runtime_handle, writer_db) =
        open_client_runtime(schema.clone(), writer);
    let writer_transport = attach_upstream(&mut writer_runtime, writer_runtime_handle, writer_db);
    let mut writer_socket = connect_sync_as(first_addr, writer.author);

    expect_ack(writer_runtime.db_insert_with_id(
        Some(40),
        writer_db,
        "todos".to_owned(),
        RowUuid::from_bytes([0x41; 16]),
        postcard::to_allocvec(&todo_cells("before restart", false)).unwrap(),
    ));
    let first_write = drain_events(&mut writer_runtime, writer_runtime_handle, 41)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteStarted { write, .. } => Some(write),
            _ => None,
        })
        .unwrap();
    assert!(pump_websocket(
        &mut writer_socket,
        &mut writer_runtime,
        writer_runtime_handle,
        writer_db,
        writer_transport,
        100,
    ));
    expect_ack(
        writer_runtime.db_get_write_state(Some(9500), WriteStateTarget::Handle(first_write)),
    );
    let state = drain_events(&mut writer_runtime, writer_runtime_handle, 9501)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteState { state, .. } => Some(state),
            _ => None,
        })
        .unwrap();
    assert_eq!(state.fate, Fate::Accepted);
    first_server.shutdown();

    let mut second_config = LoopbackWebSocketServerConfig::persistent_data_dir(
        schema.clone(),
        identity(0x5e, 0x01),
        &data_dir,
    )
    .with_row_id_seed(0x5e)
    .with_auth_admission(AuthAdmissionConfig::legacy_query_identity());
    second_config.listener.bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let second_server = LoopbackWebSocketServer::start_with_config(second_config)
        .expect("restart durable loopback WebSocket listener");
    let second_addr = second_server.local_addr();

    let reader = identity(0xc2, 0xc1);
    let (mut reader_runtime, reader_runtime_handle, reader_db) =
        open_client_runtime(schema, reader);
    let mut reader_subscription =
        prepare_todos_subscription(&mut reader_runtime, reader_runtime_handle, reader_db);
    let reader_transport = attach_upstream(&mut reader_runtime, reader_runtime_handle, reader_db);
    let mut reader_socket = connect_sync_as(second_addr, reader.author);

    assert!(pump_websocket(
        &mut reader_socket,
        &mut reader_runtime,
        reader_runtime_handle,
        reader_db,
        reader_transport,
        1000,
    ));
    assert_eq!(
        reader_subscription.fields(),
        vec![("before restart".to_owned(), false)]
    );

    expect_ack(reader_runtime.db_insert_with_id(
        Some(60),
        reader_db,
        "todos".to_owned(),
        RowUuid::from_bytes([0x42; 16]),
        postcard::to_allocvec(&todo_cells("after restart", true)).unwrap(),
    ));
    let second_write = drain_events(&mut reader_runtime, reader_runtime_handle, 61)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteStarted { write, .. } => Some(write),
            _ => None,
        })
        .unwrap();
    assert!(pump_websocket(
        &mut reader_socket,
        &mut reader_runtime,
        reader_runtime_handle,
        reader_db,
        reader_transport,
        2000,
    ));
    expect_ack(
        reader_runtime.db_get_write_state(Some(9600), WriteStateTarget::Handle(second_write)),
    );
    let state = drain_events(&mut reader_runtime, reader_runtime_handle, 9601)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteState { state, .. } => Some(state),
            _ => None,
        })
        .unwrap();
    assert_eq!(state.fate, Fate::Accepted);

    second_server.shutdown();
    let _ = std::fs::remove_dir_all(&data_dir);
}

#[test]
fn durable_loopback_websocket_policy_write_survives_restart() {
    let schema = team_read_schema();
    let data_dir = std::env::temp_dir().join(format!(
        "jazz-server-durable-policy-ws-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&data_dir);
    let team_a = AuthorId::from_bytes([0xa1; 16]);
    let secret = b"durable-policy-secret";

    let mut first_config = LoopbackWebSocketServerConfig::persistent_data_dir(
        schema.clone(),
        identity(0x5e, 0x01),
        &data_dir,
    )
    .with_auth_admission(AuthAdmissionConfig::jwt(
        jazz_server::auth_admission::JwtVerifierConfig::hmac_secret(
            Algorithm::HS256,
            secret.to_vec(),
        ),
    ))
    .with_row_id_seed(0x5e);
    first_config.listener.bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let first_server = LoopbackWebSocketServer::start_with_config(first_config)
        .expect("start durable policy loopback WebSocket listener");
    let first_addr = first_server.local_addr();

    let writer = DbIdentity {
        node: NodeUuid::from_bytes([0xd1; 16]),
        author: author_id_from_subject("durable-policy-writer"),
    };
    let (mut writer_runtime, writer_runtime_handle, writer_db) =
        open_client_runtime(schema.clone(), writer);
    let writer_transport = attach_upstream(&mut writer_runtime, writer_runtime_handle, writer_db);
    let mut writer_socket = connect_sync(first_addr);
    let writer_jwt = signed_hs256_jwt(
        secret,
        "durable-policy-writer",
        4_102_444_800,
        json!({ "team": team_a.0.to_string() }),
    );
    send_auth_handshake(
        &mut writer_socket,
        &writer_jwt,
        "durable-policy-writer",
        BTreeMap::from([("team".to_owned(), Value::Uuid(team_a.0))]),
    );

    expect_ack(writer_runtime.db_insert_with_id(
        Some(40),
        writer_db,
        "todos".to_owned(),
        RowUuid::from_bytes([0x51; 16]),
        postcard::to_allocvec(&team_todo_cells("before policy restart", false, team_a)).unwrap(),
    ));
    let first_write = drain_events(&mut writer_runtime, writer_runtime_handle, 41)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteStarted { write, .. } => Some(write),
            _ => None,
        })
        .unwrap();
    assert!(pump_websocket(
        &mut writer_socket,
        &mut writer_runtime,
        writer_runtime_handle,
        writer_db,
        writer_transport,
        100,
    ));
    expect_ack(
        writer_runtime.db_get_write_state(Some(9500), WriteStateTarget::Handle(first_write)),
    );
    let state = drain_events(&mut writer_runtime, writer_runtime_handle, 9501)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteState { state, .. } => Some(state),
            _ => None,
        })
        .unwrap();
    assert_eq!(state.fate, Fate::Accepted);
    first_server.shutdown();

    let mut second_config = LoopbackWebSocketServerConfig::persistent_data_dir(
        schema.clone(),
        identity(0x5e, 0x01),
        &data_dir,
    )
    .with_auth_admission(AuthAdmissionConfig::jwt(
        jazz_server::auth_admission::JwtVerifierConfig::hmac_secret(
            Algorithm::HS256,
            secret.to_vec(),
        ),
    ))
    .with_row_id_seed(0x5e);
    second_config.listener.bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let second_server = LoopbackWebSocketServer::start_with_config(second_config)
        .expect("restart durable policy loopback WebSocket listener");
    let second_addr = second_server.local_addr();

    let reader = DbIdentity {
        node: NodeUuid::from_bytes([0xd2; 16]),
        author: author_id_from_subject("durable-policy-reader"),
    };
    let (mut reader_runtime, reader_runtime_handle, reader_db) =
        open_client_runtime(schema, reader);
    let mut reader_subscription =
        prepare_todos_subscription(&mut reader_runtime, reader_runtime_handle, reader_db);
    let reader_transport = attach_upstream(&mut reader_runtime, reader_runtime_handle, reader_db);
    let mut reader_socket = connect_sync(second_addr);
    let reader_jwt = signed_hs256_jwt(
        secret,
        "durable-policy-reader",
        4_102_444_800,
        json!({ "team": team_a.0.to_string() }),
    );
    send_auth_handshake(
        &mut reader_socket,
        &reader_jwt,
        "durable-policy-reader",
        BTreeMap::from([("team".to_owned(), Value::Uuid(team_a.0))]),
    );

    assert!(pump_websocket(
        &mut reader_socket,
        &mut reader_runtime,
        reader_runtime_handle,
        reader_db,
        reader_transport,
        1000,
    ));
    assert_eq!(
        reader_subscription.fields(),
        vec![("before policy restart".to_owned(), false)]
    );

    expect_ack(reader_runtime.db_insert_with_id(
        Some(60),
        reader_db,
        "todos".to_owned(),
        RowUuid::from_bytes([0x52; 16]),
        postcard::to_allocvec(&team_todo_cells("after policy restart", true, team_a)).unwrap(),
    ));
    let second_write = drain_events(&mut reader_runtime, reader_runtime_handle, 61)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteStarted { write, .. } => Some(write),
            _ => None,
        })
        .unwrap();
    assert!(pump_websocket(
        &mut reader_socket,
        &mut reader_runtime,
        reader_runtime_handle,
        reader_db,
        reader_transport,
        2000,
    ));
    expect_ack(
        reader_runtime.db_get_write_state(Some(9600), WriteStateTarget::Handle(second_write)),
    );
    let state = drain_events(&mut reader_runtime, reader_runtime_handle, 9601)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteState { state, .. } => Some(state),
            _ => None,
        })
        .unwrap();
    assert_eq!(state.fate, Fate::Accepted);

    second_server.shutdown();
    let _ = std::fs::remove_dir_all(&data_dir);
}

#[test]
fn loopback_websocket_rejects_wrong_path_handshake() {
    let server = start_legacy_query_identity_server(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        InMemoryServerShellConfig::new(todos_schema(), identity(0x5e, 0x01)),
        "/sync",
    )
    .expect("start loopback WebSocket listener");
    let addr = server.local_addr();

    let result = connect(format!(
        "ws://{addr}/elsewhere?identity={}",
        author_hex(identity(0xc1, 0xc1).author)
    ));
    assert!(result.is_err(), "wrong websocket path should be rejected");

    server.shutdown();
}

#[test]
fn loopback_websocket_accepts_app_scoped_ws_path() {
    let server = start_legacy_query_identity_server(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        InMemoryServerShellConfig::new(todos_schema(), identity(0x5e, 0x01)),
        "/sync",
    )
    .expect("start loopback WebSocket listener");
    let addr = server.local_addr();

    let mut socket = connect_path(
        addr,
        &format!(
            "/apps/my-app/ws?identity={}",
            author_hex(identity(0xc1, 0xc1).author)
        ),
    );
    socket.close(None).expect("close app-scoped websocket");

    server.shutdown();
}

#[test]
fn loopback_websocket_rejects_malformed_identity_query_handshake() {
    let server = start_legacy_query_identity_server(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        InMemoryServerShellConfig::new(todos_schema(), identity(0x5e, 0x01)),
        "/sync",
    )
    .expect("start loopback WebSocket listener");
    let addr = server.local_addr();

    assert!(
        connect(format!("ws://{addr}/sync?identity=not-32-hex")).is_err(),
        "non-hex identity query should be rejected"
    );
    assert!(
        connect(format!("ws://{addr}/sync?identity=abc")).is_err(),
        "short identity query should be rejected"
    );

    server.shutdown();
}

#[test]
fn loopback_websocket_default_rejects_legacy_identity_query() {
    let server = LoopbackWebSocketServer::start(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        InMemoryServerShellConfig::new(todos_schema(), identity(0x5e, 0x01)),
        "/sync",
    )
    .expect("start loopback WebSocket listener");
    let addr = server.local_addr();

    assert!(
        connect(format!(
            "ws://{addr}/sync?identity={}",
            author_hex(identity(0xc1, 0xc1).author)
        ))
        .is_err(),
        "legacy identity query should require explicit admission opt-in"
    );

    server.shutdown();
}

#[test]
fn loopback_websocket_identity_query_controls_write_policy_identity() {
    let schema = owner_write_schema();
    let server = start_legacy_query_identity_server(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        InMemoryServerShellConfig::new(schema.clone(), identity(0x5e, 0x01)).with_row_id_seed(0x5e),
        "/sync",
    )
    .expect("start loopback WebSocket listener");
    let addr = server.local_addr();
    let owner = identity(0xc1, 0xa1);
    let accepted_owner = identity(0xc2, 0xa2);
    let other_author = AuthorId::from_bytes([0xb2; 16]);

    let (mut rejected_runtime, rejected_runtime_handle, rejected_db) =
        open_client_runtime(schema.clone(), owner);
    let rejected_transport =
        attach_upstream(&mut rejected_runtime, rejected_runtime_handle, rejected_db);
    let mut rejected_socket = connect_sync_as(addr, other_author);
    expect_ack(
        rejected_runtime.db_insert_with_id(
            Some(40),
            rejected_db,
            "todos".to_owned(),
            RowUuid::from_bytes([0x41; 16]),
            postcard::to_allocvec(&owned_todo_cells(
                "wrong websocket identity",
                false,
                owner.author,
            ))
            .unwrap(),
        ),
    );
    let rejected_write = drain_events(&mut rejected_runtime, rejected_runtime_handle, 41)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteStarted { write, .. } => Some(write),
            _ => None,
        })
        .unwrap();

    assert!(pump_websocket(
        &mut rejected_socket,
        &mut rejected_runtime,
        rejected_runtime_handle,
        rejected_db,
        rejected_transport,
        100,
    ));
    expect_ack(
        rejected_runtime.db_get_write_state(Some(9500), WriteStateTarget::Handle(rejected_write)),
    );
    let rejected_state = drain_events(&mut rejected_runtime, rejected_runtime_handle, 9501)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteState { state, .. } => Some(state),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        rejected_state.fate,
        Fate::Rejected(RejectionReason::AuthorizationDenied)
    );

    let (mut accepted_runtime, accepted_runtime_handle, accepted_db) =
        open_client_runtime(schema, accepted_owner);
    let accepted_transport =
        attach_upstream(&mut accepted_runtime, accepted_runtime_handle, accepted_db);
    let mut accepted_socket = connect_sync_as(addr, accepted_owner.author);
    expect_ack(
        accepted_runtime.db_insert_with_id(
            Some(60),
            accepted_db,
            "todos".to_owned(),
            RowUuid::from_bytes([0x42; 16]),
            postcard::to_allocvec(&owned_todo_cells(
                "matching websocket identity",
                true,
                accepted_owner.author,
            ))
            .unwrap(),
        ),
    );
    let accepted_write = drain_events(&mut accepted_runtime, accepted_runtime_handle, 61)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteStarted { write, .. } => Some(write),
            _ => None,
        })
        .unwrap();

    assert!(pump_websocket(
        &mut accepted_socket,
        &mut accepted_runtime,
        accepted_runtime_handle,
        accepted_db,
        accepted_transport,
        1000,
    ));
    expect_ack(
        accepted_runtime.db_get_write_state(Some(9600), WriteStateTarget::Handle(accepted_write)),
    );
    let accepted_state = drain_events(&mut accepted_runtime, accepted_runtime_handle, 9601)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteState { state, .. } => Some(state),
            _ => None,
        })
        .unwrap();
    assert_eq!(accepted_state.fate, Fate::Accepted);

    server.shutdown();
}

#[test]
fn loopback_websocket_first_frame_auth_handshake_controls_write_policy_identity() {
    let schema = owner_write_schema();
    let token = "static-test-token";
    let subject = "user:accepted-owner";
    let accepted_author = author_id_from_subject(subject);
    let server = LoopbackWebSocketServer::start_with_admission(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        InMemoryServerShellConfig::new(schema.clone(), identity(0x5e, 0x01)).with_row_id_seed(0x5e),
        "/sync",
        AuthAdmissionConfig::static_bearer(token),
    )
    .expect("start loopback WebSocket listener");
    let addr = server.local_addr();

    assert!(
        connect(format!(
            "ws://{addr}/sync?identity={}",
            author_hex(AuthorId::from_bytes([0xb2; 16]))
        ))
        .is_err(),
        "legacy identity query should be rejected when admission is configured"
    );

    let owner = DbIdentity {
        node: NodeUuid::from_bytes([0xc2; 16]),
        author: accepted_author,
    };
    let (mut runtime, runtime_handle, db) = open_client_runtime(schema, owner);
    let transport = attach_upstream(&mut runtime, runtime_handle, db);
    let mut socket = connect_sync(addr);
    socket
        .send(Message::Text(
            serde_json::json!({
                "bearerJwt": token,
                "sub": subject,
            })
            .to_string()
            .into(),
        ))
        .expect("send first-frame auth handshake");

    expect_ack(
        runtime.db_insert_with_id(
            Some(70),
            db,
            "todos".to_owned(),
            RowUuid::from_bytes([0x43; 16]),
            postcard::to_allocvec(&owned_todo_cells(
                "handshake websocket identity",
                true,
                accepted_author,
            ))
            .unwrap(),
        ),
    );
    let write = drain_events(&mut runtime, runtime_handle, 71)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteStarted { write, .. } => Some(write),
            _ => None,
        })
        .unwrap();

    assert!(pump_websocket(
        &mut socket,
        &mut runtime,
        runtime_handle,
        db,
        transport,
        1000,
    ));
    expect_ack(runtime.db_get_write_state(Some(9700), WriteStateTarget::Handle(write)));
    let state = drain_events(&mut runtime, runtime_handle, 9701)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteState { state, .. } => Some(state),
            _ => None,
        })
        .unwrap();
    assert_eq!(state.fate, Fate::Accepted);

    server.shutdown();
}

#[test]
fn loopback_websocket_static_bearer_header_still_requires_first_frame_subject() {
    let schema = owner_write_schema();
    let token = "static-header-token";
    let subject = "user:header-owner";
    let accepted_author = author_id_from_subject(subject);
    let server = LoopbackWebSocketServer::start_with_admission(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        InMemoryServerShellConfig::new(schema.clone(), identity(0x5e, 0x01)).with_row_id_seed(0x5e),
        "/sync",
        AuthAdmissionConfig::static_bearer(token),
    )
    .expect("start loopback WebSocket listener");
    let addr = server.local_addr();

    let owner = DbIdentity {
        node: NodeUuid::from_bytes([0xc5; 16]),
        author: accepted_author,
    };
    let (mut runtime, runtime_handle, db) = open_client_runtime(schema, owner);
    let transport = attach_upstream(&mut runtime, runtime_handle, db);
    let mut socket = connect_sync_with_bearer(addr, token);
    socket
        .send(Message::Text(
            serde_json::json!({ "sub": subject }).to_string().into(),
        ))
        .expect("send first-frame subject handshake");

    expect_ack(
        runtime.db_insert_with_id(
            Some(74),
            db,
            "todos".to_owned(),
            RowUuid::from_bytes([0x45; 16]),
            postcard::to_allocvec(&owned_todo_cells(
                "header bearer handshake identity",
                true,
                accepted_author,
            ))
            .unwrap(),
        ),
    );
    let write = drain_events(&mut runtime, runtime_handle, 75)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteStarted { write, .. } => Some(write),
            _ => None,
        })
        .unwrap();

    assert!(pump_websocket(
        &mut socket,
        &mut runtime,
        runtime_handle,
        db,
        transport,
        1000,
    ));
    expect_ack(runtime.db_get_write_state(Some(9704), WriteStateTarget::Handle(write)));
    let state = drain_events(&mut runtime, runtime_handle, 9705)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteState { state, .. } => Some(state),
            _ => None,
        })
        .unwrap();
    assert_eq!(state.fate, Fate::Accepted);

    server.shutdown();
}

#[test]
fn loopback_websocket_rejects_empty_first_frame_subject() {
    let token = "static-empty-sub-token";
    let server = LoopbackWebSocketServer::start_with_admission(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        InMemoryServerShellConfig::new(todos_schema(), identity(0x5e, 0x01)),
        "/sync",
        AuthAdmissionConfig::static_bearer(token),
    )
    .expect("start loopback WebSocket listener");
    let mut socket = connect_sync(server.local_addr());
    socket
        .send(Message::Text(
            serde_json::json!({
                "bearerJwt": token,
                "sub": "   ",
            })
            .to_string()
            .into(),
        ))
        .expect("send invalid auth handshake");

    let result = socket.read();
    assert!(
        matches!(result, Ok(Message::Close(_)) | Err(_)),
        "empty handshake subject should close or drop the socket, got {result:?}"
    );

    server.shutdown();
}

#[test]
fn loopback_websocket_verified_jwt_subject_controls_write_policy_identity() {
    let schema = owner_write_schema();
    let secret = b"jwt-owner-secret";
    let jwt_subject = "user:jwt-owner";
    let accepted_author = author_id_from_subject(jwt_subject);
    let server = LoopbackWebSocketServer::start_with_admission(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        InMemoryServerShellConfig::new(schema.clone(), identity(0x5e, 0x01)).with_row_id_seed(0x5e),
        "/sync",
        AuthAdmissionConfig::jwt(jazz_server::auth_admission::JwtVerifierConfig::hmac_secret(
            Algorithm::HS256,
            secret.to_vec(),
        )),
    )
    .expect("start JWT loopback WebSocket listener");
    let addr = server.local_addr();

    let owner = DbIdentity {
        node: NodeUuid::from_bytes([0xc4; 16]),
        author: accepted_author,
    };
    let (mut runtime, runtime_handle, db) = open_client_runtime(schema, owner);
    let transport = attach_upstream(&mut runtime, runtime_handle, db);
    let mut socket = connect_sync(addr);
    let token = signed_hs256_jwt(
        secret,
        jwt_subject,
        4_102_444_800,
        json!({ "sub": "ignored" }),
    );
    send_auth_handshake(
        &mut socket,
        &token,
        "handshake-sub-must-be-ignored",
        BTreeMap::new(),
    );

    expect_ack(
        runtime.db_insert_with_id(
            Some(72),
            db,
            "todos".to_owned(),
            RowUuid::from_bytes([0x44; 16]),
            postcard::to_allocvec(&owned_todo_cells(
                "jwt websocket identity",
                true,
                accepted_author,
            ))
            .unwrap(),
        ),
    );
    let write = drain_events(&mut runtime, runtime_handle, 73)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteStarted { write, .. } => Some(write),
            _ => None,
        })
        .unwrap();

    assert!(pump_websocket(
        &mut socket,
        &mut runtime,
        runtime_handle,
        db,
        transport,
        1000,
    ));
    expect_ack(runtime.db_get_write_state(Some(9702), WriteStateTarget::Handle(write)));
    let state = drain_events(&mut runtime, runtime_handle, 9703)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteState { state, .. } => Some(state),
            _ => None,
        })
        .unwrap();
    assert_eq!(state.fate, Fate::Accepted);

    server.shutdown();
}

#[test]
fn loopback_websocket_local_first_jwt_subject_controls_write_policy_identity() {
    let schema = owner_write_schema();
    let mut config = AuthAdmissionConfig::jwt(JwtVerifierConfig::ed_public_key_pem(
        ED25519_PUBLIC_KEY_PEM.as_bytes(),
    ));
    config.allow_local_first_auth = true;
    let server = LoopbackWebSocketServer::start_with_admission(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        InMemoryServerShellConfig::new(schema.clone(), identity(0x5e, 0x01)).with_row_id_seed(0x5e),
        "/sync",
        config,
    )
    .expect("start local-first JWT loopback WebSocket listener");
    let addr = server.local_addr();

    let header_subject = "local-first:header-owner";
    let header_author = author_id_from_subject(header_subject);
    let header_owner = DbIdentity {
        node: NodeUuid::from_bytes([0xc6; 16]),
        author: header_author,
    };
    let (mut runtime, runtime_handle, db) = open_client_runtime(schema.clone(), header_owner);
    let transport = attach_upstream(&mut runtime, runtime_handle, db);
    let token = signed_local_first_eddsa_jwt(header_subject, 4_102_444_800);
    let mut socket = connect_sync_with_bearer(addr, &token);

    expect_ack(
        runtime.db_insert_with_id(
            Some(76),
            db,
            "todos".to_owned(),
            RowUuid::from_bytes([0x46; 16]),
            postcard::to_allocvec(&owned_todo_cells(
                "local-first jwt websocket identity",
                true,
                header_author,
            ))
            .unwrap(),
        ),
    );
    let write = drain_events(&mut runtime, runtime_handle, 77)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteStarted { write, .. } => Some(write),
            _ => None,
        })
        .unwrap();

    assert!(pump_websocket(
        &mut socket,
        &mut runtime,
        runtime_handle,
        db,
        transport,
        1000,
    ));
    expect_ack(runtime.db_get_write_state(Some(9706), WriteStateTarget::Handle(write)));
    let state = drain_events(&mut runtime, runtime_handle, 9707)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteState { state, .. } => Some(state),
            _ => None,
        })
        .unwrap();
    assert_eq!(state.fate, Fate::Accepted);

    let handshake_subject = "local-first:handshake-owner";
    let handshake_author = author_id_from_subject(handshake_subject);
    let handshake_owner = DbIdentity {
        node: NodeUuid::from_bytes([0xc7; 16]),
        author: handshake_author,
    };
    let (mut handshake_runtime, handshake_runtime_handle, handshake_db) =
        open_client_runtime(schema, handshake_owner);
    let handshake_transport = attach_upstream(
        &mut handshake_runtime,
        handshake_runtime_handle,
        handshake_db,
    );
    let handshake_token = signed_local_first_eddsa_jwt(handshake_subject, 4_102_444_800);
    let mut handshake_socket = connect_sync(addr);
    send_auth_handshake(
        &mut handshake_socket,
        &handshake_token,
        "ignored-handshake-sub",
        BTreeMap::new(),
    );

    expect_ack(
        handshake_runtime.db_insert_with_id(
            Some(78),
            handshake_db,
            "todos".to_owned(),
            RowUuid::from_bytes([0x47; 16]),
            postcard::to_allocvec(&owned_todo_cells(
                "local-first first-frame websocket identity",
                true,
                handshake_author,
            ))
            .unwrap(),
        ),
    );
    let handshake_write = drain_events(&mut handshake_runtime, handshake_runtime_handle, 79)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteStarted { write, .. } => Some(write),
            _ => None,
        })
        .unwrap();

    assert!(pump_websocket(
        &mut handshake_socket,
        &mut handshake_runtime,
        handshake_runtime_handle,
        handshake_db,
        handshake_transport,
        1100,
    ));
    expect_ack(
        handshake_runtime.db_get_write_state(Some(9708), WriteStateTarget::Handle(handshake_write)),
    );
    let handshake_state = drain_events(&mut handshake_runtime, handshake_runtime_handle, 9709)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteState { state, .. } => Some(state),
            _ => None,
        })
        .unwrap();
    assert_eq!(handshake_state.fate, Fate::Accepted);

    server.shutdown();
}

#[test]
fn loopback_websocket_local_first_jwt_wrong_audience_is_rejected() {
    let mut config = AuthAdmissionConfig::jwt(JwtVerifierConfig::ed_public_key_pem(
        ED25519_PUBLIC_KEY_PEM.as_bytes(),
    ))
    .with_expected_audience("auth-app");
    config.allow_local_first_auth = true;
    let server = LoopbackWebSocketServer::start_with_admission(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        InMemoryServerShellConfig::new(todos_schema(), identity(0x5e, 0x01)),
        "/sync",
        config,
    )
    .expect("start local-first JWT loopback WebSocket listener");
    let mut socket = connect_sync(server.local_addr());
    let token = signed_local_first_eddsa_jwt_for_app(
        "local-first:wrong-app",
        4_102_444_800,
        Some("other-app"),
        Some("other-app"),
    );
    send_auth_handshake(
        &mut socket,
        &token,
        "ignored-handshake-sub",
        BTreeMap::new(),
    );

    let result = socket.read();
    assert!(
        matches!(result, Ok(Message::Close(_)) | Err(_)),
        "wrong-audience local-first JWT should close or drop the socket, got {result:?}"
    );

    server.shutdown();
}

#[test]
fn loopback_websocket_expired_jwt_is_rejected() {
    let secret = b"expired-jwt-secret";
    let server = LoopbackWebSocketServer::start_with_admission(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        InMemoryServerShellConfig::new(todos_schema(), identity(0x5e, 0x01)),
        "/sync",
        AuthAdmissionConfig::jwt(jazz_server::auth_admission::JwtVerifierConfig::hmac_secret(
            Algorithm::HS256,
            secret.to_vec(),
        )),
    )
    .expect("start JWT loopback WebSocket listener");
    let mut socket = connect_sync(server.local_addr());
    let token = signed_hs256_jwt(secret, "expired-user", 1, json!({}));
    send_auth_handshake(&mut socket, &token, "expired-user", BTreeMap::new());

    let result = socket.read();
    assert!(
        matches!(result, Ok(Message::Close(_)) | Err(_)),
        "expired JWT should close or drop the socket, got {result:?}"
    );

    server.shutdown();
}

const ED25519_PRIVATE_KEY_PEM: &str = "\
-----BEGIN PRIVATE KEY-----\n\
MC4CAQAwBQYDK2VwBCIEIGrD/e7uKYqSY4twDEsRfMMuLSrODf14dpTiTK6K1YI0\n\
-----END PRIVATE KEY-----";

const ED25519_PUBLIC_KEY_PEM: &str = "\
-----BEGIN PUBLIC KEY-----\n\
MCowBQYDK2VwAyEA2+Jj2UvNCvQiUPNYRgSi0cJSPiJI6Rs6D0UTeEpQVj8=\n\
-----END PUBLIC KEY-----";
