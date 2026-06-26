use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};

use std::collections::BTreeMap;

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
use jazz::query::Query;
use jazz::schema::{ColumnSchema, JazzSchema, TableSchema};
use jazz::tx::Fate;
use jazz_server::{InMemoryServerShellConfig, loopback_http::LoopbackHttpServer};

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

fn pump_http(
    addr: SocketAddr,
    session_id: u64,
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
        let client_frames = take_client_frames(runtime, runtime_handle, transport, request_id + 1);
        let response = request(
            addr,
            "POST",
            &format!("/sessions/{session_id}/frames"),
            &render_hex_frames(&client_frames),
        );
        assert_eq!(response.status, 200);
        let server_frames = parse_hex_frames(&response.body);
        for frame in server_frames {
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

#[test]
fn loopback_http_listener_exposes_health_sessions_frames_and_metrics() {
    let server = LoopbackHttpServer::start_default(SocketAddr::from(([127, 0, 0, 1], 0)))
        .expect("start loopback HTTP listener");
    let addr = server.local_addr();

    let health = request(addr, "GET", "/healthz", "");
    assert_eq!(health.status, 200);
    assert!(health.body.contains("status=ready"));

    let created = request(addr, "POST", "/sessions", "");
    assert_eq!(created.status, 201);
    let session_id = created
        .body
        .strip_prefix("id=")
        .and_then(|line| line.trim().parse::<u64>().ok())
        .expect("session id response");

    // Loopback frame payloads are newline-separated hex-encoded WireFrame bytes.
    // An empty body sends no frames, but still proves endpoint routing, session
    // lookup, shell tick, and outbound frame draining through HTTP.
    let frames = request(addr, "POST", &format!("/sessions/{session_id}/frames"), "");
    assert_eq!(frames.status, 200);
    assert!(frames.body.is_empty());

    let metrics = request(addr, "GET", "/metrics", "");
    assert_eq!(metrics.status, 200);
    assert!(metrics.body.contains("active_sessions=1"));
    assert!(metrics.body.contains("total_sessions=1"));
    assert!(metrics.body.contains("frames_received=0"));
    assert!(metrics.body.contains("ticks=1"));

    server.shutdown();
}

#[test]
fn loopback_http_round_trips_real_sync_frames_to_client_watch() {
    let schema = todos_schema();
    let client_identity = identity(0xc1, 0xc1);
    let server = LoopbackHttpServer::start(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        InMemoryServerShellConfig::new(schema.clone(), identity(0x5e, 0x01)).with_row_id_seed(0x5e),
    )
    .expect("start loopback HTTP listener");
    let addr = server.local_addr();

    let (mut runtime, runtime_handle, db) = open_client_runtime(schema.clone(), client_identity);
    let transport = attach_upstream(&mut runtime, runtime_handle, db);

    let created = request(addr, "POST", "/sessions", "");
    assert_eq!(created.status, 201);
    let session_id = created
        .body
        .strip_prefix("id=")
        .and_then(|line| line.trim().parse::<u64>().ok())
        .expect("session id response");

    expect_ack(runtime.db_insert_with_id(
        Some(40),
        db,
        "todos".to_owned(),
        RowUuid::from_bytes([0x41; 16]),
        postcard::to_allocvec(&todo_cells("over http", true)).unwrap(),
    ));
    let write = drain_events(&mut runtime, runtime_handle, 41)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteStarted { write, .. } => Some(write),
            _ => None,
        })
        .unwrap();

    assert!(pump_http(
        addr,
        session_id,
        &mut runtime,
        runtime_handle,
        db,
        transport,
        100,
    ));
    expect_ack(runtime.db_get_write_state(Some(9500), WriteStateTarget::Handle(write)));
    let state = drain_events(&mut runtime, runtime_handle, 9501)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteState { state, .. } => Some(state),
            _ => None,
        })
        .unwrap();
    assert_eq!(state.fate, Fate::Accepted);

    let (mut reader_runtime, reader_runtime_handle, reader_db) =
        open_client_runtime(schema, identity(0xc2, 0xc1));
    let mut reader_subscription =
        prepare_todos_subscription(&mut reader_runtime, reader_runtime_handle, reader_db);
    let reader_transport = attach_upstream(&mut reader_runtime, reader_runtime_handle, reader_db);
    let reader_created = request(addr, "POST", "/sessions", "");
    assert_eq!(reader_created.status, 201);
    let reader_session_id = reader_created
        .body
        .strip_prefix("id=")
        .and_then(|line| line.trim().parse::<u64>().ok())
        .expect("reader session id response");

    assert!(pump_http(
        addr,
        reader_session_id,
        &mut reader_runtime,
        reader_runtime_handle,
        reader_db,
        reader_transport,
        1000,
    ));
    assert_eq!(
        reader_subscription.fields(),
        vec![("over http".to_owned(), true)]
    );

    let metrics = request(addr, "GET", "/metrics", "");
    assert_eq!(metrics.status, 200);
    assert!(metrics.body.contains("frames_received="));
    assert!(!metrics.body.contains("frames_received=0\n"));
    assert!(metrics.body.contains("frames_sent="));
    assert!(!metrics.body.contains("frames_sent=0\n"));

    server.shutdown();
}

#[derive(Debug)]
struct Response {
    status: u16,
    body: String,
}

fn request(addr: SocketAddr, method: &str, path: &str, body: &str) -> Response {
    request_attempt(addr, method, path, body, 0)
}

fn request_attempt(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: &str,
    attempt: usize,
) -> Response {
    let mut stream = TcpStream::connect(addr).expect("connect loopback HTTP listener");
    if let Err(error) = write!(
        stream,
        "{method} {path} HTTP/1.1\r\nhost: {addr}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    ) {
        if matches!(
            error.kind(),
            io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
        ) && attempt < 3
        {
            return request_attempt(addr, method, path, body, attempt + 1);
        }
        panic!("write HTTP request: {error}");
    }

    let mut raw = Vec::new();
    match stream.read_to_end(&mut raw) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::ConnectionReset && !raw.is_empty() => {}
        Err(error) if error.kind() == io::ErrorKind::ConnectionReset && attempt < 3 => {
            return request_attempt(addr, method, path, body, attempt + 1);
        }
        Err(error) => panic!("read HTTP response: {error}"),
    }
    let response = String::from_utf8(raw).expect("HTTP response is utf-8");
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .expect("response has header terminator");
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .expect("response status");

    Response {
        status,
        body: body.to_owned(),
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

fn parse_hex_frames(body: &str) -> Vec<Vec<u8>> {
    body.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(decode_hex)
        .collect::<Result<_, _>>()
        .expect("valid hex frame response")
}

fn decode_hex(text: &str) -> Result<Vec<u8>, String> {
    if text.len() % 2 != 0 {
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
