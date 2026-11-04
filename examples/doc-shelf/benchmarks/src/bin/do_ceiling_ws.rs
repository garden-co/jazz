//! The single-object ceiling workload against a real Durable Object: a
//! Jazz authority on the object's own SQLite in workerd, reached over
//! WebSockets (dev/durable-objects/native-sqlite/harness).
//!
//! A collaborative task app: `tasks` rows in projects of `PROJECT` (200),
//! `GROUP` connected users per project, each subscribed to its project. A
//! user write is one transaction updating `ROWS` tasks of the writer's
//! project, so it fans out to the project's other subscribers. Every
//! session is a native in-memory replica subscribed to its project; the
//! first of each of `WRITERS` groups writes. Each write waits for the
//! authority's acknowledgement before the next one starts.
//!
//! Prints the object's own tick time per write (its single thread), the
//! round trip a writer sees, and the object's wasm memory before and after
//! the sessions connect.
//!
//! `STORE=10000 SESSIONS=50 GROUP=5 ROWS=10 WRITES=200 doc-shelf-do-ceiling-ws ws://127.0.0.1:8787`
//!
//! With `NATIVE=1` the authority is a native in-memory store in this
//! process instead, reached over in-memory queues: the same workload's
//! baseline, with only the authority's ticks timed.

use std::collections::BTreeMap;
use std::net::TcpStream;
use std::time::{Duration, Instant};

use jazz::binding_host::{OpenConfig, OpenIdentity};
use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, ReadTier, TransportError, WireTransport,
    WireTransportAdapter, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::test_support::AllowAll;
use jazz::tools::{ColumnType, Schema, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use serde_json::{Value as Json, json};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

fn env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn project_tasks() -> usize {
    static TASKS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *TASKS.get_or_init(|| env("PROJECT", 200))
}

fn schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("tasks")
                .column("project", ColumnType::Text)
                .column("title", ColumnType::Text)
                .column("status", ColumnType::Text)
                .column("rank", ColumnType::Integer)
                .column("note", ColumnType::Text)
                .index_only(["project"]),
        )
        .build()
        .allow_all()
}

fn node(n: u16) -> NodeUuid {
    let mut bytes = [0x5a; 16];
    bytes[14..].copy_from_slice(&n.to_be_bytes());
    NodeUuid::from_bytes(bytes)
}

fn author(s: usize) -> AuthorSubject {
    AuthorSubject::authenticated("https://bench.jazz.test", &format!("user-{s}")).expect("author")
}

fn task_id(n: usize) -> RowUuid {
    let mut bytes = [0u8; 16];
    bytes[0] = 0x7a;
    bytes[8..].copy_from_slice(&(n as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn project(p: usize) -> String {
    format!("project-{p:06}")
}

fn task(n: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "project".into(),
            Value::String(project(n / project_tasks())),
        ),
        (
            "title".into(),
            Value::String(format!("Task {n}: follow up")),
        ),
        ("status".into(), Value::String("open".into())),
        ("rank".into(), Value::I32(n as i32)),
        (
            "note".into(),
            Value::String(format!("Notes for task {n}. ").repeat(4)),
        ),
    ])
}

fn project_query(p: usize) -> Query {
    Query::from("tasks").filter(eq(col("project"), lit(project(p))))
}

type Socket = WebSocket<MaybeTlsStream<TcpStream>>;

fn connect(url: &str) -> Socket {
    let (socket, _) = tungstenite::connect(url).unwrap_or_else(|e| panic!("connect {url}: {e}"));
    if let MaybeTlsStream::Plain(stream) = socket.get_ref() {
        stream.set_nodelay(true).expect("nodelay");
        stream.set_nonblocking(true).expect("nonblocking");
    }
    socket
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n = chunk.iter().fold(0u32, |n, &b| (n << 8) | b as u32) << (8 * (3 - chunk.len()));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[(n >> (18 - 6 * i)) as usize & 63] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn percent(text: &str) -> String {
    text.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// One request on the control socket, or to the native authority.
fn control(socket: &mut Option<Socket>, request: Json) -> Json {
    let Some(socket) = socket else {
        return match request["op"].as_str() {
            Some("stats") => local_stats(request["reset"] == true),
            _ => json!({ "ok": true, "openMs": 0 }),
        };
    };
    socket
        .send(Message::text(request.to_string()))
        .expect("send control");
    loop {
        match socket.read() {
            Ok(Message::Text(text)) => {
                let reply: Json = serde_json::from_str(&text).expect("control reply");
                assert!(reply["ok"] == true, "control {request}: {reply}");
                return reply;
            }
            Ok(_) => {}
            Err(tungstenite::Error::Io(e)) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_micros(200));
            }
            Err(e) => panic!("control: {e}"),
        }
    }
}

/// A sync session over one WebSocket: each message one wire frame.
struct Wire {
    socket: Socket,
}

impl Wire {
    fn new(url: &str) -> Self {
        Self {
            socket: connect(url),
        }
    }

    fn recv(&mut self) -> Option<Vec<u8>> {
        loop {
            match self.socket.read() {
                Ok(Message::Binary(frame)) => return Some(frame.to_vec()),
                Ok(_) => {}
                Err(tungstenite::Error::Io(e)) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    return None;
                }
                Err(e) => panic!("session: {e}"),
            }
        }
    }
}

impl WireTransport for Wire {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        match self.socket.send(Message::binary(frame)) {
            Ok(()) => {}
            // Queued: flush until the socket takes it.
            Err(tungstenite::Error::Io(e)) if e.kind() == std::io::ErrorKind::WouldBlock => {
                while let Err(tungstenite::Error::Io(e)) = self.socket.flush() {
                    assert_eq!(e.kind(), std::io::ErrorKind::WouldBlock, "flush: {e}");
                    std::thread::sleep(Duration::from_micros(100));
                }
            }
            Err(e) => panic!("send: {e}"),
        }
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.recv()
    }
}

/// Tick `db` until `done`, giving up after a minute.
fn pump(db: &Db, mut done: impl FnMut() -> bool) {
    let start = Instant::now();
    while !done() {
        block_on(db.tick()).expect("replica tick");
        tick_authority();
        assert!(
            start.elapsed() < Duration::from_secs(TIMEOUT_S),
            "timed out"
        );
        std::thread::sleep(Duration::from_micros(50));
    }
}

const TIMEOUT_S: u64 = 20;

thread_local! {
    /// The `NATIVE=1` authority, and the wall time of each of its ticks.
    static LOCAL: std::cell::RefCell<Option<(Db, Vec<f64>)>> = const { std::cell::RefCell::new(None) };
}

/// Tick the native authority, if there is one.
fn tick_authority() {
    LOCAL.with(|local| {
        if let Some((db, ticks)) = local.borrow_mut().as_mut() {
            let start = Instant::now();
            block_on(db.tick()).expect("authority tick");
            ticks.push(start.elapsed().as_secs_f64() * 1e3);
        }
    });
}

/// The native authority's stats, shaped as the object's.
fn local_stats(reset: bool) -> Json {
    LOCAL.with(|local| {
        let mut local = local.borrow_mut();
        let (_, ticks) = local.as_mut().expect("native authority");
        // Ticks with nothing to do are not the write's.
        let busy: Vec<f64> = ticks.iter().copied().filter(|t| *t > 0.002).collect();
        let out = json!({
            "ok": true,
            "ticks": busy.len(),
            "tickMsTotal": ticks.iter().sum::<f64>(),
            "tickMsP99": 0,
            "frames": { "out": 0, "outBytes": 0, "in": 0 },
            "sql": {},
            "schedules": 0,
        });
        if reset {
            ticks.clear();
        }
        out
    })
}

/// A frame queue pair end, for the native authority.
struct Queue {
    outbound: std::rc::Rc<std::cell::RefCell<std::collections::VecDeque<Vec<u8>>>>,
    inbound: std::rc::Rc<std::cell::RefCell<std::collections::VecDeque<Vec<u8>>>>,
}

impl WireTransport for Queue {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.outbound.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.inbound.borrow_mut().pop_front()
    }
}

fn queue_pair() -> (Queue, Queue) {
    let up = std::rc::Rc::default();
    let down = std::rc::Rc::default();
    (
        Queue {
            outbound: std::rc::Rc::clone(&up),
            inbound: std::rc::Rc::clone(&down),
        },
        Queue {
            outbound: down,
            inbound: up,
        },
    )
}

fn settled(db: &Db, tx: u64) -> bool {
    use futures_util::FutureExt;
    match db
        .wait_for_transaction(tx, DurabilityTier::Global)
        .now_or_never()
    {
        Some(Ok(_)) => true,
        Some(Err(error)) => panic!("write rejected: {error}"),
        None => false,
    }
}

fn mb(bytes: &Json) -> f64 {
    bytes.as_f64().unwrap_or(0.0) / 1e6
}

fn main() {
    let base = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "ws://127.0.0.1:8787".into());
    let store = env("STORE", 10_000);
    let sessions = env("SESSIONS", 50);
    let group = env("GROUP", 5).max(1);
    let rows = env("ROWS", 10).min(project_tasks());
    let writes = env("WRITES", 200);
    let projects = store.div_ceil(project_tasks());
    let groups = sessions.div_ceil(group);
    assert!(groups <= projects, "more project groups than projects");
    let writers = groups.min(env("WRITERS", 20)).max(1);

    let native = std::env::var_os("NATIVE").is_some();
    let scope_test = std::env::var_os("SCOPE_TEST").is_some();
    let verify_only = std::env::var_os("VERIFY_ONLY").is_some();
    let mut control_socket = (!native).then(|| connect(&format!("{base}/control")));
    if native {
        let config = DbConfig::new(
            JazzSchema::new(&schema()).expect("schema compiles"),
            DbIdentity {
                node: node(0),
                author: AuthorSubject::SYSTEM,
            },
        );
        let db = block_on(Db::open(config)).expect("open authority");
        db.set_authority();
        LOCAL.with(|local| *local.borrow_mut() = Some((db, Vec::new())));
    }
    let config = OpenConfig {
        identity: OpenIdentity {
            node: node(0),
            author: author(usize::MAX),
        },
        row_id_seed: None,
        history_complete: true,
        initial_sync_flush_every: None,
        migrations: None,
        system_node: None,
    };
    let opened = control(
        &mut control_socket,
        json!({
            "op": "open",
            "schema": base64(&serde_json::to_vec(&schema()).expect("schema json")),
            "config": base64(&postcard::to_allocvec(&config).expect("config")),
        }),
    );
    eprintln!("opened in {} ms", opened["openMs"]);
    if scope_test {
        if verify_only {
            control(
                &mut control_socket,
                json!({ "op": "checkRejected", "row": task_id(store + 1).to_bytes() }),
            );
        } else {
            control(&mut control_socket, json!({ "op": "scopeChecks" }));
        }
    }

    let replica = |s: usize| {
        let config = DbConfig::new(
            JazzSchema::new(&schema()).expect("schema compiles"),
            DbIdentity {
                node: node(1 + s as u16 + if verify_only { 1000 } else { 0 }),
                author: author(s),
            },
        );
        let db = block_on(Db::open(config)).expect("open replica");
        let transport: Box<dyn jazz::db::Transport> = if native {
            let (client, server) = queue_pair();
            LOCAL.with(|local| {
                let local = local.borrow();
                let (authority, _) = local.as_ref().expect("native authority");
                authority
                    .accept_subscriber(Box::new(WireTransportAdapter::current(server)), author(s))
                    .expect("accept");
            });
            Box::new(WireTransportAdapter::current(client))
        } else {
            Box::new(WireTransportAdapter::current(Wire::new(&format!(
                "{base}/session?author={}",
                percent(author(s).canonical())
            ))))
        };
        let link = block_on(db.connect_upstream(transport));
        (db, link)
    };

    // Seed through one replica, a thousand rows per upload.
    let seeding = Instant::now();
    let (seeder, _seed_link) = replica(10_000);
    for chunk in (0..if verify_only { 0 } else { store }).step_by(1000) {
        let (_, receipt) = block_on(seeder.transaction(async |tx| {
            for n in chunk..(chunk + 1000).min(store) {
                tx.insert(
                    "tasks",
                    task(n),
                    InsertOptions {
                        row_id: Some(task_id(n)),
                        ..Default::default()
                    },
                )
                .await?;
            }
            Ok(())
        }))
        .expect("seed");
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pump(&seeder, || settled(&seeder, receipt.0))
        }))
        .is_err()
        {
            let stats = control(&mut control_socket, json!({ "op": "stats" }));
            panic!("seeding stalled; object: {stats:#}");
        }
    }
    let seeded = seeding.elapsed();
    let before = control(&mut control_socket, json!({ "op": "stats", "reset": true }));

    // Project g holds group g; the first session of each of `writers`
    // groups writes.
    let connecting = Instant::now();
    let mut replicas = Vec::new();
    for s in 0..sessions {
        let g = s / group;
        let (db, link) = replica(s);
        let prepared = db.prepare_query(&project_query(g)).expect("prepare");
        let stream = block_on(db.subscribe(
            &prepared,
            ReadOpts {
                tier: ReadTier::Remote,
                ..ReadOpts::default()
            },
        ))
        .expect("subscribe");
        replicas.push((db, g, stream, link));
    }
    let tick_all = |replicas: &mut Vec<_>| {
        let mut events = 0;
        tick_authority();
        for (db, _, stream, _) in replicas.iter_mut() {
            let db: &Db = db;
            block_on(db.tick()).expect("replica tick");
            let stream: &mut jazz::db::SubscriptionStream = stream;
            while stream.try_next_event().is_some() {
                events += 1;
            }
        }
        tick_authority();
        events
    };
    // Every session holds its project once nothing arrives for a while.
    let mut quiet = 0;
    while quiet < 20 {
        quiet = if tick_all(&mut replicas) == 0 {
            quiet + 1
        } else {
            0
        };
        std::thread::sleep(Duration::from_millis(5));
    }
    let connected = connecting.elapsed();
    let after_connect = control(&mut control_socket, json!({ "op": "stats", "reset": true }));

    if scope_test && !verify_only {
        use futures_util::FutureExt;
        // Mallory has no subscription and therefore no copy of the seeded
        // rows. Her local commit succeeds; the authority must reject the
        // second insert and roll back the first, including its history.
        let (mallory, _link) = replica(20_000);
        let (_, receipt) = block_on(mallory.transaction(async |tx| {
            for n in [store + 1, 0] {
                let mut cells = task(n);
                cells.insert("project".into(), Value::String(project(0)));
                tx.insert(
                    "tasks",
                    cells,
                    InsertOptions {
                        row_id: Some(task_id(n)),
                        ..Default::default()
                    },
                )
                .await?;
            }
            Ok(())
        }))
        .expect("stale replica accepts its local batch");
        pump(&mallory, || {
            match mallory
                .wait_for_transaction(receipt.0, DurabilityTier::Global)
                .now_or_never()
            {
                None => false,
                Some(Ok(_)) => panic!("authority accepted a conflicting batch"),
                Some(Err(error)) => {
                    assert_eq!(error.code, jazz::db::ErrorCode::WriteRejected);
                    true
                }
            }
        });
        // Capture every subscriber update after rejection, before valid
        // writes begin; no speculative row may have been published.
        for _ in 0..20 {
            assert_eq!(
                tick_all(&mut replicas),
                0,
                "rejected batch published a subscriber update"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        control(
            &mut control_socket,
            json!({ "op": "checkRejected", "row": task_id(store + 1).to_bytes() }),
        );
        eprintln!("scope rejection passed");
    }

    let writer_slots: Vec<usize> = (0..sessions)
        .filter(|s| s % group == 0 && s / group < writers)
        .collect();
    // profile.mjs profiles between these two lines.
    eprintln!("phase: writing");
    let mut trips = Vec::with_capacity(writes);
    let mut events = 0;
    for done in 0..if verify_only { 0 } else { writes } {
        let (db, g, ..) = &replicas[writer_slots[done % writer_slots.len()]];
        let base_row = g * project_tasks();
        let offset = (done * 7) % (project_tasks() - rows + 1);
        let stamp = done as i32;
        let start = Instant::now();
        let (_, receipt) = block_on(db.transaction(async |tx| {
            for n in base_row + offset..base_row + offset + rows {
                tx.update(
                    "tasks",
                    task_id(n),
                    BTreeMap::from([
                        ("status".into(), Value::String(format!("s{stamp}"))),
                        ("rank".into(), Value::I32(stamp)),
                    ]),
                    Default::default(),
                )
                .await?;
            }
            Ok(())
        }))
        .expect("user write");
        pump(db, || settled(db, receipt.0));
        trips.push(start.elapsed());
        events += tick_all(&mut replicas);
    }
    // Let the last fan-out land before reading the object's numbers.
    std::thread::sleep(Duration::from_millis(200));
    events += tick_all(&mut replicas);
    eprintln!("phase: written");
    let written = control(&mut control_socket, json!({ "op": "stats", "reset": true }));
    if !native {
        assert_eq!(written["errors"], json!([]), "authority errors: {written}");
    }
    if std::env::var_os("SMOKE").is_some() {
        // Check the published rows, not just acknowledgements or tick timings.
        for (db, group_id, _, _) in &replicas {
            let start = group_id * project_tasks();
            let end = (start + project_tasks()).min(store);
            let mut expected = (start..end)
                .map(|n| (format!("Task {n}: follow up"), "open".to_owned()))
                .collect::<BTreeMap<_, _>>();
            for done in 0..writes {
                if writer_slots[done % writer_slots.len()] / group != *group_id {
                    continue;
                }
                let offset = (done * 7) % (project_tasks() - rows + 1);
                for n in start + offset..start + offset + rows {
                    expected.insert(format!("Task {n}: follow up"), format!("s{done}"));
                }
            }
            let prepared = db
                .prepare_query(&project_query(*group_id))
                .expect("smoke query");
            let actual = block_on(db.all(&prepared, ReadOpts::default()))
                .expect("smoke rows")
                .iter()
                .map(|row| {
                    let text = |name| {
                        row.cells()
                            .find_map(|(column, value)| match value {
                                Some(Value::String(text)) if column == name => Some(text.clone()),
                                _ => None,
                            })
                            .expect("text column")
                    };
                    (text("title"), text("status"))
                })
                .collect::<BTreeMap<_, _>>();
            assert_eq!(actual, expected, "subscriber in project {group_id}");
        }
        if verify_only {
            println!(
                "Restart verification passed: {store} rows, {sessions} fresh subscribers; persisted contents agree."
            );
        } else {
            println!(
                "Smoke passed: {store} seeded rows, {sessions} subscribers, {writes} acknowledged writes; subscriber contents agree."
            );
        }
        LOCAL.with(|local| std::mem::forget(local.borrow_mut().take()));
        return;
    }

    trips.sort();
    let pct = |p: f64| trips[((trips.len() - 1) as f64 * p) as usize].as_secs_f64() * 1e3;
    let tick_ms = written["tickMsTotal"].as_f64().unwrap_or(0.0);
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "store_rows": store,
            "sessions": sessions,
            "group": group,
            "rows_per_write": rows,
            "writes": writes,
            "seed_s": seeded.as_secs_f64(),
            "connect_s": connected.as_secs_f64(),
            "object_ms_per_write": tick_ms / writes as f64,
            "object_schedules_per_write": written["schedules"].as_f64().unwrap_or(0.0) / writes as f64,
            "object_frames_in_per_write": written["frames"]["in"].as_f64().unwrap_or(0.0) / writes as f64,
            "object_ticks_per_write": written["ticks"].as_f64().unwrap_or(0.0) / writes as f64,
            "object_tick_p99_ms": written["tickMsP99"],
            "object_ceiling_writes_per_s": writes as f64 / (tick_ms / 1e3),
            "round_trip_ms_p50": pct(0.5),
            "round_trip_ms_p99": pct(0.99),
            "sql_per_write": {
                "execs": written["sql"]["execs"].as_f64().unwrap_or(0.0) / writes as f64,
                "rows_read": written["sql"]["rowsRead"].as_f64().unwrap_or(0.0) / writes as f64,
                "rows_written": written["sql"]["rowsWritten"].as_f64().unwrap_or(0.0) / writes as f64,
                "lookups": written["sql"]["lookups"].as_f64().unwrap_or(0.0) / writes as f64,
            },
            "subscriber_events_per_write": events as f64 / writes as f64,
            "object_frames_out_per_write": written["frames"]["out"].as_f64().unwrap_or(0.0) / writes as f64,
            "object_bytes_out_per_write": written["frames"]["outBytes"].as_f64().unwrap_or(0.0) / writes as f64,
            "wasm_memory_mb": {
                "seeded": mb(&before["wasmMemoryBytes"]),
                "connected": mb(&after_connect["wasmMemoryBytes"]),
                "after_writes": mb(&written["wasmMemoryBytes"]),
            },
            "database_mb": mb(&written["databaseBytes"]),
            "connect_object_ms": after_connect["tickMsTotal"],
            "sql_totals": written["sql"],
            "queries": written["queries"],
        }))
        .expect("json")
    );
    // The process exits next; dropping the native authority in TLS
    // teardown would touch other thread locals already destroyed.
    LOCAL.with(|local| std::mem::forget(local.borrow_mut().take()));
}
