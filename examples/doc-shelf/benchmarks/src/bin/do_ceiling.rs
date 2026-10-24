//! How many user writes per second one unsharded authority sustains: the
//! single-object ceiling of a Jazz app on one Durable Object.
//!
//! A collaborative task app: `tasks` rows in projects of 200, `GROUP`
//! connected users per project, each subscribed to its project. A user
//! write is one transaction updating `ROWS` tasks of the writer's project,
//! so it fans out to the project's other subscribers. The authority is a
//! file store holding `STORE` tasks. Each round, `BATCH` writers write and
//! the authority ticks once; only the authority's tick is timed (it is the
//! object's single thread). Writers are replicas; the other subscribers are
//! bare sessions whose frames are counted and dropped.
//!
//! `STORE=100000 SESSIONS=100 GROUP=5 BATCH=1 ROWS=10 WRITES=500 doc-shelf-do-ceiling`

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;
use std::time::{Duration, Instant};

use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, ReadTier, SyncMessage,
    TransportError, WireTransport, WireTransportAdapter, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::test_support::AllowAll;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

/// Tasks per project (`PROJECT`, default 200): what each session's
/// subscription holds.
fn project_tasks() -> usize {
    static TASKS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *TASKS.get_or_init(|| env("PROJECT", 200))
}

fn env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// A phase mark in the `JAZZ_SQL_TRACE` file (see `do_cost`).
fn mark(phase: &str) {
    if let Some(path) = std::env::var_os("JAZZ_SQL_TRACE") {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .expect("open trace");
        writeln!(file, "{}", serde_json::json!({ "mark": phase })).expect("mark trace");
    }
}

fn config(node: u16) -> DbConfig {
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
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
            .allow_all(),
    )
    .expect("schema compiles");
    let mut bytes = [0x5a; 16];
    bytes[14..].copy_from_slice(&node.to_be_bytes());
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes(bytes),
            author: AuthorSubject::SYSTEM,
        },
    )
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

#[derive(Default)]
struct Counter {
    frames: usize,
    bytes: usize,
}

/// A frame queue pair; `sent` counts what this side sends.
struct Wire {
    outbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    inbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    sent: Rc<RefCell<Counter>>,
}

impl WireTransport for Wire {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        let mut sent = self.sent.borrow_mut();
        sent.frames += 1;
        sent.bytes += frame.len();
        self.outbound.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.inbound.borrow_mut().pop_front()
    }
}

type Queue = Rc<RefCell<VecDeque<Vec<u8>>>>;

fn pair() -> (Wire, Wire, Queue, Rc<RefCell<Counter>>) {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let down_sent = Rc::new(RefCell::new(Counter::default()));
    (
        Wire {
            outbound: up.clone(),
            inbound: down.clone(),
            sent: Rc::new(RefCell::new(Counter::default())),
        },
        Wire {
            outbound: down.clone(),
            inbound: up,
            sent: down_sent.clone(),
        },
        down,
        down_sent,
    )
}

fn project_query(p: usize) -> Query {
    Query::from("tasks").filter(eq(col("project"), lit(project(p))))
}

fn main() {
    let store = env("STORE", 100_000);
    let sessions = env("SESSIONS", 100);
    let group = env("GROUP", 5).max(1);
    let batch = env("BATCH", 1).max(1);
    let rows = env("ROWS", 10).min(project_tasks());
    let writes = env("WRITES", 500);
    let projects = store / project_tasks();
    let groups = sessions.div_ceil(group);
    assert!(groups <= projects, "more project groups than projects");
    let writers = groups.min(env("WRITERS", 20)).max(batch.min(groups));

    let dir = tempfile::tempdir().expect("scratch dir");
    let path = dir.path().join("authority.sqlite");
    let server =
        block_on(Db::open_at(config(0), &path, FileOptions::default())).expect("open authority");
    mark("seed");
    let seeding = Instant::now();
    for chunk in (0..store).step_by(1000) {
        block_on(server.transaction(async |tx| {
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
    }
    let seeded = seeding.elapsed();

    // Project g holds group g: `writers` of the groups have a replica that
    // writes, every other session is a bare subscriber.
    let mut replicas = Vec::new();
    let mut bare: Vec<(Queue, Rc<RefCell<Counter>>)> = Vec::new();
    let mut down_counters = Vec::new();
    for s in 0..sessions {
        let g = s / group;
        let (client, authority, down, sent) = pair();
        down_counters.push(sent.clone());
        if s % group == 0 && g < writers {
            let db = block_on(Db::open(config(1 + s as u16))).expect("open replica");
            let _upstream =
                block_on(db.connect_upstream(Box::new(WireTransportAdapter::current(client))));
            server.accept_subscriber(
                Box::new(WireTransportAdapter::current(authority)),
                AuthorSubject::SYSTEM,
            );
            let prepared = db.prepare_query(&project_query(g)).expect("prepare");
            let stream = block_on(db.subscribe(
                &prepared,
                ReadOpts {
                    tier: ReadTier::Remote,
                    ..ReadOpts::default()
                },
            ))
            .expect("subscribe");
            replicas.push((db, g, stream, _upstream));
        } else {
            let subscribe = SyncMessage::Subscribe {
                id: 1,
                query: project_query(g),
                binding: BTreeMap::new(),
                since: None,
                include_deleted: false,
                tier: ReadTier::Remote,
            };
            for frame in jazz::db::encode_frames(subscribe).expect("encode") {
                client.outbound.borrow_mut().push_back(frame);
            }
            server.accept_subscriber(
                Box::new(WireTransportAdapter::current(authority)),
                AuthorSubject::SYSTEM,
            );
            bare.push((down, sent));
        }
    }
    let drain = |bare: &[(Queue, Rc<RefCell<Counter>>)]| {
        for (down, _) in bare {
            down.borrow_mut().clear();
        }
    };
    mark("connect");
    let connecting = Instant::now();
    for _ in 0..8 {
        for (db, ..) in &replicas {
            block_on(db.tick()).expect("replica tick");
        }
        block_on(server.tick()).expect("authority tick");
        drain(&bare);
    }
    for (db, _, stream, _) in &mut replicas {
        block_on(db.tick()).expect("replica tick");
        while stream.try_next_event().is_some() {}
    }
    let connected = connecting.elapsed();

    // Idle: ticks with nobody writing.
    mark("idle");
    let idle_start = Instant::now();
    for _ in 0..50 {
        block_on(server.tick()).expect("idle tick");
    }
    let idle_tick = idle_start.elapsed() / 50;

    let before_down: usize = down_counters.iter().map(|c| c.borrow().bytes).sum();
    let before_frames: usize = down_counters.iter().map(|c| c.borrow().frames).sum();
    mark("write");
    let mut ticks = Vec::new();
    let mut done = 0;
    let mut next = 0;
    while done < writes {
        for _ in 0..batch {
            let (db, g, ..) = &replicas[next % replicas.len()];
            let base = g * project_tasks();
            let offset = (done * 7) % (project_tasks() - rows + 1);
            let stamp = done as i32;
            block_on(db.transaction(async |tx| {
                for n in base + offset..base + offset + rows {
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
            block_on(db.tick()).expect("send");
            next += 1;
            done += 1;
        }
        let start = Instant::now();
        block_on(server.tick()).expect("authority tick");
        ticks.push(start.elapsed());
        drain(&bare);
        for (db, _, stream, _) in &mut replicas {
            block_on(db.tick()).expect("replica tick");
            while stream.try_next_event().is_some() {}
        }
    }
    let down: usize = down_counters
        .iter()
        .map(|c| c.borrow().bytes)
        .sum::<usize>()
        - before_down;
    let frames: usize = down_counters
        .iter()
        .map(|c| c.borrow().frames)
        .sum::<usize>()
        - before_frames;
    mark("end");
    ticks.sort();
    let total: Duration = ticks.iter().sum();
    let per_write = total / writes as u32;
    let pct = |p: f64| ticks[((ticks.len() - 1) as f64 * p) as usize];
    let file = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0)
        + std::fs::metadata(path.with_extension("sqlite-wal"))
            .map(|m| m.len())
            .unwrap_or(0);
    println!(
        "{}",
        serde_json::json!({
            "store_rows": store,
            "store_mb": file as f64 / 1e6,
            "sessions": sessions,
            "group": group,
            "batch": batch,
            "rows_per_write": rows,
            "project_tasks": project_tasks(),
            "writes": writes,
            "seed_s": seeded.as_secs_f64(),
            "connect_s": connected.as_secs_f64(),
            "idle_tick_us": idle_tick.as_secs_f64() * 1e6,
            "authority_us_per_write": per_write.as_secs_f64() * 1e6,
            "tick_p50_us": pct(0.5).as_secs_f64() * 1e6,
            "tick_p99_us": pct(0.99).as_secs_f64() * 1e6,
            "ceiling_writes_per_s": 1.0 / per_write.as_secs_f64(),
            "down_bytes_per_write": down as f64 / writes as f64,
            "down_frames_per_write": frames as f64 / writes as f64,
        })
    );
}
