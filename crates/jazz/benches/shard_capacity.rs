//! Geo-shard spike E1/E2: how much commit throughput one authority Db
//! sustains, and how N authorities on one box scale, with and without
//! cross-shard work.
//!
//! Each shard is one thread owning a durable server Db (WAL, synchronous
//! FULL, like jazz-server) plus in-memory client Dbs connected over byte
//! frames. Only the server's ticks are timed: client inserts, uploads and
//! applies would run on user devices. Capacity = commits / server tick time.
//!
//! A cross-shard commit (fraction `CROSS`) posts a message on its home shard
//! and a mention on a peer shard, plus the tx record and its resolution the
//! sharding layer would write (intent model: 2 writes on each side).
//!
//! `SHARDS=1,2,4 CLIENTS=1000 BATCH=1,16 CROSS=0,0.05,0.2 cargo bench --bench shard_capacity`

#[path = "../tests/common/mod.rs"]
#[allow(dead_code)]
mod common;

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;
use std::sync::mpsc;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use jazz::db::{Db, DbConfig, DbIdentity, FileOptions, ReadOpts, WireTransportAdapter, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::wire::{TransportError, WireTransport};

use common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};

type Queue = Rc<RefCell<VecDeque<Vec<u8>>>>;

struct Pipe {
    outbound: Queue,
    inbound: Queue,
}

impl WireTransport for Pipe {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.outbound.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.inbound.borrow_mut().pop_front()
    }
}

/// Connect `client` to `server` as `author`; returns the server→client queue.
fn connect(server: &Db, client: &Db, author: AuthorSubject) -> Queue {
    let up: Queue = Rc::default();
    let down: Queue = Rc::default();
    let client_side = Pipe {
        outbound: up.clone(),
        inbound: down.clone(),
    };
    let server_side = Pipe {
        outbound: down.clone(),
        inbound: up,
    };
    let _ = block_on(client.connect_upstream(Box::new(WireTransportAdapter::current(client_side))));
    let _ = server.accept_subscriber(Box::new(WireTransportAdapter::current(server_side)), author);
    down
}

fn id(shard: usize, kind: u8, n: usize) -> RowUuid {
    let mut bytes = [0u8; 16];
    bytes[0] = kind;
    bytes[1] = shard as u8;
    bytes[8..].copy_from_slice(&(n as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn user(shard: usize, n: usize) -> AuthorSubject {
    let mut bytes = [0u8; 16];
    bytes[0] = 0x55;
    bytes[1] = shard as u8;
    bytes[8..].copy_from_slice(&(n as u64).to_be_bytes());
    AuthorSubject::for_test_bytes(bytes)
}

fn subject(shard: usize, n: usize) -> String {
    user(shard, n).principal_parts().1
}

fn text(value: impl Into<String>) -> Value {
    Value::String(value.into())
}

fn cells(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

fn schema() -> JazzSchema {
    let me = ["user", "identity", "subject"];
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("rooms")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("memberships")
                    .fk_column("room", "rooms")
                    .column("member", ColumnType::Text)
                    .index_only(["member"])
                    .policies(read_and_allow_all_writes(session_eq("member", &me))),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("room", "rooms")
                    .column("author", ColumnType::Text)
                    .column("body", ColumnType::Text)
                    .column("sent_at", ColumnType::BigInt)
                    .composite_index(["room", "sent_at"])
                    .policies(read_and_allow_all_writes(exists(
                        "memberships",
                        vec![outer_eq("room", "room"), session_eq("member", &me)],
                    ))),
            )
            // Written by the sharding layer for cross-shard commits.
            .table(
                TableSchemaBuilder::new("mentions")
                    .column("member", ColumnType::Text)
                    .column("body", ColumnType::Text)
                    .column("tx", ColumnType::Text)
                    .index_only(["member"])
                    .policies(read_and_allow_all_writes(session_eq("member", &me))),
            )
            .table(
                TableSchemaBuilder::new("tx_records")
                    .column("state", ColumnType::Text)
                    .column("peer", ColumnType::BigInt)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

#[derive(Clone)]
struct Config {
    shards: usize,
    /// Clients (= users) per shard.
    clients: usize,
    rooms_per_user: usize,
    members_per_room: usize,
    batch: usize,
    rounds: usize,
    cross: f64,
    fsync: bool,
}

struct ShardReport {
    commits: usize,
    server_time: Duration,
    round_times: Vec<Duration>,
    frames_out: usize,
    bytes_out: usize,
    cross_sent: usize,
}

/// A cross-shard request: write a mention for `member_index` on the peer,
/// then (second message) resolve it.
enum Cross {
    Intent { tx: String, member: usize },
    Resolve { tx: String },
}

fn run_shard(
    shard: usize,
    cfg: &Config,
    dir: &std::path::Path,
    inbox: mpsc::Receiver<Cross>,
    peers: Vec<mpsc::Sender<Cross>>,
    start: Arc<Barrier>,
) -> ShardReport {
    let schema = schema();
    let config = |node: [u8; 16], author: AuthorSubject| {
        DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes(node),
                author,
            },
        )
    };
    let mut server_node = [0x11u8; 16];
    server_node[1] = shard as u8;
    let server = block_on(Db::open_at(
        config(server_node, AuthorSubject::SYSTEM),
        &dir.join(format!("shard-{shard}.db")),
        FileOptions {
            synchronous_full: cfg.fsync,
            ..FileOptions::default()
        },
    ))
    .unwrap();
    server.set_authority();

    let users = cfg.clients;
    let rooms = (users * cfg.rooms_per_user / cfg.members_per_room).max(1);
    let room_of = |u: usize, k: usize| (u * cfg.rooms_per_user + k) / cfg.members_per_room % rooms;
    let seed = |table: &str, row: RowUuid, values: BTreeMap<String, Value>| {
        server
            .seed_settled_mergeable_for_bootstrap(table, row, AuthorSubject::SYSTEM, values)
            .unwrap();
    };
    for r in 0..rooms {
        seed(
            "rooms",
            id(shard, 1, r),
            cells(&[("name", text(format!("room {r}")))]),
        );
    }
    let mut membership = 0;
    for u in 0..users {
        for k in 0..cfg.rooms_per_user {
            seed(
                "memberships",
                id(shard, 2, membership),
                cells(&[
                    ("room", Value::Uuid(id(shard, 1, room_of(u, k)).0)),
                    ("member", text(subject(shard, u))),
                ]),
            );
            membership += 1;
        }
    }
    let mut sent = 0;
    for r in 0..rooms {
        for _ in 0..20 {
            seed(
                "messages",
                id(shard, 3, sent),
                cells(&[
                    ("room", Value::Uuid(id(shard, 1, r).0)),
                    ("author", text(subject(shard, 0))),
                    ("body", text(format!("message {sent}"))),
                    ("sent_at", Value::I64(sent as i64)),
                ]),
            );
            sent += 1;
        }
    }
    block_on(server.tick()).unwrap();

    // Clients subscribe like an app: the first room's latest page, their
    // memberships and their mentions.
    let mut clients = Vec::new();
    let mut downs = Vec::new();
    let mut streams = Vec::new();
    for u in 0..users {
        let mut node = [0x77u8; 16];
        node[1] = shard as u8;
        node[8..].copy_from_slice(&(u as u64).to_be_bytes());
        let client = block_on(Db::open(config(node, user(shard, u)))).unwrap();
        downs.push(connect(&server, &client, user(shard, u)));
        let room = id(shard, 1, room_of(u, 0));
        for query in [
            Query::from("messages")
                .filter(eq(col("room"), lit(Value::Uuid(room.0))))
                .order_by("sent_at", OrderDirection::Desc)
                .limit(30),
            Query::from("memberships"),
            Query::from("mentions"),
        ] {
            let prepared = client.prepare_query(&query).unwrap();
            streams.push(block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap());
        }
        clients.push(client);
    }
    // The sharding layer's own client on this shard (writes mentions and tx
    // records for cross-shard commits).
    let mut router_node = [0x99u8; 16];
    router_node[1] = shard as u8;
    let router = block_on(Db::open(config(router_node, AuthorSubject::SYSTEM))).unwrap();
    let router_down = connect(&server, &router, AuthorSubject::SYSTEM);

    let deliver = |clients: &[Db], downs: &[Queue]| {
        for (client, down) in clients.iter().zip(downs) {
            if !down.borrow().is_empty() {
                block_on(client.tick()).unwrap();
            }
        }
        router_down.borrow_mut().clear();
    };
    for _ in 0..8 {
        for client in &clients {
            block_on(client.tick()).unwrap();
        }
        block_on(server.tick()).unwrap();
        deliver(&clients, &downs);
    }
    for stream in streams.iter_mut() {
        while stream.try_next_event().is_some() {}
    }

    start.wait();
    let mut rng = 0x9e3779b97f4a7c15u64 ^ shard as u64;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let mut report = ShardReport {
        commits: 0,
        server_time: Duration::ZERO,
        round_times: Vec::new(),
        frames_out: 0,
        bytes_out: 0,
        cross_sent: 0,
    };
    let mut posted = 0usize;
    let mut tx_seq = 0usize;
    for _round in 0..cfg.rounds {
        // Writers: `batch` random users each post into a room of theirs.
        let mut writers = Vec::new();
        for _ in 0..cfg.batch {
            let u = next() as usize % users;
            let r = room_of(u, next() as usize % cfg.rooms_per_user);
            block_on(clients[u].insert(
                "messages",
                cells(&[
                    ("room", Value::Uuid(id(shard, 1, r).0)),
                    ("author", text(subject(shard, u))),
                    ("body", text("new message body of moderate length")),
                    ("sent_at", Value::I64((1_000_000 + posted) as i64)),
                ]),
                Default::default(),
            ))
            .unwrap();
            posted += 1;
            report.commits += 1;
            writers.push(u);
            if cfg.shards > 1 && (next() % 1_000_000) as f64 / 1e6 < cfg.cross {
                let peer = (shard + 1 + next() as usize % (cfg.shards - 1)) % cfg.shards;
                let tx = format!("{shard}-{tx_seq}");
                tx_seq += 1;
                block_on(router.insert(
                    "tx_records",
                    cells(&[
                        ("state", text("pending")),
                        ("peer", Value::I64(peer as i64)),
                    ]),
                    Default::default(),
                ))
                .unwrap();
                let target = if peer > shard { peer - 1 } else { peer };
                peers[target]
                    .send(Cross::Intent {
                        tx: tx.clone(),
                        member: next() as usize % users,
                    })
                    .unwrap();
                peers[target].send(Cross::Resolve { tx }).unwrap();
                report.cross_sent += 1;
            }
        }
        // Cross-shard work other shards sent here.
        while let Ok(request) = inbox.try_recv() {
            match request {
                Cross::Intent { tx, member } => {
                    block_on(router.insert(
                        "mentions",
                        cells(&[
                            ("member", text(subject(shard, member))),
                            ("body", text("you were mentioned")),
                            ("tx", text(tx)),
                        ]),
                        Default::default(),
                    ))
                    .unwrap();
                }
                Cross::Resolve { tx } => {
                    // The resolution: one more row on this side.
                    block_on(router.insert(
                        "tx_records",
                        cells(&[
                            ("state", text(format!("committed {tx}"))),
                            ("peer", Value::I64(-1)),
                        ]),
                        Default::default(),
                    ))
                    .unwrap();
                }
            }
        }
        writers.sort_unstable();
        writers.dedup();
        for &u in &writers {
            block_on(clients[u].tick()).unwrap();
        }
        block_on(router.tick()).unwrap();
        perf_ctl(b"enable\n");
        let t = Instant::now();
        block_on(server.tick()).unwrap();
        let elapsed = t.elapsed();
        perf_ctl(b"disable\n");
        report.server_time += elapsed;
        report.round_times.push(elapsed);
        for down in &downs {
            let d = down.borrow();
            report.frames_out += d.len();
            report.bytes_out += d.iter().map(Vec::len).sum::<usize>();
        }
        deliver(&clients, &downs);
        for &u in &writers {
            block_on(clients[u].tick()).unwrap();
        }
        for stream in streams.iter_mut() {
            while stream.try_next_event().is_some() {}
        }
    }
    // Settle anything still arriving so peers are not left blocked.
    start.wait();
    while inbox.try_recv().is_ok() {}
    report
}

/// With `PERF_CTL=<fifo>` (perf record --control=fifo:<fifo> -D -1), only
/// the server ticks are profiled.
fn perf_ctl(command: &[u8]) {
    use std::io::Write;
    if let Ok(path) = std::env::var("PERF_CTL") {
        let mut fifo = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        fifo.write_all(command).unwrap();
    }
}

fn run(cfg: &Config) {
    let dir = tempfile::tempdir().unwrap();
    let (senders, receivers): (Vec<_>, Vec<_>) =
        (0..cfg.shards).map(|_| mpsc::channel::<Cross>()).unzip();
    let start = Arc::new(Barrier::new(cfg.shards));
    let mut handles = Vec::new();
    for (shard, inbox) in receivers.into_iter().enumerate() {
        let peers: Vec<_> = senders
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != shard)
            .map(|(_, s)| s.clone())
            .collect();
        let cfg = cfg.clone();
        let start = start.clone();
        let path = dir.path().to_owned();
        handles.push(std::thread::spawn(move || {
            run_shard(shard, &cfg, &path, inbox, peers, start)
        }));
    }
    drop(senders);
    let reports: Vec<ShardReport> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let commits: usize = reports.iter().map(|r| r.commits).sum();
    let capacity: f64 = reports
        .iter()
        .map(|r| r.commits as f64 / r.server_time.as_secs_f64())
        .sum();
    let mut rounds: Vec<Duration> = reports
        .iter()
        .flat_map(|r| r.round_times.iter().copied())
        .collect();
    rounds.sort();
    let pct = |p: f64| rounds[((rounds.len() - 1) as f64 * p) as usize];
    let frames: usize = reports.iter().map(|r| r.frames_out).sum();
    let bytes: usize = reports.iter().map(|r| r.bytes_out).sum();
    let cross: usize = reports.iter().map(|r| r.cross_sent).sum();
    println!(
        "shards={} clients/shard={} members/room={} batch={} cross={:.2} fsync={} | capacity {:>8.0} commits/s ({:>7.0}/shard) | tick p50 {:>9.3?} p99 {:>9.3?} | out/commit {:.1} frames {:.0} B | cross sent {}",
        cfg.shards,
        cfg.clients,
        cfg.members_per_room,
        cfg.batch,
        cfg.cross,
        cfg.fsync,
        capacity,
        capacity / cfg.shards as f64,
        pct(0.5),
        pct(0.99),
        frames as f64 / commits as f64,
        bytes as f64 / commits as f64,
        cross,
    );
}

fn main() {
    // `MEMSTATUS=0`: turn off SQLite's allocation statistics, whose global
    // mutex every connection in the process takes on each malloc.
    if std::env::var("MEMSTATUS").as_deref() == Ok("0") {
        let rc =
            unsafe { rusqlite::ffi::sqlite3_config(rusqlite::ffi::SQLITE_CONFIG_MEMSTATUS, 0) };
        assert_eq!(rc, rusqlite::ffi::SQLITE_OK);
    }
    let list = |k: &str, d: &str| -> Vec<String> {
        std::env::var(k)
            .unwrap_or_else(|_| d.to_owned())
            .split(',')
            .map(str::to_owned)
            .collect()
    };
    let one = |k: &str, d: usize| {
        std::env::var(k)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(d)
    };
    for shards in list("SHARDS", "1") {
        for clients in list("CLIENTS", "500") {
            for members in list("MEMBERS", "10") {
                for batch in list("BATCH", "1") {
                    for cross in list("CROSS", "0") {
                        for fsync in list("FSYNC", "1") {
                            run(&Config {
                                shards: shards.parse().unwrap(),
                                clients: clients.parse().unwrap(),
                                rooms_per_user: one("ROOMS_PER_USER", 3),
                                members_per_room: members.parse().unwrap(),
                                batch: batch.parse().unwrap(),
                                rounds: one("ROUNDS", 300),
                                cross: cross.parse().unwrap(),
                                fsync: fsync == "1",
                            });
                        }
                    }
                }
            }
        }
    }
}
