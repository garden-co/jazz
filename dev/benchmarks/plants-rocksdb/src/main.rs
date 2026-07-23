//! RocksDB-focused storage benchmark on the USDA PLANTS checklist.
//!
//! Every plant is assigned a stable UUID and ingested three ways, then the same
//! 500 random plants are fetched back by that id:
//!
//! 1. **`raw`** — RocksDB driven directly (no Jazz): one `put` per row, 500
//!    native point `get`s for the read.
//! 2. **`jazz`** — `jazz::db::Db<RocksDbStorage>`: batched transactions, then the
//!    500 ids fetched with one membership read (Jazz's local read ignores
//!    indexes and full-scans, so 500 sequential point queries are impractical —
//!    a small point-lookup probe reports that per-id cost separately).
//! 3. **`server`** — a `jazz::db::Db<RocksDbStorage>` **client** that syncs over a
//!    real localhost WebSocket into a RocksDB-backed **Jazz Server**
//!    (`jazz_server::LoopbackWebSocketServer`). Writes are timed through sync
//!    settlement; the 500-by-id read runs locally on the client after sync.
//!    NOTE: the server shell's sync ingestion is super-linear in rows already
//!    stored, so cap this topology at `--limit 25000` (~4.5 min, fully synced);
//!    the full 93k would take ~40 min. That per-batch cost is itself a result
//!    (the server ingest path, not the harness, is the bottleneck).
//!
//! An optional synthetic **EBS** write delay (`--ebs-delay-ms`) models a
//! network-attached volume by charging a fixed latency per durable commit batch.
//!
//! ```text
//! dev/benchmarks/plants-rocksdb/scripts/setup.sh                 # download the dataset
//! cargo run --release -p plants-rocksdb-bench -- --topology raw,jazz          # full 93k, seconds
//! cargo run --release -p plants-rocksdb-bench -- --topology server --limit 25000  # ~4.5 min
//! cargo run --release -p plants-rocksdb-bench -- --limit 5000 --ebs-delay-ms 2     # all three
//! ```

use std::collections::{BTreeMap, VecDeque};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use jazz::db::{Db, DbConfig, DbIdentity, WireTransportAdapter, block_on};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::{Durability, RocksDbStorage};
use jazz::ids::{AuthorId, NodeUuid};
use jazz::query::{Query, col, eq, in_list, lit};
use jazz::schema::{JazzSchema, TableSchema};
use jazz::wire::{TransportError, WireTransport};

use jazz_server::loopback_websocket::{LoopbackWebSocketServer, LoopbackWebSocketServerConfig};

use rocksdb::{DB as RawDb, DBCompressionType, Options as RawOptions, WriteBatch};
use tungstenite::Message;
use tungstenite::stream::MaybeTlsStream;
use uuid::Uuid;

const TABLE: &str = "plants";
// NB: not "id" — that name resolves to Jazz's implicit row-identity (Uuid)
// column, which would type-mismatch against our String uuid literals.
const ID_COL: &str = "plant_id";
const FIELDS: [&str; 5] = [
    "symbol",
    "synonym_symbol",
    "scientific_name",
    "common_name",
    "family",
];
const FIELD_SEP: u8 = 0x1f;

// ---------------------------------------------------------------------------
// Dataset
// ---------------------------------------------------------------------------

/// One record: a stable assigned UUID plus the five CSV fields.
struct Plant {
    id: String,
    fields: [String; 5],
}

impl Plant {
    /// The row as Jazz cells (`id` + the five columns) for `tx.insert`.
    fn cells(&self) -> BTreeMap<String, Value> {
        let mut cells = BTreeMap::new();
        cells.insert(ID_COL.to_owned(), Value::String(self.id.clone()));
        for (name, value) in FIELDS.iter().zip(self.fields.iter()) {
            cells.insert((*name).to_owned(), Value::String(value.clone()));
        }
        cells
    }

    /// The row encoded for the raw-RocksDB value: fields joined by `FIELD_SEP`.
    fn raw_value(&self) -> Vec<u8> {
        self.fields
            .iter()
            .map(String::as_bytes)
            .collect::<Vec<_>>()
            .join(&[FIELD_SEP][..])
    }
}

/// Parse one RFC-4180-ish line: comma-separated, double-quoted fields, `""` a
/// literal quote. The USDA file quotes every field, one record per line.
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    cur.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            } else {
                cur.push(c);
            }
        } else {
            match c {
                '"' => in_quotes = true,
                ',' => fields.push(std::mem::take(&mut cur)),
                _ => cur.push(c),
            }
        }
    }
    fields.push(cur);
    fields
}

/// Load the dataset and assign each row a stable UUID derived from its index so
/// every topology and every run sees the exact same id set.
fn load_dataset(path: &Path, limit: Option<usize>) -> Vec<Plant> {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| {
        panic!(
            "read dataset {}: {e}\nrun dev/benchmarks/plants-rocksdb/scripts/setup.sh first",
            path.display()
        )
    });
    let mut plants = Vec::new();
    for line in text.lines().skip(1) {
        if line.is_empty() {
            continue;
        }
        let mut cols = parse_csv_line(line);
        cols.resize(5, String::new());
        let index = plants.len() as u64;
        plants.push(Plant {
            id: Uuid::from_u128(splitmix64(index.wrapping_add(1)) as u128).to_string(),
            fields: [
                std::mem::take(&mut cols[0]),
                std::mem::take(&mut cols[1]),
                std::mem::take(&mut cols[2]),
                std::mem::take(&mut cols[3]),
                std::mem::take(&mut cols[4]),
            ],
        });
        if let Some(limit) = limit
            && plants.len() >= limit
        {
            break;
        }
    }
    plants
}

/// Pick `count` distinct plant ids at random (seeded, reproducible).
fn sample_ids(plants: &[Plant], count: usize, seed: u64) -> Vec<String> {
    let count = count.min(plants.len());
    let mut chosen = std::collections::BTreeSet::new();
    let mut state = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    while chosen.len() < count {
        state = splitmix64(state);
        chosen.insert((state as usize) % plants.len());
    }
    chosen.into_iter().map(|i| plants[i].id.clone()).collect()
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

// ---------------------------------------------------------------------------
// Jazz plumbing
// ---------------------------------------------------------------------------

fn schema() -> JazzSchema {
    let mut columns = vec![ColumnSchema::new(ID_COL, ColumnType::String)];
    columns.extend(
        FIELDS
            .iter()
            .map(|n| ColumnSchema::new(*n, ColumnType::String)),
    );
    JazzSchema::new([TableSchema::new(TABLE, columns)])
}

fn open_rocks_db(schema: &JazzSchema, path: &Path) -> Db<RocksDbStorage> {
    open_rocks_db_as(schema, path, [1u8; 16], AuthorId::SYSTEM)
}

fn open_rocks_db_as(
    schema: &JazzSchema,
    path: &Path,
    node: [u8; 16],
    author: AuthorId,
) -> Db<RocksDbStorage> {
    let cfs: Vec<String> = schema.column_families();
    let refs: Vec<&str> = cfs.iter().map(String::as_str).collect();
    let storage = RocksDbStorage::open_with_durability(path, &refs, Durability::WalNoSync)
        .expect("open rocksdb storage");
    block_on(Db::open(DbConfig::new(
        schema.clone(),
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes(node),
            author,
        },
    )))
    .expect("open db")
}

/// Fetch the 500 sampled ids with a single membership read, then probe the
/// per-id point-lookup cost on a small sub-sample.
fn jazz_read_by_id(db: &Db<RocksDbStorage>, ids: &[String]) -> (Duration, usize, Duration) {
    let query = Query::from(TABLE).filter(in_list(
        col(ID_COL),
        ids.iter().map(|id| lit(Value::String(id.clone()))),
    ));
    let prepared = db.prepare_query(&query).expect("prepare in_list query");
    let t = Instant::now();
    let rows = db.read(&prepared).expect("read in_list query");
    let bulk = t.elapsed();

    // Per-id point-lookup probe: Jazz's local read full-scans per query, so this
    // is the true "by id" cost. Averaged over a small sub-sample to stay fast.
    let probe_n = ids.len().min(16);
    let t = Instant::now();
    for id in ids.iter().take(probe_n) {
        let q = Query::from(TABLE).filter(eq(col(ID_COL), lit(Value::String(id.clone()))));
        let prepared = db.prepare_query(&q).expect("prepare point query");
        let _ = db.read(&prepared).expect("read point query");
    }
    let per_lookup = t.elapsed() / probe_n.max(1) as u32;
    (bulk, rows.len(), per_lookup)
}

// ---------------------------------------------------------------------------
// Synthetic EBS write delay
// ---------------------------------------------------------------------------

/// Fixed latency charged once per durable commit batch, modelling a
/// network-attached volume. Applied identically to every topology's write loop.
#[derive(Clone, Copy)]
struct EbsDelay {
    per_batch: Duration,
}

impl EbsDelay {
    fn new(ms: u64) -> Self {
        Self {
            per_batch: Duration::from_millis(ms),
        }
    }
    fn charge(self) {
        if !self.per_batch.is_zero() {
            std::thread::sleep(self.per_batch);
        }
    }
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

struct Metrics {
    label: String,
    rows: usize,
    found: usize,
    read_kind: &'static str,
    write: Duration,
    flush: Duration,
    read_500: Duration,
    per_lookup: Option<Duration>,
    /// For the server topology: rows the server durably persisted (should equal
    /// `rows`). `None` for the local-only topologies.
    synced_to_server: Option<usize>,
}

impl Metrics {
    fn print(&self) {
        let rows_per_s = self.rows as f64 / self.write.as_secs_f64().max(1e-9);
        println!("═══ {} ═══", self.label);
        println!(
            "  write all           {:>8.3} s   ({:.0} rows/s)",
            self.write.as_secs_f64(),
            rows_per_s
        );
        println!("  flush / settle      {:>8.3} s", self.flush.as_secs_f64());
        if let Some(synced) = self.synced_to_server {
            println!("  synced to server    {:>8} / {} rows", synced, self.rows);
        }
        println!(
            "  get 500 by id       {:>8.3} ms  ({}, {}/{} found)",
            self.read_500.as_secs_f64() * 1e3,
            self.read_kind,
            self.found,
            500.min(self.rows),
        );
        if let Some(per) = self.per_lookup {
            let extrapolated = per.as_secs_f64() * 500.0;
            println!(
                "    per-id point lookup {:>6.3} ms  (500 sequential ≈ {:.1} s)",
                per.as_secs_f64() * 1e3,
                extrapolated
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Topology 1: raw RocksDB
// ---------------------------------------------------------------------------

fn run_raw(plants: &[Plant], ids: &[String], batch: usize, ebs: EbsDelay) -> Metrics {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut opts = RawOptions::default();
    opts.create_if_missing(true);
    opts.set_compression_type(DBCompressionType::Lz4);
    let db = RawDb::open(&opts, dir.path()).expect("open raw rocksdb");

    let t = Instant::now();
    for chunk in plants.chunks(batch) {
        let mut wb = WriteBatch::default();
        for plant in chunk {
            wb.put(plant.id.as_bytes(), plant.raw_value());
        }
        db.write(&wb).expect("write batch");
        ebs.charge();
    }
    let write = t.elapsed();

    let t = Instant::now();
    db.flush().expect("flush");
    let flush = t.elapsed();

    // 500 native point gets.
    let t = Instant::now();
    let mut found = 0;
    for id in ids {
        if db.get(id.as_bytes()).expect("get").is_some() {
            found += 1;
        }
    }
    let read_500 = t.elapsed();

    Metrics {
        label: "raw RocksDB".to_owned(),
        rows: plants.len(),
        found,
        read_kind: "500 point gets",
        write,
        flush,
        read_500,
        per_lookup: None,
        synced_to_server: None,
    }
}

// ---------------------------------------------------------------------------
// Topology 2: Jazz + RocksDB
// ---------------------------------------------------------------------------

fn run_jazz(plants: &[Plant], ids: &[String], batch: usize, ebs: EbsDelay) -> Metrics {
    let schema = schema();
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();

    let db = open_rocks_db(&schema, &path);
    let t = Instant::now();
    for chunk in plants.chunks(batch) {
        db.transaction(|tx| {
            for plant in chunk {
                tx.insert(TABLE, plant.cells())?;
            }
            Ok(())
        })
        .expect("commit batch");
        ebs.charge();
    }
    let write = t.elapsed();

    let t = Instant::now();
    db.close().expect("close db");
    let flush = t.elapsed();
    drop(db);

    // Reopen cold and read the 500 ids from cold storage.
    let cold = open_rocks_db(&schema, &path);
    let (read_500, found, per_lookup) = jazz_read_by_id(&cold, ids);
    cold.close().expect("close cold db");

    Metrics {
        label: "Jazz + RocksDB".to_owned(),
        rows: plants.len(),
        found,
        read_kind: "cold in_list",
        write,
        flush,
        read_500,
        per_lookup: Some(per_lookup),
        synced_to_server: None,
    }
}

// ---------------------------------------------------------------------------
// Topology 3: Jazz client -> Jazz Server (both RocksDB)
// ---------------------------------------------------------------------------

/// Blocking-WebSocket `WireTransport` bridging a Jazz client to the loopback
/// server. Reads are non-blocking so it fits the synchronous `tick()` model.
struct WsClientTransport {
    socket: tungstenite::WebSocket<MaybeTlsStream<TcpStream>>,
    inbox: VecDeque<Vec<u8>>,
    sent: Arc<AtomicU64>,
    recv: Arc<AtomicU64>,
    /// Set once the server closes the connection or a read errors, so a blocked
    /// write bails with an error instead of spinning forever.
    closed: bool,
}

/// Max wall-clock a single frame's flush may spend under backpressure before we
/// treat the connection as dead. Very generous, since late batches ingest slowly
/// (the server's history consolidation is super-linear in rows already stored).
const FLUSH_DEADLINE: Duration = Duration::from_secs(600);

fn is_would_block(error: &tungstenite::Error) -> bool {
    matches!(error, tungstenite::Error::Io(e) if e.kind() == std::io::ErrorKind::WouldBlock)
}

impl WsClientTransport {
    /// Drain every message currently available on the (non-blocking) socket into
    /// the inbox. Never blocks. Also relieves the server's send buffer, which is
    /// what lets a blocked write make progress. Flags the connection closed on a
    /// non-would-block error.
    fn pump_reads(&mut self) {
        loop {
            match self.socket.read() {
                Ok(Message::Binary(bytes)) => {
                    if let Ok(frames) = postcard::from_bytes::<Vec<Vec<u8>>>(&bytes) {
                        self.recv.fetch_add(frames.len() as u64, Ordering::Relaxed);
                        self.inbox.extend(frames);
                    }
                }
                Ok(Message::Ping(payload)) => {
                    let _ = self.socket.write(Message::Pong(payload));
                }
                Ok(_) => {}
                Err(e) if is_would_block(&e) => break,
                Err(_) => {
                    self.closed = true;
                    break;
                }
            }
        }
    }
}

impl WireTransport for WsClientTransport {
    // Non-blocking send that never drops: on write-buffer/would-block pressure it
    // drains inbound frames (relieving the server so it keeps reading us) and
    // retries until the frame is fully flushed. A single-threaded blocking send
    // would deadlock once both directions' buffers fill mid-burst. Bails with an
    // error if the connection closes or a flush stalls past FLUSH_DEADLINE.
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        if self.closed {
            return Err(TransportError::Failed("ws connection closed".to_owned()));
        }
        let batch = postcard::to_allocvec(&vec![frame])
            .map_err(|e| TransportError::Failed(format!("encode frame batch: {e}")))?;
        let deadline = Instant::now();
        let mut pending = Some(Message::Binary(batch.into()));
        while let Some(message) = pending.take() {
            match self.socket.write(message) {
                Ok(()) => {}
                Err(tungstenite::Error::WriteBufferFull(returned)) => {
                    self.pump_reads();
                    pending = Some(*returned);
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) if is_would_block(&e) => {} // queued; fall through to flush
                Err(e) => return Err(TransportError::Failed(format!("ws write: {e}"))),
            }
            if self.closed || deadline.elapsed() > FLUSH_DEADLINE {
                return Err(TransportError::Failed("ws write stalled/closed".to_owned()));
            }
        }
        loop {
            match self.socket.flush() {
                Ok(()) => break,
                Err(tungstenite::Error::WriteBufferFull(_)) => self.pump_reads(),
                Err(e) if is_would_block(&e) => {
                    self.pump_reads();
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => return Err(TransportError::Failed(format!("ws flush: {e}"))),
            }
            if self.closed || deadline.elapsed() > FLUSH_DEADLINE {
                return Err(TransportError::Failed("ws flush stalled/closed".to_owned()));
            }
        }
        self.sent.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        if let Some(frame) = self.inbox.pop_front() {
            return Some(frame);
        }
        self.pump_reads();
        self.inbox.pop_front()
    }
}

fn run_server(plants: &[Plant], ids: &[String], batch: usize, ebs: EbsDelay) -> Metrics {
    if plants.len() > 25_000 {
        eprintln!(
            "  note: the server topology's sync ingestion is super-linear; {} rows will take \
             far longer than the ~4.5 min for 25k (full 93k ≈ 40 min). Consider --limit 25000.",
            plants.len()
        );
    }
    let schema = schema();

    // --- Start a RocksDB-backed Jazz Server on an ephemeral localhost port. ---
    let server_dir = tempfile::tempdir().expect("server tempdir");
    let config = LoopbackWebSocketServerConfig::persistent_data_dir(
        schema.clone(),
        DbIdentity {
            node: NodeUuid::from_bytes([9u8; 16]),
            author: AuthorId::SYSTEM,
        },
        server_dir.path(),
    );
    let server = LoopbackWebSocketServer::start_with_config(config).expect("start jazz server");
    let addr = server.local_addr();

    // --- Client Db (RocksDB) connected upstream to the server via WebSocket.
    // The client authors commits under the *same* id the server admits for the
    // anonymous session, so its uploads are trusted (and not dropped). ---
    let client_author = jazz_server::auth_admission::author_id_from_subject("anonymous");
    let client_dir = tempfile::tempdir().expect("client tempdir");
    let client = open_rocks_db_as(&schema, client_dir.path(), [2u8; 16], client_author);

    let (socket, _resp) = tungstenite::connect(format!("ws://{addr}/sync")).expect("connect ws");
    if let MaybeTlsStream::Plain(stream) = socket.get_ref() {
        stream
            .set_nonblocking(true)
            .expect("set client socket non-blocking");
    }
    let sent = Arc::new(AtomicU64::new(0));
    let recv = Arc::new(AtomicU64::new(0));
    let transport = WsClientTransport {
        socket,
        inbox: VecDeque::new(),
        sent: Arc::clone(&sent),
        recv: Arc::clone(&recv),
        closed: false,
    };

    // Connect upstream and let the catalogue handshake settle before writing, so
    // schema negotiation completes against the empty server first.
    let _upstream = client.connect_upstream(Box::new(WireTransportAdapter::current(transport)));
    for _ in 0..50 {
        client.tick().expect("handshake tick");
        std::thread::sleep(Duration::from_millis(2));
    }

    // --- Write and ship incrementally: commit a batch, then tick to stream it to
    // the server before the next batch. This paces uploads to the server's ingest
    // speed (the reliable transport applies backpressure) instead of blasting the
    // whole dataset in one burst. Timed through final sync settlement so the
    // server durably holds every row. ---
    let t = Instant::now();
    let total_batches = plants.len().div_ceil(batch);
    let progress = std::env::var("JZ_PROGRESS").is_ok();
    for (i, chunk) in plants.chunks(batch).enumerate() {
        client
            .transaction(|tx| {
                for plant in chunk {
                    tx.insert(TABLE, plant.cells())?;
                }
                Ok(())
            })
            .expect("commit batch");
        ebs.charge();
        client.tick().expect("ship batch");
        if progress && (i + 1).is_multiple_of(5) {
            eprintln!(
                "    write: batch {}/{} shipped  sent={} recv={}  elapsed={:.0}s",
                i + 1,
                total_batches,
                sent.load(Ordering::Relaxed),
                recv.load(Ordering::Relaxed),
                t.elapsed().as_secs_f64()
            );
        }
    }
    // Drive remaining sync to quiescence.
    drain_sync(&client, &sent, &recv);
    let write = t.elapsed();

    // --- Read the 500 ids locally on the client after sync (warm). ---
    let (read_500, found, per_lookup) = jazz_read_by_id(&client, ids);

    client.close().expect("close client db");
    server.shutdown();

    // Self-check: reopen the server's RocksDB directory and confirm it durably
    // holds every uploaded row (proves the write path really reached the server).
    let verify = open_rocks_db(&schema, server_dir.path());
    let synced = verify
        .read(
            &verify
                .prepare_query(&Query::from(TABLE))
                .expect("prepare scan"),
        )
        .expect("scan server")
        .len();
    verify.close().expect("close verify db");

    Metrics {
        label: "Jazz client -> Jazz Server (RocksDB)".to_owned(),
        rows: plants.len(),
        found,
        read_kind: "warm in_list",
        write,
        flush: Duration::ZERO,
        read_500,
        per_lookup: Some(per_lookup),
        synced_to_server: Some(synced),
    }
}

/// Tick the client until sync is quiescent: no frame sent or received for
/// `IDLE_TICKS` consecutive ticks (or a hard timeout).
fn drain_sync(client: &Db<RocksDbStorage>, sent: &Arc<AtomicU64>, recv: &Arc<AtomicU64>) {
    // Quiescence: no frame moves for `IDLE_TICKS` ticks. The server-shell sync
    // path is slow (~seconds per 1000-row batch) but keeps acking, so frames
    // move well within this window; the guard below only trips on a real stall.
    const IDLE_TICKS: u32 = 150;
    const STALL_TIMEOUT: Duration = Duration::from_secs(120);
    let mut last = (sent.load(Ordering::Relaxed), recv.load(Ordering::Relaxed));
    let mut idle = 0u32;
    let start = Instant::now();
    let mut last_progress = Instant::now();
    let mut last_log = Instant::now();
    loop {
        client.tick().expect("drain tick");
        std::thread::sleep(Duration::from_millis(10));
        let now = (sent.load(Ordering::Relaxed), recv.load(Ordering::Relaxed));
        if now == last {
            idle += 1;
        } else {
            idle = 0;
            last_progress = Instant::now();
        }
        last = now;
        if std::env::var("JZ_PROGRESS").is_ok() && last_log.elapsed() > Duration::from_secs(1) {
            eprintln!(
                "    drain: sent={} recv={} idle={idle} elapsed={:.0}s",
                now.0,
                now.1,
                start.elapsed().as_secs_f64()
            );
            last_log = Instant::now();
        }
        if idle >= IDLE_TICKS {
            break;
        }
        if last_progress.elapsed() > STALL_TIMEOUT {
            eprintln!("  warning: sync stalled (no progress for {STALL_TIMEOUT:?}); giving up");
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

fn main() {
    let mut limit: Option<usize> = None;
    let mut batch = 1000usize;
    let mut ebs_ms = 0u64;
    let mut sample = 500usize;
    let mut seed = 0x5eedu64;
    let mut topologies = vec!["raw".to_owned(), "jazz".to_owned(), "server".to_owned()];
    let default_data = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/data/plantlst.txt"));
    let mut data = default_data;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--limit" => limit = Some(parse_next(&mut args, "--limit")),
            "--batch" => batch = parse_next(&mut args, "--batch"),
            "--ebs-delay-ms" => ebs_ms = parse_next(&mut args, "--ebs-delay-ms"),
            "--sample" => sample = parse_next(&mut args, "--sample"),
            "--seed" => seed = parse_next(&mut args, "--seed"),
            "--data" => {
                data = PathBuf::from(args.next().expect("--data needs a path"));
            }
            "--topology" => {
                let value: String = args.next().expect("--topology needs a value");
                topologies = value.split(',').map(|s| s.trim().to_owned()).collect();
            }
            "--help" | "-h" => {
                print_help();
                return;
            }
            other => panic!("unknown argument: {other}"),
        }
    }

    let plants = load_dataset(&data, limit);
    assert!(!plants.is_empty(), "dataset is empty: {}", data.display());
    let ids = sample_ids(&plants, sample, seed);
    let ebs = EbsDelay::new(ebs_ms);

    println!(
        "dataset {} rows | batch {} | sample {} | ebs-delay {} ms/batch\n",
        plants.len(),
        batch,
        ids.len(),
        ebs_ms
    );

    for topology in &topologies {
        let metrics = match topology.as_str() {
            "raw" => run_raw(&plants, &ids, batch, ebs),
            "jazz" => run_jazz(&plants, &ids, batch, ebs),
            "server" => run_server(&plants, &ids, batch, ebs),
            other => panic!("unknown topology: {other} (expected raw|jazz|server)"),
        };
        metrics.print();
        println!();
    }
}

fn parse_next<T: std::str::FromStr>(args: &mut impl Iterator<Item = String>, flag: &str) -> T
where
    T::Err: std::fmt::Display,
{
    let raw = args
        .next()
        .unwrap_or_else(|| panic!("{flag} needs a value"));
    raw.parse()
        .unwrap_or_else(|e| panic!("{flag}: invalid value {raw:?}: {e}"))
}

fn print_help() {
    println!(
        "plants-rocksdb-bench — RocksDB ingestion + 500-by-id read on the USDA plants dataset\n\n\
         Options:\n\
         \x20 --limit <n>          only ingest the first n plants\n\
         \x20 --batch <n>          rows per commit batch (default 1000)\n\
         \x20 --ebs-delay-ms <n>   synthetic per-batch durable-write latency (default 0)\n\
         \x20 --sample <n>         number of random ids to fetch back (default 500)\n\
         \x20 --seed <n>           RNG seed for the id sample (default 0x5eed)\n\
         \x20 --topology <list>    comma-separated: raw,jazz,server (default all)\n\
         \x20 --data <path>        dataset path (default <crate>/data/plantlst.txt)\n"
    );
}
