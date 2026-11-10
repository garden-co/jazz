//! S5 durable streams: a writer appends token batches to a stream row's
//! text; tailers watch it live, and a resumer drops off and catches up.
//!
//! Gates (`INV-BENCH-8`): every tailer observes prefix-monotone content
//! (each version extends the last) and converges exactly on the written
//! stream; the resumer converges after reconnecting; the authority holds
//! the complete stream. Anchors: a plain log file and a SQLite table
//! holding the same bytes.

use std::collections::BTreeMap;
use std::io::Write;
use std::time::Instant;

use jazz::db::{Db, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz_sim::oracle::{self, Live};
use jazz_sim::support::{allow_all, cells, compile, insert, open, row, settle, text, user};
use jazz_sim::{
    Latencies, PauseMode, PeerProfile, Sim, SimulatorTransportCodec, bench_profile, emit_summary,
    env_u64,
};
use serde_json::{Value as JsonValue, json};

const STREAMS: &str = "streams";

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let profile = bench_profile();
    let config = Config {
        seed: env_u64("JAZZ_SEED", 0x5500_0001),
        profile: format!("{profile:?}").to_lowercase(),
        streams: profile.select(1, 2, 4),
        tokens_per_second: profile.select(20, 50, 100),
        batch_tokens: 4,
        run_seconds: profile.select(2, 5, 10),
        tailers: profile.select(2, 4, 8),
        link: PeerProfile::new(
            "regional",
            env_u64("JAZZ_LINK_ONE_WAY_MS", 20),
            env_u64("JAZZ_LINK_JITTER_MS", 5),
            0,
        ),
    };
    run(&config, true);
}

#[allow(dead_code)]
pub fn smoke() {
    let config = Config {
        seed: 0x5500_0001,
        profile: "s5-smoke".to_owned(),
        streams: 1,
        tokens_per_second: 8,
        batch_tokens: 2,
        run_seconds: 1,
        tailers: 1,
        link: PeerProfile::new("s5-smoke", 2, 1, 0),
    };
    run(&config, false);
    db_surface(&config);
}

#[derive(Clone)]
struct Config {
    seed: u64,
    profile: String,
    streams: usize,
    tokens_per_second: u64,
    batch_tokens: usize,
    run_seconds: u64,
    tailers: usize,
    link: PeerProfile,
}

impl Config {
    fn tokens(&self) -> usize {
        (self.tokens_per_second * self.run_seconds) as usize
    }

    fn appends_per_stream(&self) -> usize {
        self.tokens().div_ceil(self.batch_tokens)
    }
}

fn schema() -> JazzSchema {
    compile(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(STREAMS)
                    .column("name", ColumnType::Text)
                    .column("content", ColumnType::Text)
                    .policies(allow_all()),
            )
            .build(),
    )
}

fn stream(s: usize) -> RowUuid {
    row(0x50, s as u64)
}

/// The deterministic token text of stream `s`.
fn token(s: usize, n: usize) -> String {
    format!("s{s}t{n} ")
}

fn content(row: &oracle::Cells) -> String {
    match row.get("content") {
        Some(Value::String(text)) => text.clone(),
        other => panic!("content {other:?}"),
    }
}

/// A watcher of every stream, checking each version extends the last.
struct Tailer {
    name: String,
    live: Live,
    last: BTreeMap<RowUuid, String>,
    versions: u64,
}

impl Tailer {
    fn observe(&mut self) {
        self.live.poll();
        for (id, row) in &self.live.rows {
            let now = content(row);
            let before = self.last.get(id).cloned().unwrap_or_default();
            assert!(
                now.starts_with(&before),
                "{}: stream content must be prefix-monotone: {before:?} then {now:?}",
                self.name
            );
            if now != before {
                self.versions += 1;
            }
            self.last.insert(*id, now);
        }
    }
}

fn run(config: &Config, emit: bool) {
    let mut sim = Sim::new(config.seed, SimulatorTransportCodec::WireFrames);
    sim.add("core", open(schema(), 0, AuthorSubject::SYSTEM));
    sim.add("writer", open(schema(), 1, user(1)));
    sim.connect("writer", "core", user(1), config.link.clone());
    let names: Vec<String> = (0..config.tailers)
        .map(|t| format!("tailer-{t}"))
        .chain(["resumer".to_owned()])
        .collect();
    for (n, name) in names.iter().enumerate() {
        sim.add(
            name.clone(),
            open(schema(), 10 + n as u32, user(10 + n as u32)),
        );
        sim.connect(name, "core", user(10 + n as u32), config.link.clone());
    }
    for s in 0..config.streams {
        let created = insert(
            sim.db("writer"),
            STREAMS,
            stream(s),
            cells([("name", text(format!("stream-{s}"))), ("content", text(""))]),
        );
        settle(&sim, sim.db("writer"), &created);
    }
    let mut tailers: Vec<Tailer> = names
        .iter()
        .map(|name| Tailer {
            name: name.clone(),
            live: Live::subscribe(sim.db(name), &Query::from(STREAMS), ReadOpts::default()),
            last: BTreeMap::new(),
            versions: 0,
        })
        .collect();
    sim.settle();
    for tailer in &mut tailers {
        tailer.observe();
    }

    let mut expected: BTreeMap<RowUuid, String> = (0..config.streams)
        .map(|s| (stream(s), String::new()))
        .collect();
    let interval_ms = 1_000 * config.batch_tokens as u64 / config.tokens_per_second.max(1);
    let mut tail_latency = Latencies::default();
    let appends = config.appends_per_stream();
    let wall = Instant::now();
    let resume_at = appends / 3;
    let rejoin_at = 2 * appends / 3;
    let mut resume_bytes = 0;
    for batch in 0..appends {
        if batch == resume_at {
            sim.pause("resumer", PauseMode::Disconnect);
        }
        if batch == rejoin_at {
            let (_, before) = sim.link_stats("resumer");
            sim.resume("resumer");
            sim.settle();
            resume_bytes = sim.link_stats("resumer").1.bytes - before.bytes;
        }
        let sent_at = sim.now_ms();
        for s in 0..config.streams {
            let first = batch * config.batch_tokens;
            let last = (first + config.batch_tokens).min(config.tokens());
            let piece: String = (first..last).map(|n| token(s, n)).collect();
            let handle = block_on(sim.db("writer").append_value(
                STREAMS,
                stream(s),
                "content",
                piece.clone().into_bytes(),
            ))
            .expect("append");
            expected
                .get_mut(&stream(s))
                .expect("stream")
                .push_str(&piece);
            settle(&sim, sim.db("writer"), &handle);
        }
        sim.settle();
        for tailer in &mut tailers {
            tailer.observe();
            if sim.is_connected(&tailer.name) {
                assert_eq!(tailer.last, expected, "{} is caught up", tailer.name);
            }
        }
        tail_latency.record(sim.now_ms() - sent_at);
        sim.advance(interval_ms);
    }
    sim.settle();
    let wall_us = wall.elapsed().as_micros() as u64;
    for tailer in &mut tailers {
        tailer.observe();
        assert_eq!(tailer.last, expected, "{} converged exactly", tailer.name);
    }
    let core: BTreeMap<RowUuid, String> =
        oracle::read(sim.db("core"), &Query::from(STREAMS), ReadOpts::default())
            .iter()
            .map(|(id, row)| (*id, content(row)))
            .collect();
    assert_eq!(
        core, expected,
        "the authority holds every stream completely"
    );
    assert!(resume_bytes > 0, "the resumer caught up over the wire");

    // A fresh client's full load of the same streams, for the resume ratio.
    sim.add("fresh", open(schema(), 99, user(99)));
    sim.connect("fresh", "core", user(99), config.link.clone());
    let mut fresh = Live::subscribe(sim.db("fresh"), &Query::from(STREAMS), ReadOpts::default());
    sim.settle();
    fresh.poll();
    let full_bytes = sim.link_stats("fresh").1.bytes;
    assert_eq!(
        fresh.rows.values().map(content).collect::<Vec<_>>(),
        expected.values().cloned().collect::<Vec<_>>()
    );

    if emit {
        let total_bytes: usize = expected.values().map(String::len).sum();
        let (log_bytes, log_us) = log_floor(config);
        let (sqlite_bytes, sqlite_us) = sqlite_baseline(config);
        let mut fields = sim.json_fields();
        fields.extend(
            json!({
                "phase": "summary",
                "streams": config.streams,
                "appends": appends * config.streams,
                "tokens": config.tokens() * config.streams,
                "content_bytes": total_bytes,
                "wall_us": wall_us,
                "resume_bytes": resume_bytes,
                "full_rehydrate_bytes": full_bytes,
                "tailer_versions": tailers.iter().map(|t| t.versions).collect::<Vec<_>>(),
                "log_floor_bytes": log_bytes,
                "log_floor_us": log_us,
                "sqlite_bytes": sqlite_bytes,
                "sqlite_us": sqlite_us,
            })
            .as_object()
            .expect("object")
            .clone(),
        );
        fields.extend(tail_latency.json_fields("tail_virtual_ms"));
        emit_summary(
            "s5_durable_stream",
            "deterministic",
            config.seed,
            &config.profile,
            JsonValue::Object(fields),
        );
    }
}

/// The floor: the same tokens appended to a plain file.
fn log_floor(config: &Config) -> (u64, u64) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("stream.log");
    let start = Instant::now();
    let mut file = std::fs::File::create(&path).expect("log file");
    for batch in 0..config.appends_per_stream() {
        for s in 0..config.streams {
            let first = batch * config.batch_tokens;
            let last = (first + config.batch_tokens).min(config.tokens());
            let piece: String = (first..last).map(|n| token(s, n)).collect();
            file.write_all(piece.as_bytes()).expect("append");
        }
    }
    file.sync_all().expect("sync");
    (
        std::fs::metadata(&path).expect("size").len(),
        start.elapsed().as_micros() as u64,
    )
}

/// A SQLite reference: one row per stream, appended in place.
fn sqlite_baseline(config: &Config) -> (u64, u64) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("streams.sqlite");
    let start = Instant::now();
    let db = rusqlite::Connection::open(&path).expect("sqlite");
    db.execute_batch("PRAGMA journal_mode = WAL; CREATE TABLE streams (id INTEGER PRIMARY KEY, content TEXT NOT NULL);")
        .expect("schema");
    for s in 0..config.streams {
        db.execute("INSERT INTO streams VALUES (?1, '')", [s as i64])
            .expect("insert");
    }
    for batch in 0..config.appends_per_stream() {
        for s in 0..config.streams {
            let first = batch * config.batch_tokens;
            let last = (first + config.batch_tokens).min(config.tokens());
            let piece: String = (first..last).map(|n| token(s, n)).collect();
            db.execute(
                "UPDATE streams SET content = content || ?2 WHERE id = ?1",
                rusqlite::params![s as i64, piece],
            )
            .expect("append");
        }
    }
    db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("checkpoint");
    drop(db);
    (
        std::fs::metadata(&path).expect("size").len(),
        start.elapsed().as_micros() as u64,
    )
}

/// The same appends on one local `Db`: a subscription follows them and
/// the stored value is exact.
fn db_surface(config: &Config) {
    let db: Db = open(schema(), 0, AuthorSubject::SYSTEM);
    for s in 0..config.streams {
        insert(
            &db,
            STREAMS,
            stream(s),
            cells([("name", text(format!("stream-{s}"))), ("content", text(""))]),
        );
    }
    let mut tailer = Tailer {
        name: "local".to_owned(),
        live: Live::subscribe(&db, &Query::from(STREAMS), ReadOpts::default()),
        last: BTreeMap::new(),
        versions: 0,
    };
    tailer.observe();
    let mut appends = 0;
    for batch in 0..config.appends_per_stream() {
        for s in 0..config.streams {
            let first = batch * config.batch_tokens;
            let last = (first + config.batch_tokens).min(config.tokens());
            let piece: String = (first..last).map(|n| token(s, n)).collect();
            block_on(db.append_value(STREAMS, stream(s), "content", piece.into_bytes()))
                .expect("append");
            appends += 1;
            tailer.observe();
        }
    }
    assert_eq!(appends, config.appends_per_stream() * config.streams);
    assert_eq!(tailer.live.rows.len(), config.streams);
    for s in 0..config.streams {
        let full: String = (0..config.tokens()).map(|n| token(s, n)).collect();
        assert_eq!(tailer.last[&stream(s)], full);
    }
}
