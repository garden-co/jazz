use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File};
use std::future::Future;
use std::io::{BufWriter, Write};
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use hdrhistogram::Histogram;
use jazz::db::{
    Db, DbConfig, DbIdentity, Node, ReadOpts, SeededRowIdSource, SubscriptionEvent,
    SubscriptionStream, Transport,
};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::{Durability, RocksDbStorage};
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState};
use jazz::peer::PeerState;
use jazz::protocol::SyncMessage;
use jazz::query::Query;
use jazz::schema::{JazzSchema, TableSchema};
use jazz::time::GlobalSeq;
use jazz::tx::{DurabilityTier, Fate};
use jazz::wire::TransportError;
use jazz_sim::{PeerProfile, bench_profile, emit_json_line, mem, metadata_fields};
use rusqlite::{Connection, params};
use serde_json::{Value as JsonValue, json};
use tempfile::TempDir;

const STREAMS: &str = "streams";
const STREAM_DOCS: &str = "streamDocs";

fn main() {
    if std::env::var("JAZZ_SMOKE").is_ok() {
        smoke();
        return;
    }
    let config = Config::from_env();
    let phase_selection = PhaseSelection::from_env();
    let profile = PeerProfile::new(
        config.profile.clone(),
        env_u64("JAZZ_LINK_ONE_WAY_MS", 1),
        env_u64("JAZZ_LINK_JITTER_MS", 0),
        env_u64("JAZZ_LINK_OVERHEAD_MS", 0),
    );
    if phase_selection.should_run("default") {
        let jazz = run_jazz(&config);
        let log = run_log_floor(&config);
        let sqlite = run_sqlite_baseline(&config);
        emit_summary(&config, &profile, &jazz, &log, &sqlite);
    }
    if phase_selection.should_run("db_surface_live") {
        let db_surface = run_db_surface(&config);
        emit_db_surface_summary(&config, &profile, &db_surface);
    }
    if phase_selection.should_run("process_local_resume") {
        let resume_canary = run_process_local_resume_canary(&config);
        emit_process_local_resume_canary(&config, &profile, &resume_canary);
    }
}

struct PhaseSelection {
    selected: Option<BTreeSet<String>>,
}

impl PhaseSelection {
    fn from_env() -> Self {
        let selected = std::env::var("JAZZ_BENCH_PHASES").ok().and_then(|raw| {
            let phases = raw
                .split(',')
                .map(str::trim)
                .filter(|phase| !phase.is_empty())
                .map(str::to_owned)
                .collect::<BTreeSet<_>>();
            if phases.is_empty() {
                None
            } else {
                Some(phases)
            }
        });
        let selection = Self { selected };
        selection.assert_supported();
        selection
    }

    fn should_run(&self, phase: &str) -> bool {
        self.selected
            .as_ref()
            .is_none_or(|selected| selected.contains(phase))
    }

    fn assert_supported(&self) {
        let Some(selected) = &self.selected else {
            return;
        };
        for phase in selected {
            assert!(
                matches!(
                    phase.as_str(),
                    "default" | "db_surface_live" | "process_local_resume"
                ),
                "unsupported JAZZ_BENCH_PHASES value {phase:?}; supported values: default, db_surface_live, process_local_resume"
            );
        }
    }
}

pub fn smoke() {
    let config = Config {
        seed: 0x5500_0001,
        profile: "s5-smoke".to_owned(),
        streams: 1,
        tokens_per_second: 4,
        batch_tokens: 2,
        run_seconds: 1,
        tailers: 1,
        resumers: 1,
    };
    let summary = run_jazz(&config);
    assert_eq!(summary.appends, config.commits_per_stream());
    assert_eq!(summary.resume_samples.len(), 3 * config.resumers);
    let db_surface = run_db_surface(&config);
    assert_eq!(db_surface.appends, config.commits_per_stream());
    assert_eq!(db_surface.rows, config.streams);
    let resume_canary = run_process_local_resume_canary(&config);
    assert!(resume_canary.full_rehydrate_bytes > 0);
    assert!(resume_canary.resume_bytes > 0);
    assert!(matches!(
        resume_canary.resume_status,
        "resumed_smaller" | "resumed_larger_or_equal"
    ));
}

#[derive(Debug)]
struct Config {
    seed: u64,
    profile: String,
    streams: usize,
    tokens_per_second: usize,
    batch_tokens: usize,
    run_seconds: usize,
    tailers: usize,
    resumers: usize,
}

impl Config {
    fn from_env() -> Self {
        let bench_profile = bench_profile();
        let full_rate = std::env::var("JAZZ_S5_FULL_RATE").is_ok();
        Self {
            seed: env_u64("JAZZ_SEED", 0x5500_0001),
            profile: std::env::var("JAZZ_PROFILE").unwrap_or_else(|_| "s5-local".to_owned()),
            streams: env_usize("JAZZ_S5_STREAMS", 1).max(1),
            tokens_per_second: env_usize(
                "JAZZ_S5_TOKENS_PER_SECOND",
                if full_rate {
                    100
                } else {
                    bench_profile.select(5, 10, 20)
                },
            )
            .max(1),
            batch_tokens: env_usize("JAZZ_S5_BATCH_TOKENS", bench_profile.select(5, 10, 10)).max(1),
            run_seconds: env_usize("JAZZ_S5_RUN_SECONDS", bench_profile.select(1, 2, 3)).max(1),
            tailers: env_usize("JAZZ_S5_TAILERS", bench_profile.select(1, 2, 2)),
            resumers: env_usize("JAZZ_S5_RESUMERS", bench_profile.select(1, 2, 2)),
        }
    }

    fn tokens_per_stream(&self) -> usize {
        self.tokens_per_second * self.run_seconds
    }

    fn commits_per_stream(&self) -> usize {
        self.tokens_per_stream().div_ceil(self.batch_tokens)
    }
}

#[derive(Debug)]
struct JazzSummary {
    appends: usize,
    append_latency: Histogram<u64>,
    tail_latency: Histogram<u64>,
    elapsed_us: u128,
    sync_bytes: u64,
    writer_upload_bytes: u64,
    history_bytes: u64,
    resume: Histogram<u64>,
    resume_bytes: u64,
    resume_samples: Vec<ResumeSample>,
    core_cpu_us: u128,
}

#[derive(Debug)]
struct ResumeSample {
    gap_tokens: usize,
    bytes: u64,
    elapsed_us: u64,
}

#[derive(Debug)]
struct BaselineSummary {
    elapsed_us: u128,
    bytes: u64,
    zstd3_bytes: u64,
    zstd19_bytes: u64,
}

#[derive(Debug)]
struct DbSurfaceSummary {
    appends: usize,
    tailers: usize,
    rows: usize,
    append_p50_us: u64,
    append_p95_us: u64,
    append_p99_us: u64,
    update_p50_us: u64,
    update_p99_us: u64,
    wait_p50_us: u64,
    wait_p99_us: u64,
    drain_p50_us: u64,
    drain_p99_us: u64,
    changed_p50_us: u64,
    changed_p95_us: u64,
    current_p50_us: u64,
    current_p95_us: u64,
    elapsed_us: u128,
    history_bytes: u64,
    edge_acceptance: Histogram<u64>,
    edge_hydration_bytes: u64,
    edge_hydration_rows: usize,
}

#[derive(Debug)]
struct ResumeCanarySummary {
    full_rehydrate_bytes: usize,
    resume_bytes: usize,
    resume_ratio: f64,
    resume_status: &'static str,
    rows: usize,
}

struct QueueTransport {
    outbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
}

fn queue_duplex() -> (Box<dyn Transport>, Box<dyn Transport>) {
    let client_to_server = Rc::new(RefCell::new(VecDeque::new()));
    let server_to_client = Rc::new(RefCell::new(VecDeque::new()));
    (
        Box::new(QueueTransport {
            outbound: Rc::clone(&client_to_server),
            inbound: Rc::clone(&server_to_client),
        }),
        Box::new(QueueTransport {
            outbound: server_to_client,
            inbound: client_to_server,
        }),
    )
}

impl Transport for QueueTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.outbound.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

fn run_jazz(config: &Config) -> JazzSummary {
    let schema = schema();
    let (core_dir, mut core) = open_node(node(250), schema.clone());
    let mut tailers = (0..config.tailers)
        .map(|idx| {
            let (dir, node) = open_node(node(30 + idx as u8), schema.clone());
            (
                dir,
                node,
                PeerState::new(),
                vec![Vec::<u8>::new(); config.streams],
            )
        })
        .collect::<Vec<_>>();
    let mut contents = vec![Vec::<u8>::new(); config.streams];
    let mut global_seq = 1_u64;
    for stream in 0..config.streams {
        seed_stream(&mut core, stream, &mut global_seq);
        for (_, tailer, peer, seen) in &mut tailers {
            let update = peer.current_rows_update(&mut core, STREAM_DOCS).unwrap();
            tailer.apply_sync_message(update).unwrap();
            seen[stream] = Vec::new();
        }
    }

    let mut append_latency = Histogram::new(3).unwrap();
    let mut tail_latency = Histogram::new(3).unwrap();
    let mut sync_bytes = 0_u64;
    let mut writer_upload_bytes = 0_u64;
    let mut core_cpu_us = 0_u128;
    let start = Instant::now();
    let mut now_ms = 1_000;
    for seq in 0..config.commits_per_stream() {
        for (stream, content) in contents.iter_mut().enumerate() {
            let before = Instant::now();
            append_tokens(config, content, stream, seq);
            let commit = MergeableCommit::new(STREAM_DOCS, stream_doc_row(stream), now_ms)
                .made_by(AuthorId::SYSTEM)
                .cells(cells([
                    ("stream", Value::Uuid(stream_row(stream).0)),
                    ("content", Value::Bytes(content.clone())),
                ]));
            let (tx_id, unit) = core.commit_mergeable_unit(commit).unwrap();
            core.apply_fate_update(
                tx_id,
                Fate::Accepted,
                Some(GlobalSeq(global_seq)),
                Some(DurabilityTier::Global),
            )
            .unwrap();
            global_seq += 1;
            let append_elapsed_us = before.elapsed().as_micros();
            append_latency.record(append_elapsed_us as u64).unwrap();
            core_cpu_us += append_elapsed_us;
            let tail_start = Instant::now();
            for (_, tailer, peer, seen) in &mut tailers {
                let update = peer.current_rows_update(&mut core, STREAM_DOCS).unwrap();
                sync_bytes += view_update_bytes(&update);
                tailer.apply_sync_message(update).unwrap();
                let current = read_doc(tailer, stream);
                assert!(current.starts_with(&seen[stream]));
                seen[stream] = current;
                tail_latency
                    .record(tail_start.elapsed().as_micros() as u64)
                    .unwrap();
            }
            writer_upload_bytes += commit_unit_bytes(&unit);
            now_ms += 1;
        }
    }
    for (_, _, _, seen) in &tailers {
        for (stream, content) in contents.iter().enumerate().take(config.streams) {
            assert_eq!(&seen[stream], content);
        }
    }

    let mut resume = Histogram::new(3).unwrap();
    let mut resume_bytes = 0_u64;
    let mut resume_samples = Vec::new();
    let resume_points = [25_usize, 50, 75];
    for point_pct in resume_points {
        let known_tokens = config.tokens_per_stream() * point_pct / 100;
        let gap_tokens = config.tokens_per_stream().saturating_sub(known_tokens);
        for idx in 0..config.resumers {
            let (_dir, mut resumer) = open_node(node(90 + idx as u8), schema.clone());
            let mut peer = PeerState::new();
            let resume_start = Instant::now();
            let update = peer.current_rows_update(&mut core, STREAM_DOCS).unwrap();
            let bytes = view_update_bytes(&update);
            resume_bytes += bytes;
            resumer.apply_sync_message(update).unwrap();
            let elapsed_us = resume_start.elapsed().as_micros() as u64;
            resume.record(elapsed_us).unwrap();
            resume_samples.push(ResumeSample {
                gap_tokens,
                bytes,
                elapsed_us,
            });
            for (stream, content) in contents.iter().enumerate().take(config.streams) {
                assert_eq!(read_doc(&mut resumer, stream), *content);
            }
        }
    }

    JazzSummary {
        appends: config.streams * config.commits_per_stream(),
        append_latency,
        tail_latency,
        elapsed_us: start.elapsed().as_micros(),
        sync_bytes,
        writer_upload_bytes,
        history_bytes: storage_bytes(core_dir.path()),
        resume,
        resume_bytes,
        resume_samples,
        core_cpu_us,
    }
}

fn run_db_surface(config: &Config) -> DbSurfaceSummary {
    let schema = schema();
    let (core_dir, mut core) = open_node(node(250), schema.clone());
    let (_edge_dir, mut edge) = open_node(node(170), schema.clone());
    let mut edge_peer = PeerState::new();
    let outbound = Rc::new(RefCell::new(VecDeque::new()));
    let inbound = Rc::new(RefCell::new(VecDeque::new()));
    let (dir, db) = open_db(node(70), schema.clone());
    let _upstream = db.connect_upstream(Box::new(QueueTransport {
        outbound: Rc::clone(&outbound),
        inbound: Rc::clone(&inbound),
    }));
    let mut edge_acceptance = Histogram::new(3).unwrap();
    let mut contents = vec![Vec::<u8>::new(); config.streams];
    for stream in 0..config.streams {
        let stream_write = db
            .insert_with_id(
                STREAMS,
                stream_row(stream),
                cells([("name", Value::String(format!("stream-{stream}")))]),
            )
            .expect("db stream insert");
        block_on(stream_write.wait(DurabilityTier::Local)).expect("db stream local wait");
        drain_db_route(
            &db,
            &outbound,
            &inbound,
            &mut edge,
            &mut edge_peer,
            &mut core,
            &mut edge_acceptance,
        );
        let doc_write = db
            .insert_with_id(
                STREAM_DOCS,
                stream_doc_row(stream),
                cells([
                    ("stream", Value::Uuid(stream_row(stream).0)),
                    ("content", Value::Bytes(Vec::new())),
                ]),
            )
            .expect("db stream doc insert");
        block_on(doc_write.wait(DurabilityTier::Local)).expect("db stream doc local wait");
        drain_db_route(
            &db,
            &outbound,
            &inbound,
            &mut edge,
            &mut edge_peer,
            &mut core,
            &mut edge_acceptance,
        );
    }
    let mut edge_hydration_bytes = 0;
    let mut edge_hydration_rows = 0;
    for table in [STREAMS, STREAM_DOCS] {
        let update = edge_peer.current_rows_update(&mut core, table).unwrap();
        edge_hydration_bytes += view_update_bytes(&update);
        edge_hydration_rows += result_row_count(&update);
        edge.apply_sync_message(update).unwrap();
    }

    let query = Query::from(STREAM_DOCS);
    let prepared = db.prepare_query(&query).expect("prepare stream docs query");
    let mut watches = (0..config.tailers)
        .map(|_| block_on(db.subscribe(&prepared, ReadOpts::default())).expect("db subscribe"))
        .collect::<Vec<_>>();
    let mut watch_rows = watches
        .iter_mut()
        .map(|watch| {
            let event = block_on(watch.next_event()).expect("db subscription opened");
            let rows = subscription_opened_rows(event);
            assert_eq!(rows.len(), config.streams);
            rows
        })
        .collect::<Vec<_>>();

    let mut append_latencies = Vec::new();
    let mut update_latencies = Vec::new();
    let mut wait_latencies = Vec::new();
    let mut drain_latencies = Vec::new();
    let mut changed_latencies = Vec::new();
    let mut current_latencies = Vec::new();
    let start = Instant::now();
    for seq in 0..config.commits_per_stream() {
        for stream in 0..contents.len() {
            append_tokens(config, &mut contents[stream], stream, seq);
            let content = contents[stream].clone();
            let before = Instant::now();
            let update_start = Instant::now();
            let write = db
                .update(
                    STREAM_DOCS,
                    stream_doc_row(stream),
                    cells([("content", Value::Bytes(content))]),
                )
                .expect("db stream doc update");
            update_latencies.push(update_start.elapsed().as_micros() as u64);
            let wait_start = Instant::now();
            block_on(write.wait(DurabilityTier::Local)).expect("db stream doc local wait");
            wait_latencies.push(wait_start.elapsed().as_micros() as u64);
            let drain_start = Instant::now();
            drain_db_route(
                &db,
                &outbound,
                &inbound,
                &mut edge,
                &mut edge_peer,
                &mut core,
                &mut edge_acceptance,
            );
            drain_latencies.push(drain_start.elapsed().as_micros() as u64);
            append_latencies.push(before.elapsed().as_micros() as u64);

            for (watch, rows) in watches.iter_mut().zip(&mut watch_rows) {
                let changed_start = Instant::now();
                let event = block_on(watch.next_event()).expect("db subscription changed");
                changed_latencies.push(changed_start.elapsed().as_micros() as u64);
                let current_start = Instant::now();
                apply_subscription_event(rows, event);
                let seen = db_stream_docs(&schema, rows.clone());
                current_latencies.push(current_start.elapsed().as_micros() as u64);
                assert_eq!(seen, contents);
            }
        }
    }
    assert_eq!(
        db_stream_docs(&schema, db.read(&prepared).expect("db read stream docs")),
        contents
    );

    DbSurfaceSummary {
        appends: config.streams * config.commits_per_stream(),
        tailers: watches.len(),
        rows: config.streams,
        append_p50_us: percentile(&mut append_latencies.clone(), 50),
        append_p95_us: percentile(&mut append_latencies.clone(), 95),
        append_p99_us: percentile(&mut append_latencies, 99),
        update_p50_us: percentile(&mut update_latencies.clone(), 50),
        update_p99_us: percentile(&mut update_latencies, 99),
        wait_p50_us: percentile(&mut wait_latencies.clone(), 50),
        wait_p99_us: percentile(&mut wait_latencies, 99),
        drain_p50_us: percentile(&mut drain_latencies.clone(), 50),
        drain_p99_us: percentile(&mut drain_latencies, 99),
        changed_p50_us: percentile(&mut changed_latencies.clone(), 50),
        changed_p95_us: percentile(&mut changed_latencies, 95),
        current_p50_us: percentile(&mut current_latencies.clone(), 50),
        current_p95_us: percentile(&mut current_latencies, 95),
        elapsed_us: start.elapsed().as_micros(),
        history_bytes: storage_bytes(dir.path()) + storage_bytes(core_dir.path()),
        edge_acceptance,
        edge_hydration_bytes,
        edge_hydration_rows,
    }
}

fn run_process_local_resume_canary(config: &Config) -> ResumeCanarySummary {
    let schema = schema();
    let (_server_dir, server_state) = open_node(node(180), schema.clone());
    let server = Node::new(server_state);
    let (_client_dir, client) = open_db(node(181), schema.clone());
    let streams = config.streams.max(16);
    let mut content = Vec::new();
    let mut global_seq = 1_u64;
    {
        let server_node = server.node();
        let mut core = server_node.borrow_mut();
        for stream in 0..streams {
            seed_stream(&mut core, stream, &mut global_seq);
        }
    }

    let (client_transport, server_transport) = queue_duplex();
    let upstream = client.connect_upstream(client_transport);
    let subscriber = server.accept_subscriber(server_transport, AuthorId::SYSTEM);
    let query = Query::from(STREAM_DOCS);
    let prepared = client
        .prepare_query(&query)
        .expect("prepare stream docs query");
    let mut watch =
        block_on(client.subscribe(&prepared, ReadOpts::default())).expect("db subscribe");

    client.tick().expect("client fresh subscribe tick");
    subscriber
        .borrow_mut()
        .serve_current_rows(STREAM_DOCS)
        .expect("serve fresh rows");
    client.tick().expect("client fresh apply tick");
    let mut rows = Vec::new();
    drain_subscription_events(&mut watch, &mut rows);
    assert_eq!(rows.len(), streams);
    let full_rehydrate_bytes = subscriber
        .borrow()
        .last_resume_bytes()
        .expect("fresh rehydrate bytes");

    let cursor = subscriber
        .borrow_mut()
        .take_resume_cursor()
        .expect("resume cursor");
    assert!(client.detach_connection(&upstream));
    assert!(server.detach_connection(&subscriber));

    append_tokens(config, &mut content, 0, 0);
    {
        let server_node = server.node();
        let mut core = server_node.borrow_mut();
        let (tx_id, _) = core
            .commit_mergeable_unit(
                MergeableCommit::new(STREAM_DOCS, stream_doc_row(0), 10).cells(cells([
                    ("stream", Value::Uuid(stream_row(0).0)),
                    ("content", Value::Bytes(content.clone())),
                ])),
            )
            .expect("server commit");
        core.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(GlobalSeq(global_seq)),
            Some(DurabilityTier::Global),
        )
        .expect("server fate");
    }

    let (client_transport, server_transport) = queue_duplex();
    let _resumed_upstream = client.connect_upstream(client_transport);
    let resumed = server.accept_subscriber_with_resume(server_transport, AuthorId::SYSTEM, cursor);

    client.tick().expect("client resumed subscribe tick");
    server.tick().expect("server resumed tick");
    client.tick().expect("client resumed apply tick");

    let resume_bytes = resumed
        .borrow()
        .last_resume_bytes()
        .expect("resume catch-up bytes");
    drain_subscription_events(&mut watch, &mut rows);
    let seen = db_stream_docs(&schema, rows);
    let mut expected = vec![Vec::new(); streams];
    expected[0] = content;
    assert_eq!(seen, expected);

    ResumeCanarySummary {
        full_rehydrate_bytes,
        resume_bytes,
        resume_ratio: resume_bytes as f64 / full_rehydrate_bytes.max(1) as f64,
        resume_status: if resume_bytes < full_rehydrate_bytes {
            "resumed_smaller"
        } else {
            "resumed_larger_or_equal"
        },
        rows: seen.len(),
    }
}

fn drain_db_route(
    db: &Db<RocksDbStorage>,
    outbound: &Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: &Rc<RefCell<VecDeque<SyncMessage>>>,
    edge: &mut NodeState<RocksDbStorage>,
    edge_peer: &mut PeerState,
    core: &mut NodeState<RocksDbStorage>,
    edge_acceptance: &mut Histogram<u64>,
) {
    db.tick().unwrap();
    while let Some(unit) = outbound.borrow_mut().pop_front() {
        let SyncMessage::CommitUnit { tx, versions } = unit.clone() else {
            continue;
        };
        let start = Instant::now();
        edge_peer
            .ingest_edge_mergeable_commit_unit(edge, tx, versions, u64::MAX)
            .unwrap();
        edge_acceptance
            .record(start.elapsed().as_micros() as u64)
            .unwrap();
        for update in core.apply_sync_message(unit).unwrap() {
            edge.apply_sync_message(update.clone()).unwrap();
            inbound.borrow_mut().push_back(update);
            db.tick().unwrap();
        }
    }
}

fn seed_stream(core: &mut NodeState<RocksDbStorage>, stream: usize, global_seq: &mut u64) {
    let tx = core
        .commit_mergeable(
            MergeableCommit::new(STREAMS, stream_row(stream), 1)
                .cells(cells([("name", Value::String(format!("stream-{stream}")))])),
        )
        .unwrap();
    core.apply_fate_update(
        tx,
        Fate::Accepted,
        Some(GlobalSeq(*global_seq)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    *global_seq += 1;
    let tx = core
        .commit_mergeable(
            MergeableCommit::new(STREAM_DOCS, stream_doc_row(stream), 2).cells(cells([
                ("stream", Value::Uuid(stream_row(stream).0)),
                ("content", Value::Bytes(Vec::new())),
            ])),
        )
        .unwrap();
    core.apply_fate_update(
        tx,
        Fate::Accepted,
        Some(GlobalSeq(*global_seq)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    *global_seq += 1;
}

fn run_log_floor(config: &Config) -> BaselineSummary {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stream.log");
    let mut writer = BufWriter::new(File::create(&path).unwrap());
    let mut payloads = Vec::new();
    let start = Instant::now();
    for stream in 0..config.streams {
        let mut content = Vec::new();
        for seq in 0..config.commits_per_stream() {
            let before = content.len();
            append_tokens(config, &mut content, stream, seq);
            let record = &content[before..];
            let compressed = zstd::bulk::compress(record, 3).unwrap();
            writer
                .write_all(&(compressed.len() as u32).to_le_bytes())
                .unwrap();
            writer.write_all(&compressed).unwrap();
            payloads.extend_from_slice(record);
        }
    }
    writer.flush().unwrap();
    writer.get_ref().sync_all().unwrap();
    BaselineSummary {
        elapsed_us: start.elapsed().as_micros(),
        bytes: fs::metadata(path).unwrap().len(),
        zstd3_bytes: zstd::bulk::compress(&payloads, 3).unwrap().len() as u64,
        zstd19_bytes: zstd::bulk::compress(&payloads, 19).unwrap().len() as u64,
    }
}

fn run_sqlite_baseline(config: &Config) -> BaselineSummary {
    let dir = tempfile::tempdir().unwrap();
    let conn = Connection::open(dir.path().join("stream.sqlite")).unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.pragma_update(None, "synchronous", "NORMAL").unwrap();
    conn.execute(
        "CREATE TABLE events(stream INTEGER NOT NULL, seq INTEGER NOT NULL, payload BLOB NOT NULL, PRIMARY KEY(stream, seq))",
        [],
    )
    .unwrap();
    let mut payloads = Vec::new();
    let start = Instant::now();
    for stream in 0..config.streams {
        let mut content = Vec::new();
        for seq in 0..config.commits_per_stream() {
            let before = content.len();
            append_tokens(config, &mut content, stream, seq);
            let payload = &content[before..];
            conn.execute(
                "INSERT INTO events VALUES (?1, ?2, ?3)",
                params![stream as i64, seq as i64, payload],
            )
            .unwrap();
            payloads.extend_from_slice(payload);
        }
    }
    let elapsed_us = start.elapsed().as_micros();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    drop(conn);
    BaselineSummary {
        elapsed_us,
        bytes: storage_bytes(dir.path()),
        zstd3_bytes: zstd::bulk::compress(&payloads, 3).unwrap().len() as u64,
        zstd19_bytes: zstd::bulk::compress(&payloads, 19).unwrap().len() as u64,
    }
}

fn emit_summary(
    config: &Config,
    profile: &PeerProfile,
    jazz: &JazzSummary,
    log: &BaselineSummary,
    sqlite: &BaselineSummary,
) {
    let tokens = (config.streams * config.tokens_per_stream()).max(1) as f64;
    let mut fields = metadata_fields(
        "s5_durable_stream",
        "synchronous",
        config.seed,
        &profile.name,
    );
    fields.insert("streams".to_owned(), json!(config.streams));
    fields.insert(
        "tokens_per_second".to_owned(),
        json!(config.tokens_per_second),
    );
    fields.insert("batch_tokens".to_owned(), json!(config.batch_tokens));
    fields.insert("run_seconds".to_owned(), json!(config.run_seconds));
    fields.insert("tailers".to_owned(), json!(config.tailers));
    fields.insert("resumers".to_owned(), json!(config.resumers));
    fields.insert("commits".to_owned(), json!(jazz.appends));
    fields.insert(
        "append_p50_us".to_owned(),
        json!(jazz.append_latency.value_at_quantile(0.5)),
    );
    fields.insert(
        "append_tail_p50_us".to_owned(),
        json!(jazz.tail_latency.value_at_quantile(0.5)),
    );
    fields.insert(
        "append_tail_p99_us".to_owned(),
        json!(jazz.tail_latency.value_at_quantile(0.99)),
    );
    fields.insert(
        "link_rtt_floor_us".to_owned(),
        json!(profile.one_way_latency_ms * 2_000),
    );
    fields.insert(
        "sustained_appends_per_sec".to_owned(),
        json!(jazz.appends as f64 / (jazz.elapsed_us as f64 / 1_000_000.0).max(0.000_001)),
    );
    fields.insert(
        "history_metadata_bytes_per_token".to_owned(),
        json!(jazz.history_bytes as f64 / tokens),
    );
    fields.insert("peak_rss_bytes".to_owned(), json!(mem::peak_rss_bytes()));
    fields.insert(
        "durability_regime".to_owned(),
        json!("wal_no_sync_equivalent; log floor fsyncs once at end"),
    );
    fields.insert(
        "log_floor_bytes_per_token".to_owned(),
        json!(log.bytes as f64 / tokens),
    );
    fields.insert(
        "sqlite_db_bytes_per_token".to_owned(),
        json!(sqlite.bytes as f64 / tokens),
    );
    fields.insert(
        "storage_bytes_per_token_note".to_owned(),
        json!("most meaningful at full-rate/full-duration knobs"),
    );
    fields.insert(
        "synced_bytes_per_token_per_tailer".to_owned(),
        json!(jazz.sync_bytes as f64 / tokens / config.tailers.max(1) as f64),
    );
    fields.insert(
        "writer_upload_bytes".to_owned(),
        json!(jazz.writer_upload_bytes),
    );
    fields.insert(
        "writer_upload_bytes_per_token".to_owned(),
        json!(jazz.writer_upload_bytes as f64 / tokens),
    );
    fields.insert(
        "resume_p50_us".to_owned(),
        json!(jazz.resume.value_at_quantile(0.5)),
    );
    fields.insert("resume_bytes".to_owned(), json!(jazz.resume_bytes));
    fields.insert(
        "resume_gap_tokens".to_owned(),
        json!(
            jazz.resume_samples
                .iter()
                .map(|sample| sample.gap_tokens)
                .collect::<Vec<_>>()
        ),
    );
    fields.insert(
        "resume_bytes_by_gap".to_owned(),
        json!(
            jazz.resume_samples
                .iter()
                .map(|sample| sample.bytes)
                .collect::<Vec<_>>()
        ),
    );
    fields.insert(
        "resume_elapsed_us_by_gap".to_owned(),
        json!(
            jazz.resume_samples
                .iter()
                .map(|sample| sample.elapsed_us)
                .collect::<Vec<_>>()
        ),
    );
    fields.insert(
        "storage_amplification".to_owned(),
        json!(jazz.history_bytes as f64 / (tokens * 4.0)),
    );
    fields.insert(
        "core_cpu_us_per_append".to_owned(),
        json!(jazz.core_cpu_us as f64 / jazz.appends.max(1) as f64),
    );
    fields.insert("zstd3_payload_bytes".to_owned(), json!(log.zstd3_bytes));
    fields.insert("zstd19_payload_bytes".to_owned(), json!(log.zstd19_bytes));
    fields.insert(
        "log_floor_nosync_elapsed_us".to_owned(),
        json!(log.elapsed_us),
    );
    fields.insert("sqlite_elapsed_us".to_owned(), json!(sqlite.elapsed_us));
    fields.insert(
        "correctness".to_owned(),
        json!("prefix_monotone_and_resumer_exact"),
    );
    emit_json_line("s5_durable_stream", &JsonValue::Object(fields).to_string());
}

fn emit_db_surface_summary(config: &Config, profile: &PeerProfile, summary: &DbSurfaceSummary) {
    let tokens = (config.streams * config.tokens_per_stream()).max(1) as f64;
    let mut fields = metadata_fields(
        "s5_durable_stream",
        "db_surface",
        config.seed,
        &profile.name,
    );
    fields.insert("phase".to_owned(), json!("db_surface_live"));
    fields.insert("streams".to_owned(), json!(config.streams));
    fields.insert(
        "tokens_per_second".to_owned(),
        json!(config.tokens_per_second),
    );
    fields.insert("batch_tokens".to_owned(), json!(config.batch_tokens));
    fields.insert("run_seconds".to_owned(), json!(config.run_seconds));
    fields.insert("tailers".to_owned(), json!(summary.tailers));
    fields.insert("commits".to_owned(), json!(summary.appends));
    fields.insert("rows".to_owned(), json!(summary.rows));
    fields.insert("append_p50_us".to_owned(), json!(summary.append_p50_us));
    fields.insert("append_p95_us".to_owned(), json!(summary.append_p95_us));
    fields.insert("append_p99_us".to_owned(), json!(summary.append_p99_us));
    fields.insert("update_p50_us".to_owned(), json!(summary.update_p50_us));
    fields.insert("update_p99_us".to_owned(), json!(summary.update_p99_us));
    fields.insert("wait_p50_us".to_owned(), json!(summary.wait_p50_us));
    fields.insert("wait_p99_us".to_owned(), json!(summary.wait_p99_us));
    fields.insert("drain_p50_us".to_owned(), json!(summary.drain_p50_us));
    fields.insert("drain_p99_us".to_owned(), json!(summary.drain_p99_us));
    fields.insert("changed_p50_us".to_owned(), json!(summary.changed_p50_us));
    fields.insert("changed_p95_us".to_owned(), json!(summary.changed_p95_us));
    fields.insert("current_p50_us".to_owned(), json!(summary.current_p50_us));
    fields.insert("current_p95_us".to_owned(), json!(summary.current_p95_us));
    fields.insert(
        "sustained_appends_per_sec".to_owned(),
        json!(summary.appends as f64 / (summary.elapsed_us as f64 / 1_000_000.0).max(0.000_001)),
    );
    fields.insert(
        "history_metadata_bytes_per_token".to_owned(),
        json!(summary.history_bytes as f64 / tokens),
    );
    fields.insert("peak_rss_bytes".to_owned(), json!(mem::peak_rss_bytes()));
    fields.insert(
        "correctness".to_owned(),
        json!("db_watch_tailers_prefix_monotone_and_final_exact"),
    );
    emit_json_line("s5_durable_stream", &JsonValue::Object(fields).to_string());

    let mut acceptance = metadata_fields(
        "s5_durable_stream",
        "db_surface",
        config.seed,
        &profile.name,
    );
    acceptance.insert("phase".to_owned(), json!("edge_mergeable_acceptance"));
    acceptance.insert(
        "acceptance_p50_us".to_owned(),
        json!(summary.edge_acceptance.value_at_quantile(0.50)),
    );
    acceptance.insert(
        "acceptance_p95_us".to_owned(),
        json!(summary.edge_acceptance.value_at_quantile(0.95)),
    );
    acceptance.insert("durability_tier".to_owned(), json!("Edge"));
    emit_json_line(
        "s5_durable_stream",
        &JsonValue::Object(acceptance).to_string(),
    );

    let mut hydration = metadata_fields(
        "s5_durable_stream",
        "db_surface",
        config.seed,
        &profile.name,
    );
    hydration.insert("phase".to_owned(), json!("edge_permission_scope_hydration"));
    hydration.insert("scope".to_owned(), json!("durable_stream_table_surface"));
    hydration.insert(
        "hydration_bytes".to_owned(),
        json!(summary.edge_hydration_bytes),
    );
    hydration.insert(
        "hydration_floor_bytes".to_owned(),
        json!(summary.edge_hydration_bytes),
    );
    hydration.insert(
        "hydration_rows".to_owned(),
        json!(summary.edge_hydration_rows),
    );
    emit_json_line(
        "s5_durable_stream",
        &JsonValue::Object(hydration).to_string(),
    );
}

fn emit_process_local_resume_canary(
    config: &Config,
    profile: &PeerProfile,
    summary: &ResumeCanarySummary,
) {
    let mut fields = metadata_fields(
        "s5_durable_stream",
        "db_surface",
        config.seed,
        &profile.name,
    );
    fields.insert("phase".to_owned(), json!("process_local_resume"));
    fields.insert(
        "full_rehydrate_bytes".to_owned(),
        json!(summary.full_rehydrate_bytes),
    );
    fields.insert("resume_bytes".to_owned(), json!(summary.resume_bytes));
    fields.insert("resume_ratio".to_owned(), json!(summary.resume_ratio));
    fields.insert("resume_status".to_owned(), json!(summary.resume_status));
    fields.insert("rows".to_owned(), json!(summary.rows));
    emit_json_line("s5_durable_stream", &JsonValue::Object(fields).to_string());
}

fn append_tokens(config: &Config, content: &mut Vec<u8>, stream: usize, seq: usize) {
    let remaining = config
        .tokens_per_stream()
        .saturating_sub(seq * config.batch_tokens);
    let count = remaining.min(config.batch_tokens);
    for token in 0..count {
        let n = (stream as u64).wrapping_mul(1_000_003) ^ (seq as u64) ^ (token as u64);
        content.extend_from_slice(format!("{:04x}", n & 0xffff).as_bytes());
    }
}

fn read_doc(node: &mut NodeState<RocksDbStorage>, stream: usize) -> Vec<u8> {
    let schema = schema();
    let table = table_schema(&schema, STREAM_DOCS);
    node.current_rows(STREAM_DOCS, DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .find(|row| row.row_uuid() == stream_doc_row(stream))
        .and_then(|row| match row.cell(table, "content").unwrap() {
            Value::Bytes(bytes) => Some(bytes),
            _ => None,
        })
        .unwrap_or_default()
}

fn schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new(STREAMS, [col("name", ColumnType::String)]),
        TableSchema::new(
            STREAM_DOCS,
            [
                col("stream", ColumnType::Uuid),
                col("content", ColumnType::Bytes),
            ],
        )
        .with_reference("stream", STREAMS),
    ])
}

fn open_node(node_uuid: NodeUuid, schema: JazzSchema) -> (TempDir, NodeState<RocksDbStorage>) {
    let dir = tempfile::tempdir().unwrap();
    let refs = schema.column_families();
    let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
    let node = NodeState::new(node_uuid, schema, storage).unwrap();
    (dir, node)
}

fn open_db(node_uuid: NodeUuid, schema: JazzSchema) -> (TempDir, Db<RocksDbStorage>) {
    let dir = tempfile::tempdir().unwrap();
    let refs = schema.column_families();
    let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
    let db = block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: node_uuid,
            author: AuthorId::SYSTEM,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(u64::from_le_bytes(
            node_uuid.as_bytes()[..8]
                .try_into()
                .expect("node seed bytes"),
        )))),
        large_value_checkpoint_op_interval: 1024,
    }))
    .expect("db open");
    (dir, db)
}

fn storage_bytes(path: &std::path::Path) -> u64 {
    fs::read_dir(path)
        .into_iter()
        .flat_map(|entries| entries.flatten())
        .map(|entry| {
            let meta = entry.metadata().unwrap();
            if meta.is_dir() {
                storage_bytes(&entry.path())
            } else {
                meta.len()
            }
        })
        .sum()
}

fn commit_unit_bytes(update: &SyncMessage) -> u64 {
    match update {
        SyncMessage::CommitUnit { versions, .. } => versions
            .iter()
            .map(|version| version.record().raw().len() as u64 + 64)
            .sum(),
        _ => 0,
    }
}

fn view_update_bytes(update: &SyncMessage) -> u64 {
    match update {
        SyncMessage::ViewUpdate {
            version_bundles,
            peer_payload_inventory,
            result_member_adds,
            result_member_removes,
            ..
        } => {
            version_bundles
                .iter()
                .flat_map(|bundle| bundle.versions.iter())
                .map(|version| version.record().raw().len() as u64 + 64)
                .sum::<u64>()
                + (peer_payload_inventory.complete_tx_payloads.len() as u64 * 24)
                + ((result_member_adds.len() + result_member_removes.len()) as u64 * 64)
        }
        _ => 0,
    }
}

fn result_row_count(update: &SyncMessage) -> usize {
    match update {
        SyncMessage::ViewUpdate {
            result_member_adds,
            result_member_removes,
            ..
        } => result_member_adds.len() + result_member_removes.len(),
        _ => 0,
    }
}

fn cells<const N: usize>(items: [(&str, Value); N]) -> BTreeMap<String, Value> {
    items.into_iter().map(|(k, v)| (k.to_owned(), v)).collect()
}

fn col(name: &str, ty: ColumnType) -> ColumnSchema {
    ColumnSchema::new(name, ty)
}

fn table_schema<'a>(schema: &'a JazzSchema, table: &str) -> &'a TableSchema {
    schema
        .tables
        .iter()
        .find(|candidate| candidate.name == table)
        .unwrap()
}

fn subscription_opened_rows(event: SubscriptionEvent) -> Vec<jazz::node::CurrentRow> {
    match event {
        SubscriptionEvent::Opened { current, .. } | SubscriptionEvent::Reset { current, .. } => {
            current.rows
        }
        other => panic!("expected subscription snapshot, got {other:?}"),
    }
}

fn drain_subscription_events(
    subscription: &mut SubscriptionStream,
    rows: &mut Vec<jazz::node::CurrentRow>,
) {
    while let Some(event) = subscription.try_next_event() {
        apply_subscription_event(rows, event);
    }
}

fn apply_subscription_event(rows: &mut Vec<jazz::node::CurrentRow>, event: SubscriptionEvent) {
    match event {
        SubscriptionEvent::Opened { current, .. } | SubscriptionEvent::Reset { current, .. } => {
            *rows = current.rows;
        }
        SubscriptionEvent::Delta {
            added,
            updated,
            removed,
            ..
        } => {
            let removed = removed
                .into_iter()
                .map(|row| row.row_uuid)
                .collect::<BTreeSet<_>>();
            rows.retain(|row| !removed.contains(&row.row_uuid()));
            for row in added.into_iter().chain(updated) {
                if let Some(slot) = rows
                    .iter_mut()
                    .find(|existing| existing.row_uuid() == row.row_uuid())
                {
                    *slot = row;
                } else {
                    rows.push(row);
                }
            }
        }
        SubscriptionEvent::Closed => {}
    }
}

fn db_stream_docs(schema: &JazzSchema, rows: Vec<jazz::node::CurrentRow>) -> Vec<Vec<u8>> {
    let table = table_schema(schema, STREAM_DOCS);
    let mut docs = rows
        .into_iter()
        .map(|row| {
            let stream = match row.cell(table, "stream").unwrap() {
                Value::Uuid(uuid) => stream_idx(RowUuid(uuid)),
                other => panic!("unexpected stream cell {other:?}"),
            };
            let content = match row.cell(table, "content").unwrap() {
                Value::Bytes(bytes) => bytes,
                other => panic!("unexpected content cell {other:?}"),
            };
            (stream, content)
        })
        .collect::<Vec<_>>();
    docs.sort_by_key(|(stream, _)| *stream);
    docs.into_iter().map(|(_, content)| content).collect()
}

fn row(tag: u8, value: u64) -> RowUuid {
    let mut bytes = [tag; 16];
    bytes[8..].copy_from_slice(&value.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn stream_row(stream: usize) -> RowUuid {
    row(1, stream as u64)
}

fn stream_doc_row(stream: usize) -> RowUuid {
    row(2, stream as u64)
}

fn stream_idx(row: RowUuid) -> usize {
    u64::from_be_bytes(row.as_bytes()[8..].try_into().unwrap()) as usize
}

fn percentile(values: &mut [u64], percentile: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    let idx = ((values.len() - 1) * percentile) / 100;
    values[idx]
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
