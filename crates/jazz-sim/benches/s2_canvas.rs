use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::pin;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, SeededRowIdSource, SubscriptionEvent};
use jazz::groove::records::Value;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::node::{CurrentRow, MergeableCommit, NodeState};
use jazz::peer::PeerState;
use jazz::protocol::{RegisterShapeOptions, ShapeAst, Subscribe, SubscriptionKey, SyncMessage};
use jazz::query::{Binding, Query, ValidatedQuery, col, eq, lit, param};
use jazz::schema::JazzSchema;
use jazz::time::GlobalTime;
use jazz::tools::policy_expr as public_policy_expr;
use jazz::tools::public_schema::{
    ColumnType as PublicColumnType, SchemaBuilder, TableSchema as PublicTableSchema,
};
use jazz::tx::{DurabilityTier, Fate};
use jazz_sim::distributions::Lcg;
use jazz_sim::fixture::{
    apply_sync_message_settled, commit_mergeable_unit_settled, ingest_commit_unit_settled,
};
use jazz_sim::public_schema_fixture::{all_operation_policies, compile_public_schema};
use jazz_sim::view_accounting::{bytes_floor, version_bundle_refs, view_update_bytes};
use jazz_sim::{
    DeterministicDriver, DriverContext, Metrics, NodeRole, PeerProfile, SimulatorTransportCodec,
    ThreadedDriver, Topology, bench_profile, emit_json_line, loopback_transport_message, mem,
    metadata_fields, scenario_transport_codec_env,
};
use jazz_storage_rocksdb::{Durability, RocksDbStorage};
use serde_json::{Value as JsonValue, json};

const CANVASES: &str = "canvases";
const INVITES: &str = "canvasInvites";
const SHAPES: &str = "shapes";

type SharedMetrics = Arc<Mutex<Metrics>>;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    if std::env::var("JAZZ_SMOKE").is_ok() {
        smoke();
        return;
    }
    let config = Config::from_env();
    let profile = PeerProfile::new(
        config.profile.clone(),
        env_u64("JAZZ_LINK_ONE_WAY_MS", 1),
        env_u64("JAZZ_LINK_JITTER_MS", 0),
        env_u64("JAZZ_LINK_OVERHEAD_MS", 0),
    );
    for coalesced in [false, true] {
        let topology = topology(&config, profile.clone());
        let mut deterministic = DeterministicDriver::new(topology.clone(), config.seed)
            .with_transport_codec(config.transport_codec);
        let summary = run_live(&mut deterministic, &config, coalesced);
        let transport_metrics = deterministic.metrics_json_fields();
        emit_live_summary(
            "deterministic",
            coalesced,
            &config,
            &summary,
            transport_metrics,
        );
        for summary in run_historical_loads(&mut deterministic, &config, coalesced) {
            emit_historical_load_summary(coalesced, &config, &summary);
        }
        emit_concurrent_live_summary(
            coalesced,
            &config,
            &run_concurrent_live(&config, coalesced, profile.clone()),
        );
        emit_db_surface_summary(coalesced, &config, &run_db_surface(&config, coalesced));
    }
    let topology = topology(&config, profile);
    let mut threaded = ThreadedDriver::new(topology, config.seed ^ 0x5200_fa11)
        .with_transport_codec(config.transport_codec);
    let failure = run_failure(&mut threaded, &config);
    let transport_metrics = threaded.metrics_json_fields();
    emit_failure_summary(&config, &failure, transport_metrics);
}

pub fn smoke() {
    let config = Config {
        seed: 0x5200_cafe,
        profile: "s2-smoke".to_owned(),
        shapes: 4,
        active: 2,
        passive: 1,
        rate_per_sec: 2,
        duration_secs: 1,
        transport_codec: SimulatorTransportCodec::WireFrames,
    };
    let profile = PeerProfile::new(config.profile.clone(), 1, 0, 0);
    for coalesced in [false, true] {
        let topology = topology(&config, profile.clone());
        let mut deterministic = DeterministicDriver::new(topology, config.seed);
        let _summary = run_live(&mut deterministic, &config, coalesced);
        let historical = run_historical_loads(&mut deterministic, &config, coalesced);
        if !historical.is_empty() {
            assert_eq!(historical.len(), 3);
        }
        let db_surface = run_db_surface(&config, coalesced);
        assert_eq!(db_surface.rows, config.shapes);
        assert_eq!(
            db_surface.writes_applied,
            config.active * config.commits_per_active(coalesced)
        );
        let concurrent = run_concurrent_live(&config, coalesced, profile.clone());
        assert!(concurrent.converged);
        assert_eq!(concurrent.spy_rows, 0);
        assert_eq!(concurrent.spy_updates, 0);
    }
    let topology = topology(&config, profile);
    let mut deterministic = DeterministicDriver::new(topology, config.seed ^ 0x5200_fa11);
    let failure = run_failure(&mut deterministic, &config);
    assert_eq!(failure.spy_rows, 0);
}

#[derive(Clone, Debug)]
struct Config {
    seed: u64,
    profile: String,
    shapes: usize,
    active: usize,
    passive: usize,
    rate_per_sec: usize,
    duration_secs: usize,
    transport_codec: SimulatorTransportCodec,
}

impl Config {
    fn from_env() -> Self {
        let bench_profile = bench_profile();
        Self {
            seed: env_u64("JAZZ_SEED", 0x5200_cafe),
            profile: std::env::var("JAZZ_PROFILE").unwrap_or_else(|_| "s2-local".to_owned()),
            shapes: env_usize("JAZZ_S2_SHAPES", bench_profile.select(8, 20, 40)).max(1),
            active: env_usize("JAZZ_S2_ACTIVE", bench_profile.select(1, 2, 3)).max(1),
            passive: env_usize("JAZZ_S2_PASSIVE", bench_profile.select(1, 2, 3)),
            rate_per_sec: env_usize("JAZZ_S2_RATE", bench_profile.select(2, 4, 5)).max(1),
            duration_secs: env_usize("JAZZ_S2_SECONDS", 1).max(1),
            transport_codec: scenario_transport_codec_env("JAZZ_S2_TRANSPORT_CODEC"),
        }
    }

    fn commits_per_active(&self, coalesced: bool) -> usize {
        if coalesced {
            (self.rate_per_sec * self.duration_secs).min((self.duration_secs * 1_000).div_ceil(16))
        } else {
            self.rate_per_sec * self.duration_secs
        }
    }
}

#[derive(Debug)]
struct LiveSummary {
    commits: usize,
    participants: usize,
    latency: Histogram<u64>,
    wall_receipt: Histogram<u64>,
    core_ingest_done: Histogram<u64>,
    emission_construct: Histogram<u64>,
    link_handoff_to_delivered: Histogram<u64>,
    delivered_to_applied: Histogram<u64>,
    link_one_way_floor_us: u64,
    link_rtt_floor_us: u64,
    bytes_total: u64,
    bytes_floor: u64,
    merge_versions: usize,
    merges_of_merges: usize,
    core_tick: Histogram<u64>,
    history_rows_written: usize,
}

#[derive(Debug)]
struct ConcurrentLiveSummary {
    offered_commits_per_sec: f64,
    achieved_commits_per_sec: f64,
    updates_delivered_per_sec: f64,
    offered_commits: usize,
    accepted_commits: usize,
    updates_delivered: usize,
    participants: usize,
    wall_duration_us: u64,
    local_commit_visibility_us: Histogram<u64>,
    receipt_latency_us: Histogram<u64>,
    merge_versions: usize,
    merges_of_merges: usize,
    history_rows_written: usize,
    core_tick: Histogram<u64>,
    bytes_total: u64,
    bytes_floor: u64,
    transport_metrics: serde_json::Map<String, JsonValue>,
    converged: bool,
    spy_rows: usize,
    spy_updates: usize,
}

#[derive(Debug)]
struct FailureSummary {
    recovery_to_convergence_us: u64,
    final_rows: usize,
    spy_rows: usize,
    disconnected_catchup_bytes: u64,
}

#[derive(Debug)]
struct HistoricalLoadSummary {
    cut_percent: u64,
    position: GlobalTime,
    latency_us: u128,
    rows: usize,
}

#[derive(Debug)]
struct DbSurfaceSummary {
    fixture_rows: usize,
    subscriptions: usize,
    writes_applied: usize,
    watch_changes: usize,
    write_p50_us: u64,
    write_p95_us: u64,
    changed_p50_us: u64,
    changed_p95_us: u64,
    current_p50_us: u64,
    current_p95_us: u64,
    rows: usize,
}

struct Participant {
    name: String,
    node: NodeState<RocksDbStorage>,
    _dir: tempfile::TempDir,
    peer: PeerState,
}

fn run_live(ctx: &mut dyn DriverContext, config: &Config, coalesced: bool) -> LiveSummary {
    let schema = schema();
    let canvas = canvas_id();
    let (_core_dir, mut core) = open_node(node(250), schema.clone());
    let (_writer_dir, mut writer) = open_node(node(1), schema.clone());
    install_participant_claims(&mut core, config);
    install_participant_claims(&mut writer, config);
    seed_fixture(ctx, config, &mut writer, &mut core);

    let (shape, binding) = shape_subscription(&schema, canvas);
    let mut participants = open_participants(config, &schema);
    let mut spy = open_participant(
        "spy",
        node(90),
        schema,
        AuthorSubject::for_test_bytes([0x55; 16]),
    );
    for participant in &mut participants {
        hydrate(ctx, &mut core, participant, &shape, &binding);
    }
    hydrate(ctx, &mut core, &mut spy, &shape, &binding);
    assert!(rows(&mut spy.node, &shape, &binding).is_empty());

    let mut rng = Lcg::new(config.seed ^ u64::from(coalesced));
    let mut latency = Histogram::new(3).unwrap();
    let mut wall_receipt = Histogram::new(3).unwrap();
    let mut core_ingest_done = Histogram::new(3).unwrap();
    let mut emission_construct = Histogram::new(3).unwrap();
    let mut link_handoff_to_delivered = Histogram::new(3).unwrap();
    let mut delivered_to_applied = Histogram::new(3).unwrap();
    let mut core_tick = Histogram::new(3).unwrap();
    let mut bytes_total = 0_u64;
    let mut floor_bytes = 0_u64;
    let mut commits = 0_usize;
    let per_active = config.commits_per_active(coalesced);
    let mut pending_receives = Vec::with_capacity(participants.len());
    for _step in 0..per_active {
        for active_idx in 0..config.active {
            let shape_idx = zipf_index(&mut rng, config.shapes);
            let row_uuid = shape_row(shape_idx);
            let x = (rng.next_u64() % 10_000) as f64 / 10.0;
            let y = (rng.next_u64() % 10_000) as f64 / 10.0;
            let start_ms = ctx.now_ms();
            let submit_at = Instant::now();
            let commit = MergeableCommit::new(SHAPES, row_uuid, 10_000 + commits as u64)
                .made_by(participant_author(active_idx))
                .cells(shape_cells(canvas, shape_idx, x, y));
            let (tx_id, unit) =
                commit_mergeable_unit_settled(&mut participants[active_idx].node, commit).unwrap();
            ctx.send(&participants[active_idx].name, "core", unit);
            let delivered_to_core = ctx.recv("core");
            let SyncMessage::CommitUnit { tx, versions } = delivered_to_core.message else {
                unreachable!();
            };
            let core_start = Instant::now();
            let fates =
                ingest_commit_unit_settled(&mut core, tx, versions, u64::MAX).expect("core ingest");
            for fate in fates {
                ctx.send("core", &participants[active_idx].name, fate);
                let delivered = ctx.recv(&participants[active_idx].name);
                apply_sync_message_settled(&mut participants[active_idx].node, delivered.message)
                    .unwrap();
            }
            assert_eq!(
                block_on(participants[active_idx].node.transaction_state(tx_id))
                    .unwrap()
                    .0,
                Fate::Accepted
            );
            core_ingest_done
                .record(submit_at.elapsed().as_micros() as u64)
                .unwrap();
            core_tick
                .record(core_start.elapsed().as_micros() as u64)
                .unwrap();
            commits += 1;

            pending_receives.clear();
            for (idx, participant) in participants.iter_mut().enumerate() {
                let emit_start = Instant::now();
                let update = block_on(participant.peer.query_update(&mut core, &shape, &binding))
                    .expect("participant update");
                let emit_elapsed = emit_start.elapsed().as_micros() as u64;
                emission_construct.record(emit_elapsed).unwrap();
                bytes_total += view_update_bytes(&update);
                floor_bytes += bytes_floor(&update);
                let sent_at = Instant::now();
                ctx.send("core", &participant.name, update);
                pending_receives.push((idx, sent_at));
            }
            for &(idx, sent_at) in &pending_receives {
                let delivered = ctx.recv(&participants[idx].name);
                link_handoff_to_delivered
                    .record(sent_at.elapsed().as_micros() as u64)
                    .unwrap();
                let participant = &mut participants[idx];
                debug_assert_eq!(delivered.to, participant.name);
                let apply_start = Instant::now();
                apply_sync_message_settled(&mut participant.node, delivered.message)
                    .expect("participant apply");
                delivered_to_applied
                    .record(apply_start.elapsed().as_micros() as u64)
                    .unwrap();
                if idx != active_idx {
                    wall_receipt
                        .record(submit_at.elapsed().as_micros() as u64)
                        .expect("wall receipt sample");
                    latency
                        .record((ctx.now_ms() - start_ms) * 1_000)
                        .expect("latency sample");
                }
            }
        }
    }
    for participant in &mut participants {
        let update = block_on(
            participant
                .peer
                .rehydrate_query(&mut core, &shape, &binding),
        )
        .expect("final participant rehydrate");
        bytes_total += view_update_bytes(&update);
        floor_bytes += bytes_floor(&update);
        ctx.send("core", &participant.name, update);
        let delivered = ctx.recv(&participant.name);
        apply_sync_message_settled(&mut participant.node, delivered.message)
            .expect("final participant apply");
    }
    let expected = shape_state(&mut core);
    for participant in &mut participants {
        assert_eq!(shape_state(&mut participant.node), expected);
    }
    assert!(rows(&mut spy.node, &shape, &binding).is_empty());
    let (merge_versions, merges_of_merges) = merge_counters(&mut core, config.shapes);
    LiveSummary {
        commits,
        participants: participants.len(),
        latency,
        wall_receipt,
        core_ingest_done,
        emission_construct,
        link_handoff_to_delivered,
        delivered_to_applied,
        link_one_way_floor_us: env_u64("JAZZ_LINK_ONE_WAY_MS", 1) * 1_000,
        link_rtt_floor_us: 2 * env_u64("JAZZ_LINK_ONE_WAY_MS", 1) * 1_000,
        bytes_total,
        bytes_floor: floor_bytes,
        merge_versions,
        merges_of_merges,
        core_tick,
        history_rows_written: config.shapes + commits + merge_versions,
    }
}

#[derive(Clone, Copy, Debug)]
struct ClientTiers {
    read_tier: DurabilityTier,
    write_wait_tier: DurabilityTier,
}

impl ClientTiers {
    fn realtime_canvas() -> Self {
        Self {
            read_tier: DurabilityTier::None,
            write_wait_tier: DurabilityTier::None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct LinkDurations {
    client_core: Duration,
}

#[derive(Clone, Copy, Debug)]
struct WorkItem {
    shape_idx: usize,
    x: f64,
    y: f64,
}

enum CoreInbound {
    Commit {
        writer_idx: usize,
        deliver_at: Instant,
        message: Box<SyncMessage>,
    },
    WriterDone {
        writer_idx: usize,
    },
}

enum ReaderInbound {
    Update {
        deliver_at: Instant,
        message: Box<SyncMessage>,
    },
    Done,
}

struct WriterFate {
    message: SyncMessage,
}

struct ReaderCorePeer {
    peer: PeerState,
}

struct ReaderActorArgs {
    name: String,
    is_spy: bool,
    read_tier: DurabilityTier,
    _dir: tempfile::TempDir,
    node: NodeState<RocksDbStorage>,
    reader_rx: mpsc::Receiver<ReaderInbound>,
    epoch: Instant,
    shape: ValidatedQuery,
    binding: Binding,
}

struct WriterResult {
    submitted_commits: usize,
    local_commit_visibility_us: Histogram<u64>,
}

struct CoreResult {
    bytes_total: u64,
    bytes_floor: u64,
    accepted_commits: usize,
    core_tick: Histogram<u64>,
    merge_versions: usize,
    merges_of_merges: usize,
    history_rows_written: usize,
    state: BTreeMap<RowUuid, (u64, u64)>,
}

struct ReaderResult {
    name: String,
    is_spy: bool,
    rows: usize,
    updates_delivered: usize,
    receipt_latency_us: Histogram<u64>,
    state: BTreeMap<RowUuid, (u64, u64)>,
}

struct NoopContext {
    start: Instant,
}

impl DriverContext for NoopContext {
    fn driver_name(&self) -> &'static str {
        "s2-concurrent-setup"
    }

    fn now_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    fn send(&mut self, _from: &str, _to: &str, _message: SyncMessage) {}

    fn recv(&mut self, node: &str) -> jazz_sim::DeliveredMessage {
        panic!("setup context has no receiver for {node}");
    }

    fn record_latency(&mut self, _metric: &str, _micros: u64) {}

    fn record_counter(&mut self, _metric: &str, _value: u64) {}
}

fn run_concurrent_live(
    config: &Config,
    coalesced: bool,
    profile: PeerProfile,
) -> ConcurrentLiveSummary {
    let schema = schema();
    let canvas = canvas_id();
    let (shape, binding) = shape_subscription(&schema, canvas);
    let tiers = ClientTiers::realtime_canvas();
    let links = link_durations(&profile);
    let transport_codec = config.transport_codec;
    let transport_metrics = Arc::new(Mutex::new(Metrics::default()));
    let workload = precompute_workload(config, coalesced);
    let epoch = Instant::now();
    let offered_commits = workload.iter().map(Vec::len).sum::<usize>();
    let offered_commits_per_sec = offered_commits as f64 / config.duration_secs as f64;
    let active_count = config.active;
    let shapes_count = config.shapes;
    let duration_secs = config.duration_secs;
    let rate_per_sec = config.rate_per_sec;
    let writer_write_wait_tiers = vec![tiers.write_wait_tier; config.active];

    let mut setup_ctx = NoopContext {
        start: Instant::now(),
    };
    let (core_dir, mut core) = open_node(node(250), schema.clone());
    let (_fixture_writer_dir, mut fixture_writer) = open_node(node(1), schema.clone());
    install_participant_claims(&mut core, config);
    install_participant_claims(&mut fixture_writer, config);
    seed_concurrent_fixture(&mut setup_ctx, config, &mut fixture_writer, &mut core);

    apply_core_binding(&mut core, &shape, &binding);
    let mut writer_nodes = Vec::with_capacity(config.active);
    for writer_idx in 0..config.active {
        let (dir, mut writer_node) = open_node(node(20 + writer_idx as u8), schema.clone());
        install_claims(&mut writer_node, participant_author(writer_idx));
        apply_binding(&mut writer_node, &shape, &binding, participant_author(0));
        writer_nodes.push((dir, writer_node));
    }

    let mut reader_nodes = Vec::with_capacity(config.passive + 1);
    let mut passive_reader_positions = Vec::with_capacity(config.passive);
    for passive_idx in 0..config.passive {
        let participant_idx = config.active + passive_idx;
        let (dir, mut reader_node) = open_node(node(20 + participant_idx as u8), schema.clone());
        install_claims(&mut reader_node, participant_author(participant_idx));
        apply_binding(&mut reader_node, &shape, &binding, participant_author(0));
        passive_reader_positions.push(reader_nodes.len());
        reader_nodes.push((
            format!("p{participant_idx}"),
            false,
            tiers.read_tier,
            node(20 + participant_idx as u8),
            dir,
            reader_node,
        ));
    }

    let mut initial_core_peer = PeerState::client_link(participant_author(0));
    let invited_core_update =
        block_on(initial_core_peer.rehydrate_query(&mut core, &shape, &binding))
            .expect("invited core rehydrate");
    let writer_initial_update = invited_core_update;
    for (_, node) in &mut writer_nodes {
        apply_sync_message_settled(node, writer_initial_update.clone())
            .expect("writer initial apply");
    }

    let mut reader_core_peers = Vec::with_capacity(config.passive);
    for position in &passive_reader_positions {
        let mut core_peer = PeerState::client_link(participant_author(0));
        let reader_initial_update =
            block_on(core_peer.rehydrate_query(&mut core, &shape, &binding))
                .expect("core reader initial rehydrate");
        reader_core_peers.push(ReaderCorePeer { peer: core_peer });
        let (_, _, _, _, _, node) = &mut reader_nodes[*position];
        apply_sync_message_settled(node, reader_initial_update).expect("reader initial apply");
    }

    let (spy_dir, mut spy_node) = open_node(node(90), schema.clone());
    let spy_author = participant_author(90);
    install_claims(&mut core, spy_author);
    apply_binding(&mut spy_node, &shape, &binding, spy_author);
    let mut spy_peer = PeerState::client_link(spy_author);
    let spy_initial = block_on(spy_peer.rehydrate_query(&mut core, &shape, &binding)).unwrap();
    apply_sync_message_settled(&mut spy_node, spy_initial).unwrap();
    assert!(rows(&mut spy_node, &shape, &binding).is_empty());
    reader_core_peers.push(ReaderCorePeer { peer: spy_peer });
    reader_nodes.push((
        "spy".to_owned(),
        true,
        tiers.read_tier,
        node(90),
        spy_dir,
        spy_node,
    ));

    let (core_tx, core_rx) = mpsc::channel::<CoreInbound>();
    let mut reader_txs = Vec::with_capacity(reader_nodes.len());
    let mut reader_handles = Vec::with_capacity(reader_nodes.len());
    while epoch.elapsed().as_millis() == 0 {
        thread::yield_now();
    }

    for (reader_idx, (name, is_spy, read_tier, node_uuid, dir, node)) in
        reader_nodes.into_iter().enumerate()
    {
        let (reader_tx, reader_rx) = mpsc::channel::<ReaderInbound>();
        reader_txs.push(reader_tx);
        let reader_shape = shape.clone();
        let reader_binding = binding.clone();
        let reader_schema = schema.clone();
        drop(node);
        reader_handles.push(thread::spawn(move || {
            let mut node = reopen_node(&dir, node_uuid, reader_schema);
            apply_binding(
                &mut node,
                &reader_shape,
                &reader_binding,
                if is_spy {
                    participant_author(90)
                } else {
                    participant_author(0)
                },
            );
            run_reader_actor(ReaderActorArgs {
                name,
                is_spy,
                read_tier,
                _dir: dir,
                node,
                reader_rx,
                epoch,
                shape: reader_shape,
                binding: reader_binding,
            })
        }));
        debug_assert_eq!(reader_idx + 1, reader_txs.len());
    }

    let mut writer_fate_txs = Vec::with_capacity(config.active);
    let mut writer_fate_rxs = Vec::with_capacity(config.active);
    for _ in 0..config.active {
        let (tx, rx) = mpsc::channel::<WriterFate>();
        writer_fate_txs.push(tx);
        writer_fate_rxs.push(rx);
    }

    let core_shape = shape.clone();
    let core_binding = binding.clone();
    drop(core);
    let core_handle = thread::spawn({
        let writer_fate_txs = writer_fate_txs.clone();
        let transport_metrics = Arc::clone(&transport_metrics);
        let core_schema = schema.clone();
        move || {
            let mut core = reopen_node(&core_dir, node(250), core_schema);
            for writer_idx in 0..active_count {
                install_claims(&mut core, participant_author(writer_idx));
            }
            apply_core_binding(&mut core, &core_shape, &core_binding);
            run_core_actor(
                core_dir,
                core,
                reader_core_peers,
                core_rx,
                reader_txs,
                writer_fate_txs,
                core_shape,
                core_binding,
                links,
                transport_codec,
                transport_metrics,
                epoch,
                shapes_count,
                active_count,
            )
        }
    });

    let start = Instant::now();
    let mut writer_handles = Vec::with_capacity(config.active);
    for (writer_idx, ((dir, writer_node), items)) in
        writer_nodes.into_iter().zip(workload).enumerate()
    {
        let tx = core_tx.clone();
        let fate_rx = writer_fate_rxs.remove(0);
        let transport_metrics = Arc::clone(&transport_metrics);
        let writer_shape = shape.clone();
        let writer_binding = binding.clone();
        let writer_schema = schema.clone();
        drop(writer_node);
        let write_wait_tier = writer_write_wait_tiers[writer_idx];
        writer_handles.push(thread::spawn(move || {
            let mut node = reopen_node(&dir, node(20 + writer_idx as u8), writer_schema);
            install_claims(&mut node, participant_author(writer_idx));
            apply_binding(
                &mut node,
                &writer_shape,
                &writer_binding,
                participant_author(0),
            );
            run_writer_actor(
                writer_idx,
                dir,
                node,
                items,
                tx,
                fate_rx,
                write_wait_tier,
                links,
                transport_codec,
                transport_metrics,
                epoch,
                start,
                Duration::from_secs(duration_secs as u64),
                rate_per_sec,
                writer_shape,
                writer_binding,
            )
        }));
    }
    drop(core_tx);
    drop(writer_fate_txs);

    let mut local_commit_visibility_us = Histogram::new(3).unwrap();
    let mut submitted_commits = 0;
    for handle in writer_handles {
        let result = handle.join().expect("writer actor joined");
        submitted_commits += result.submitted_commits;
        merge_histogram(
            &mut local_commit_visibility_us,
            &result.local_commit_visibility_us,
        );
    }

    let core_result = core_handle.join().expect("core actor joined");

    let mut receipt_latency_us = Histogram::new(3).unwrap();
    let mut updates_delivered = 0_usize;
    let mut converged = true;
    let mut spy_rows = 0_usize;
    let mut spy_updates = 0_usize;
    for handle in reader_handles {
        let result = handle.join().expect("reader actor joined");
        merge_histogram(&mut receipt_latency_us, &result.receipt_latency_us);
        updates_delivered += result.updates_delivered;
        if result.is_spy {
            spy_rows = result.rows;
            spy_updates = result.updates_delivered;
            converged &= result.rows == 0 && result.updates_delivered == 0;
        } else if result.state != core_result.state {
            converged = false;
        }
        let _ = result.name;
    }
    assert!(converged, "concurrent threaded readers converged");
    assert_eq!(spy_rows, 0, "spy materialized no rows");
    assert_eq!(spy_updates, 0, "spy observed no updates");

    let wall_duration = start.elapsed();
    let wall_secs = wall_duration.as_secs_f64().max(f64::EPSILON);
    let accepted_commits = core_result.accepted_commits;
    assert_eq!(
        accepted_commits, submitted_commits,
        "every submitted write reaches Core"
    );
    ConcurrentLiveSummary {
        offered_commits_per_sec,
        achieved_commits_per_sec: accepted_commits as f64 / wall_secs,
        updates_delivered_per_sec: updates_delivered as f64 / wall_secs,
        offered_commits,
        accepted_commits,
        updates_delivered,
        participants: config.passive,
        wall_duration_us: wall_duration.as_micros() as u64,
        local_commit_visibility_us,
        receipt_latency_us,
        merge_versions: core_result.merge_versions,
        merges_of_merges: core_result.merges_of_merges,
        history_rows_written: core_result.history_rows_written,
        core_tick: core_result.core_tick,
        bytes_total: core_result.bytes_total,
        bytes_floor: core_result.bytes_floor,
        transport_metrics: transport_metrics
            .lock()
            .expect("transport metrics lock")
            .to_json_fields(),
        converged,
        spy_rows,
        spy_updates,
    }
}

#[allow(clippy::too_many_arguments)]
fn run_writer_actor(
    writer_idx: usize,
    _dir: tempfile::TempDir,
    mut node: NodeState<RocksDbStorage>,
    items: Vec<WorkItem>,
    core_tx: mpsc::Sender<CoreInbound>,
    fate_rx: mpsc::Receiver<WriterFate>,
    write_wait_tier: DurabilityTier,
    links: LinkDurations,
    transport_codec: SimulatorTransportCodec,
    transport_metrics: SharedMetrics,
    epoch: Instant,
    start: Instant,
    duration: Duration,
    rate_per_sec: usize,
    _shape: ValidatedQuery,
    _binding: Binding,
) -> WriterResult {
    let mut submitted_commits = 0;
    let mut local_commit_visibility_us = Histogram::new(3).unwrap();
    let slot_nanos = (1_000_000_000_u128 / rate_per_sec.max(1) as u128).max(1);
    for (step, item) in items.into_iter().enumerate() {
        let deadline = start + Duration::from_nanos((step as u128 * slot_nanos) as u64);
        park_until(deadline);
        if Instant::now().duration_since(start) >= duration + Duration::from_millis(1) {
            break;
        }
        let row_uuid = shape_row(item.shape_idx);
        let intent = Instant::now();
        let made_at = epoch.elapsed().as_millis() as u64;
        let commit = MergeableCommit::new(SHAPES, row_uuid, made_at)
            .made_by(participant_author(writer_idx))
            .cells(shape_cells(canvas_id(), item.shape_idx, item.x, item.y));
        let (tx_id, unit) =
            commit_mergeable_unit_settled(&mut node, commit).expect("writer commit");
        local_commit_visibility_us
            .record(intent.elapsed().as_micros() as u64)
            .expect("local visibility sample");
        core_tx
            .send(CoreInbound::Commit {
                writer_idx,
                deliver_at: Instant::now() + links.client_core,
                message: Box::new(transport_loopback(
                    transport_codec,
                    unit,
                    &transport_metrics,
                )),
            })
            .expect("core actor open");
        submitted_commits += 1;
        await_write_tier(&mut node, tx_id, write_wait_tier, &fate_rx);
    }
    park_until(start + duration);
    let _ = core_tx.send(CoreInbound::WriterDone { writer_idx });
    WriterResult {
        submitted_commits,
        local_commit_visibility_us,
    }
}

#[allow(clippy::too_many_arguments)]
fn run_core_actor(
    _dir: tempfile::TempDir,
    mut core: NodeState<RocksDbStorage>,
    mut reader_peers: Vec<ReaderCorePeer>,
    core_rx: mpsc::Receiver<CoreInbound>,
    reader_txs: Vec<mpsc::Sender<ReaderInbound>>,
    writer_fate_txs: Vec<mpsc::Sender<WriterFate>>,
    shape: ValidatedQuery,
    binding: Binding,
    links: LinkDurations,
    transport_codec: SimulatorTransportCodec,
    transport_metrics: SharedMetrics,
    epoch: Instant,
    shapes: usize,
    writer_count: usize,
) -> CoreResult {
    let mut writer_done = vec![false; writer_count];
    let mut bytes_total = 0;
    let mut bytes_floor_total = 0;
    let mut accepted_commits = 0_usize;
    let mut core_tick = Histogram::new(3).unwrap();
    while let Ok(message) = core_rx.recv() {
        match message {
            CoreInbound::Commit {
                writer_idx,
                deliver_at,
                message,
            } => {
                park_until(deliver_at);
                let SyncMessage::CommitUnit { tx, versions } = *message else {
                    unreachable!("writer sends commit units");
                };
                let start = Instant::now();
                let updates = ingest_commit_unit_settled(
                    &mut core,
                    tx,
                    versions,
                    epoch.elapsed().as_millis() as u64,
                )
                .expect("core ingest");
                core_tick
                    .record(start.elapsed().as_micros() as u64)
                    .expect("core tick sample");
                for update in updates {
                    if global_fate_update_accepted(writer_idx, &update, &writer_fate_txs) {
                        accepted_commits += 1;
                    }
                }
                for (reader_idx, reader) in reader_peers.iter_mut().enumerate() {
                    let update = block_on(reader.peer.query_update(&mut core, &shape, &binding))
                        .expect("core reader query update");
                    bytes_total += view_update_bytes(&update);
                    bytes_floor_total += bytes_floor(&update);
                    let _ = reader_txs[reader_idx].send(ReaderInbound::Update {
                        deliver_at: Instant::now() + links.client_core,
                        message: Box::new(transport_loopback(
                            transport_codec,
                            update,
                            &transport_metrics,
                        )),
                    });
                }
            }
            CoreInbound::WriterDone { writer_idx } => {
                writer_done[writer_idx] = true;
                if !writer_done.iter().all(|done| *done) {
                    continue;
                }
                for (reader_idx, reader) in reader_peers.iter_mut().enumerate() {
                    let update = block_on(reader.peer.rehydrate_query(&mut core, &shape, &binding))
                        .expect("core final reader rehydrate");
                    bytes_total += view_update_bytes(&update);
                    bytes_floor_total += bytes_floor(&update);
                    let _ = reader_txs[reader_idx].send(ReaderInbound::Update {
                        deliver_at: Instant::now() + links.client_core,
                        message: Box::new(transport_loopback(
                            transport_codec,
                            update,
                            &transport_metrics,
                        )),
                    });
                }
                for reader in &reader_txs {
                    let _ = reader.send(ReaderInbound::Done);
                }
                break;
            }
        }
    }
    let (merge_versions, merges_of_merges) = merge_counters(&mut core, shapes);
    let state = shape_state(&mut core);
    CoreResult {
        bytes_total,
        bytes_floor: bytes_floor_total,
        accepted_commits,
        core_tick,
        merge_versions,
        merges_of_merges,
        history_rows_written: shapes + accepted_commits + merge_versions,
        state,
    }
}

fn run_reader_actor(args: ReaderActorArgs) -> ReaderResult {
    let ReaderActorArgs {
        name,
        is_spy,
        read_tier,
        _dir,
        mut node,
        reader_rx,
        epoch,
        shape,
        binding,
    } = args;
    let mut receipt_latency_us = Histogram::new(3).unwrap();
    let mut updates_delivered = 0_usize;
    let mut observed_tx_ids = std::collections::BTreeSet::new();
    while let Ok(message) = reader_rx.recv() {
        match message {
            ReaderInbound::Update {
                deliver_at,
                message,
            } => {
                park_until(deliver_at);
                let update_tx_ids = observed_shape_tx_ids(&message, read_tier);
                apply_sync_message_settled(&mut node, *message).expect("reader apply update");
                let now_ms = epoch.elapsed().as_millis() as u64;
                for tx_id in update_tx_ids {
                    if !observed_tx_ids.insert(tx_id) {
                        continue;
                    }
                    let made_at = tx_id.physical_ms();
                    if made_at == 0 {
                        continue;
                    }
                    let latency_us = now_ms.saturating_sub(made_at) * 1_000;
                    receipt_latency_us
                        .record(latency_us)
                        .expect("receipt latency sample");
                    updates_delivered += 1;
                }
            }
            ReaderInbound::Done => break,
        }
    }
    let state = shape_state(&mut node);
    let rows = rows(&mut node, &shape, &binding).len();
    ReaderResult {
        name,
        is_spy,
        rows,
        updates_delivered,
        receipt_latency_us,
        state,
    }
}

fn precompute_workload(config: &Config, coalesced: bool) -> Vec<Vec<WorkItem>> {
    let mut rng = Lcg::new(config.seed ^ u64::from(coalesced));
    let mut per_writer = vec![Vec::new(); config.active];
    for _step in 0..config.commits_per_active(coalesced) {
        for writer_items in per_writer.iter_mut().take(config.active) {
            let shape_idx = zipf_index(&mut rng, config.shapes);
            let x = (rng.next_u64() % 10_000) as f64 / 10.0;
            let y = (rng.next_u64() % 10_000) as f64 / 10.0;
            writer_items.push(WorkItem { shape_idx, x, y });
        }
    }
    per_writer
}

fn apply_core_binding(
    core: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
) {
    apply_sync_message_settled(
        core,
        SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(shape),
            opts: RegisterShapeOptions::default(),
        },
    )
    .unwrap();
    let values = shape
        .params()
        .keys()
        .map(|name| binding.values().get(name).cloned().unwrap())
        .collect();
    apply_sync_message_settled(
        core,
        SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription: SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
                read_view: RegisterShapeOptions::default().read_view_key(),
            },
            values,
            known_state: None,
            delegated_session: None,
        }),
    )
    .unwrap();
}

fn link_durations(profile: &PeerProfile) -> LinkDurations {
    let latency_ms = client_core_latency_ms();
    LinkDurations {
        client_core: Duration::from_millis(latency_ms + profile.per_message_overhead_ms),
    }
}

fn global_fate_update_accepted(
    writer_idx: usize,
    update: &SyncMessage,
    writer_fate_txs: &[mpsc::Sender<WriterFate>],
) -> bool {
    if matches!(update, SyncMessage::FateUpdate { .. }) {
        let _ = writer_fate_txs[writer_idx].send(WriterFate {
            message: update.clone(),
        });
    }
    matches!(
        update,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            durability,
            ..
        } if durability.is_some_and(|tier| tier >= DurabilityTier::Global)
    )
}

fn observed_shape_tx_ids(update: &SyncMessage, read_tier: DurabilityTier) -> Vec<jazz::tx::TxId> {
    if !observed_at_read_tier(update, read_tier) {
        return Vec::new();
    }
    match update {
        SyncMessage::ViewUpdate(jazz::protocol::ViewUpdatePayload {
            supporting_rows, ..
        }) => supporting_rows
            .added_rows()
            .iter()
            .filter(|input| input.version_table.as_str() == SHAPES)
            .map(|input| input.version.tx)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
        _ => Vec::new(),
    }
}

fn observed_at_read_tier(update: &SyncMessage, tier: DurabilityTier) -> bool {
    if tier == DurabilityTier::None {
        return true;
    }
    match update {
        SyncMessage::ViewUpdate(jazz::protocol::ViewUpdatePayload {
            version_carriers, ..
        }) => version_bundle_refs(version_carriers)
            .any(|bundle| bundle.durability >= tier && matches!(bundle.fate, Fate::Accepted)),
        SyncMessage::FateUpdate { durability, .. } => durability.is_some_and(|seen| seen >= tier),
        _ => false,
    }
}

fn await_write_tier(
    node: &mut NodeState<RocksDbStorage>,
    tx_id: jazz::tx::TxId,
    tier: DurabilityTier,
    fate_rx: &mpsc::Receiver<WriterFate>,
) {
    match tier {
        DurabilityTier::None | DurabilityTier::Local => return,
        DurabilityTier::Global => {}
    }
    while let Ok(update) = fate_rx.recv() {
        let matches_tier = match &update.message {
            SyncMessage::FateUpdate {
                tx_id: seen,
                fate: Fate::Accepted,
                durability,
                ..
            } => *seen == tx_id && durability.is_some_and(|seen| seen >= tier),
            SyncMessage::FateUpdate { tx_id: seen, .. } => *seen == tx_id,
            _ => false,
        };
        apply_sync_message_settled(node, update.message).expect("writer apply fate update");
        if matches_tier {
            return;
        }
    }
}

fn park_until(deadline: Instant) {
    let now = Instant::now();
    if deadline > now {
        thread::park_timeout(deadline - now);
    }
}

fn merge_histogram(target: &mut Histogram<u64>, source: &Histogram<u64>) {
    target.add(source).expect("histogram merge");
}

fn run_historical_loads(
    ctx: &mut dyn DriverContext,
    config: &Config,
    coalesced: bool,
) -> Vec<HistoricalLoadSummary> {
    let schema = schema();
    let canvas = canvas_id();
    let (_core_dir, mut core) = open_history_complete_node(node(250), schema.clone());
    let (_writer_dir, mut writer) = open_node(node(1), schema.clone());
    install_participant_claims(&mut core, config);
    install_participant_claims(&mut writer, config);
    seed_fixture(ctx, config, &mut writer, &mut core);
    let (shape, binding) = shape_subscription(&schema, canvas);
    let table = schema
        .tables
        .iter()
        .find(|table| table.name == SHAPES)
        .unwrap()
        .clone();

    let total_commits = config.active * config.commits_per_active(coalesced);
    let cut_targets = [25_u64, 50, 75]
        .into_iter()
        .map(|percent| {
            let target = ((total_commits as u64 * percent).max(1)).div_ceil(100);
            (percent, target as usize)
        })
        .collect::<Vec<_>>();
    let mut next_cut = 0_usize;
    let mut summaries = Vec::new();
    let mut expected = (0..config.shapes)
        .map(|idx| {
            (
                shape_row(idx),
                ((idx as f64).to_bits(), (idx as f64).to_bits()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut rng = Lcg::new(config.seed ^ u64::from(coalesced));
    let per_active = config.commits_per_active(coalesced);
    let mut commits = 0_usize;

    for _step in 0..per_active {
        for active_idx in 0..config.active {
            let shape_idx = zipf_index(&mut rng, config.shapes);
            let row_uuid = shape_row(shape_idx);
            let x = (rng.next_u64() % 10_000) as f64 / 10.0;
            let y = (rng.next_u64() % 10_000) as f64 / 10.0;
            let commit = MergeableCommit::new(SHAPES, row_uuid, 10_000 + commits as u64)
                .made_by(participant_author(active_idx))
                .cells(shape_cells(canvas, shape_idx, x, y));
            let (_tx_id, unit) = commit_mergeable_unit_settled(&mut writer, commit).unwrap();
            let SyncMessage::CommitUnit { tx, versions } = unit else {
                unreachable!();
            };
            let tx_id = tx.tx_id;
            let updates =
                ingest_commit_unit_settled(&mut core, tx, versions, u64::MAX).expect("core ingest");
            for update in updates {
                apply_sync_message_settled(&mut writer, update).unwrap();
            }
            commits += 1;
            expected.insert(row_uuid, (x.to_bits(), y.to_bits()));

            while next_cut < cut_targets.len() && commits >= cut_targets[next_cut].1 {
                let (percent, _) = cut_targets[next_cut];
                let position = block_on(core.transaction_record(tx_id))
                    .unwrap()
                    .global_time
                    .unwrap();
                let start = Instant::now();
                let rows = match block_on(core.at(position).read(&shape, &binding)) {
                    Ok(rows) => rows,
                    Err(error) => {
                        if is_known_historical_implicit_include_coverage_gap(&error) {
                            emit_historical_load_gate(coalesced, config, &error);
                        } else {
                            panic!("unexpected historical load error: {error:?}");
                        }
                        return summaries;
                    }
                };
                let latency_us = start.elapsed().as_micros();
                let actual = rows
                    .into_iter()
                    .map(|row| {
                        let x = match row.cell(&table, "x").unwrap() {
                            Value::F64(value) => value.to_bits(),
                            other => panic!("unexpected x {other:?}"),
                        };
                        let y = match row.cell(&table, "y").unwrap() {
                            Value::F64(value) => value.to_bits(),
                            other => panic!("unexpected y {other:?}"),
                        };
                        (row.row_uuid(), (x, y))
                    })
                    .collect::<BTreeMap<_, _>>();
                assert_eq!(actual, expected, "historical load at {percent}%");
                summaries.push(HistoricalLoadSummary {
                    cut_percent: percent,
                    position,
                    latency_us,
                    rows: actual.len(),
                });
                next_cut += 1;
            }
        }
    }
    summaries
}

fn run_db_surface(config: &Config, coalesced: bool) -> DbSurfaceSummary {
    let schema = schema();
    let canvas = canvas_id();
    let (_dir, db) = open_db(node(70), participant_author(0), schema.clone());

    let canvas_write = block_on(db.insert(
        CANVASES,
        canvas_cells(),
        jazz::db::InsertOptions {
            row_id: Some(canvas),
            ..Default::default()
        },
    ))
    .expect("db canvas insert");
    block_on(canvas_write.wait(DurabilityTier::Local)).expect("db canvas local wait");
    for idx in 0..(config.active + config.passive) {
        let invite = block_on(db.insert(
            INVITES,
            BTreeMap::from([
                ("canvas".to_owned(), Value::Uuid(canvas.0)),
                (
                    "userID".to_owned(),
                    Value::String(participant_author(idx).principal_parts().1),
                ),
            ]),
            jazz::db::InsertOptions {
                row_id: Some(row(10_000 + idx)),
                ..Default::default()
            },
        ))
        .expect("db invite insert");
        block_on(invite.wait(DurabilityTier::Local)).expect("db invite local wait");
    }

    let mut shape_rows = Vec::with_capacity(config.shapes);
    let mut expected = BTreeMap::new();
    for idx in 0..config.shapes {
        let write = block_on(db.insert(
            SHAPES,
            shape_cells(canvas, idx, idx as f64, idx as f64),
            Default::default(),
        ))
        .expect("db shape insert");
        let row_uuid = write.row_uuid();
        block_on(write.wait(DurabilityTier::Local)).expect("db shape local wait");
        shape_rows.push(row_uuid);
        expected.insert(row_uuid, ((idx as f64).to_bits(), (idx as f64).to_bits()));
    }

    let query = db_canvas_query(canvas);
    let prepared_query = db.prepare_query(&query).expect("db prepare query");
    let mut watches = (0..(config.active + config.passive))
        .map(|_| {
            block_on(db.subscribe(&prepared_query, ReadOpts::default())).expect("db subscribe")
        })
        .collect::<Vec<_>>();
    let mut watch_rows = Vec::with_capacity(watches.len());
    for watch in &mut watches {
        let mut rows = BTreeMap::new();
        apply_db_subscription_event(
            &mut rows,
            block_on(watch.next_event()).expect("db subscription opens"),
        );
        assert_eq!(rows.len(), config.shapes);
        watch_rows.push(rows);
    }
    let spy_query = db_canvas_query(row(99_999));
    let prepared_spy_query = db.prepare_query(&spy_query).expect("db prepare spy query");
    let mut spy_watch =
        block_on(db.subscribe(&prepared_spy_query, ReadOpts::default())).expect("db spy watch");
    let mut spy_rows = BTreeMap::new();
    apply_db_subscription_event(
        &mut spy_rows,
        block_on(spy_watch.next_event()).expect("db spy subscription opens"),
    );
    assert!(spy_rows.is_empty());

    let mut rng = Lcg::new(config.seed ^ u64::from(coalesced));
    let per_active = config.commits_per_active(coalesced);
    let mut write_latencies = Vec::new();
    let mut changed_latencies = Vec::new();
    let mut current_latencies = Vec::new();
    let mut watch_changes = 0_usize;
    let mut writes_applied = 0_usize;

    for _step in 0..per_active {
        for _active_idx in 0..config.active {
            let shape_idx = zipf_index(&mut rng, config.shapes);
            let row_uuid = shape_rows[shape_idx];
            let x = (rng.next_u64() % 10_000) as f64 / 10.0;
            let y = (rng.next_u64() % 10_000) as f64 / 10.0;
            let patch = BTreeMap::from([
                ("x".to_owned(), Value::F64(x)),
                ("y".to_owned(), Value::F64(y)),
            ]);
            let start = Instant::now();
            let write = block_on(db.update(SHAPES, row_uuid, patch, Default::default()))
                .expect("db shape update");
            block_on(write.wait(DurabilityTier::Local)).expect("db update local wait");
            write_latencies.push(start.elapsed().as_micros() as u64);
            expected.insert(row_uuid, (x.to_bits(), y.to_bits()));
            writes_applied += 1;

            for (watch, rows_by_id) in watches.iter_mut().zip(watch_rows.iter_mut()) {
                let start = Instant::now();
                if let Some(event) = watch.try_next_event() {
                    watch_changes += 1;
                    apply_db_subscription_event(rows_by_id, event);
                }
                changed_latencies.push(start.elapsed().as_micros() as u64);
                let start = Instant::now();
                let rows = rows_by_id.values().cloned().collect::<Vec<_>>();
                current_latencies.push(start.elapsed().as_micros() as u64);
                assert_eq!(db_rows_state(&schema, rows), expected);
            }
            while let Some(event) = spy_watch.try_next_event() {
                apply_db_subscription_event(&mut spy_rows, event);
            }
            assert!(spy_rows.is_empty());
        }
    }

    assert_eq!(db_shape_state(&db, &schema, &query), expected);

    DbSurfaceSummary {
        fixture_rows: 1 + config.active + config.passive + config.shapes,
        subscriptions: watches.len(),
        writes_applied,
        watch_changes,
        write_p50_us: percentile(&mut write_latencies.clone(), 50),
        write_p95_us: percentile(&mut write_latencies, 95),
        changed_p50_us: percentile(&mut changed_latencies.clone(), 50),
        changed_p95_us: percentile(&mut changed_latencies, 95),
        current_p50_us: percentile(&mut current_latencies.clone(), 50),
        current_p95_us: percentile(&mut current_latencies, 95),
        rows: expected.len(),
    }
}

fn run_failure(ctx: &mut dyn DriverContext, config: &Config) -> FailureSummary {
    let schema = schema();
    let canvas = canvas_id();
    let (core_dir, mut core) = open_node(node(250), schema.clone());
    let (_writer_dir, mut writer) = open_node(node(1), schema.clone());
    install_participant_claims(&mut core, config);
    install_participant_claims(&mut writer, config);
    seed_fixture(ctx, config, &mut writer, &mut core);
    let (shape, binding) = shape_subscription(&schema, canvas);
    let mut participant =
        open_participant("reconnect", node(60), schema.clone(), participant_author(0));
    hydrate(ctx, &mut core, &mut participant, &shape, &binding);
    let mut disconnected =
        open_participant("offline", node(61), schema.clone(), participant_author(1));
    hydrate(ctx, &mut core, &mut disconnected, &shape, &binding);
    let mut spy = open_participant(
        "spy",
        node(62),
        schema.clone(),
        AuthorSubject::for_test_bytes([0x44; 16]),
    );
    hydrate(ctx, &mut core, &mut spy, &shape, &binding);

    let start = ctx.now_ms();
    let mut catchup_bytes = 0;
    for idx in 0..(config.active * config.rate_per_sec.min(20)) {
        let row_uuid = shape_row(idx % config.shapes);
        let commit = MergeableCommit::new(SHAPES, row_uuid, 50_000 + idx as u64)
            .made_by(participant_author(0))
            .cells(shape_cells(canvas, idx, idx as f64, (idx * 2) as f64));
        let (_tx_id, unit) = commit_mergeable_unit_settled(&mut participant.node, commit).unwrap();
        let SyncMessage::CommitUnit { tx, versions } = unit else {
            unreachable!();
        };
        ingest_commit_unit_settled(&mut core, tx, versions, u64::MAX).unwrap();
        if idx == 5 {
            drop(core);
            let cfs = schema.column_families();
            let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
            let storage =
                RocksDbStorage::open_with_durability(core_dir.path(), &refs, Durability::WalNoSync)
                    .unwrap();
            core = block_on(NodeState::new_with_shared_test_catalogue(
                node(250),
                schema.clone(),
                storage,
            ))
            .unwrap();
            install_participant_claims(&mut core, config);
        }
    }
    let update = block_on(
        disconnected
            .peer
            .rehydrate_query(&mut core, &shape, &binding),
    )
    .expect("catch up rehydrate");
    catchup_bytes += view_update_bytes(&update);
    ctx.send("core", &disconnected.name, update);
    let delivered = ctx.recv(&disconnected.name);
    apply_sync_message_settled(&mut disconnected.node, delivered.message).unwrap();
    assert_eq!(shape_state(&mut disconnected.node), shape_state(&mut core));
    assert!(rows(&mut spy.node, &shape, &binding).is_empty());
    FailureSummary {
        recovery_to_convergence_us: (ctx.now_ms() - start) * 1_000,
        final_rows: shape_state(&mut core).len(),
        spy_rows: rows(&mut spy.node, &shape, &binding).len(),
        disconnected_catchup_bytes: catchup_bytes,
    }
}

fn schema() -> JazzSchema {
    let shape_kind = PublicColumnType::ScalarEnum {
        name: "shape_type".to_owned(),
        variants: vec!["circle".to_owned(), "rectangle".to_owned()],
    };
    let invite_policy = public_policy_expr::exists(public_policy_expr::table(INVITES).where_(
        public_policy_expr::rel::all_of([
            public_policy_expr::rel::eq_outer("canvas", "canvas"),
            public_policy_expr::rel::eq_session("userID", vec!["user", "identity", "subject"]),
        ]),
    ));
    compile_public_schema(
        SchemaBuilder::new()
            .table(PublicTableSchema::builder(CANVASES).column("name", PublicColumnType::Text))
            .table(
                PublicTableSchema::builder(INVITES)
                    .fk_column("canvas", CANVASES)
                    .column("userID", PublicColumnType::Text),
            )
            .table(
                PublicTableSchema::builder(SHAPES)
                    .fk_column("canvas", CANVASES)
                    .column("type", shape_kind)
                    .column("text", PublicColumnType::Text)
                    .column("x", PublicColumnType::Double)
                    .column("y", PublicColumnType::Double)
                    .policies(all_operation_policies(invite_policy)),
            )
            .build(),
    )
}

fn seed_fixture(
    ctx: &mut dyn DriverContext,
    config: &Config,
    writer: &mut NodeState<RocksDbStorage>,
    core: &mut NodeState<RocksDbStorage>,
) {
    let canvas = canvas_id();
    commit_global(
        ctx,
        writer,
        core,
        CANVASES,
        canvas,
        AuthorSubject::SYSTEM,
        canvas_cells(),
        1,
    );
    for idx in 0..(config.active + config.passive) {
        commit_global(
            ctx,
            writer,
            core,
            INVITES,
            row(10_000 + idx),
            AuthorSubject::SYSTEM,
            BTreeMap::from([
                ("canvas".to_owned(), Value::Uuid(canvas.0)),
                (
                    "userID".to_owned(),
                    Value::String(participant_author(idx).principal_parts().1),
                ),
            ]),
            100 + idx as u64,
        );
    }
    for idx in 0..config.shapes {
        commit_global(
            ctx,
            writer,
            core,
            SHAPES,
            shape_row(idx),
            AuthorSubject::SYSTEM,
            shape_cells(canvas, idx, idx as f64, idx as f64),
            1_000 + idx as u64,
        );
    }
}

fn seed_concurrent_fixture(
    ctx: &mut dyn DriverContext,
    config: &Config,
    writer: &mut NodeState<RocksDbStorage>,
    core: &mut NodeState<RocksDbStorage>,
) {
    let canvas = canvas_id();
    commit_global_at(
        ctx,
        writer,
        core,
        CANVASES,
        canvas,
        AuthorSubject::SYSTEM,
        canvas_cells(),
        0,
    );
    for idx in 0..(config.active + config.passive) {
        commit_global_at(
            ctx,
            writer,
            core,
            INVITES,
            row(10_000 + idx),
            AuthorSubject::SYSTEM,
            BTreeMap::from([
                ("canvas".to_owned(), Value::Uuid(canvas.0)),
                (
                    "userID".to_owned(),
                    Value::String(participant_author(idx).principal_parts().1),
                ),
            ]),
            0,
        );
    }
    for idx in 0..config.shapes {
        commit_global_at(
            ctx,
            writer,
            core,
            SHAPES,
            shape_row(idx),
            AuthorSubject::SYSTEM,
            shape_cells(canvas, idx, idx as f64, idx as f64),
            0,
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn commit_global(
    ctx: &mut dyn DriverContext,
    writer: &mut NodeState<RocksDbStorage>,
    core: &mut NodeState<RocksDbStorage>,
    table: &str,
    row_uuid: RowUuid,
    made_by: AuthorSubject,
    cells: BTreeMap<String, Value>,
    seq: u64,
) {
    commit_global_at(
        ctx,
        writer,
        core,
        table,
        row_uuid,
        made_by,
        cells,
        1_000 + seq,
    );
}

#[allow(clippy::too_many_arguments)]
fn commit_global_at(
    ctx: &mut dyn DriverContext,
    writer: &mut NodeState<RocksDbStorage>,
    core: &mut NodeState<RocksDbStorage>,
    table: &str,
    row_uuid: RowUuid,
    made_by: AuthorSubject,
    cells: BTreeMap<String, Value>,
    made_at: u64,
) {
    let (tx_id, unit) = commit_mergeable_unit_settled(
        writer,
        MergeableCommit::new(table, row_uuid, made_at)
            .made_by(made_by)
            .cells(cells),
    )
    .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        unreachable!();
    };
    ingest_commit_unit_settled(core, tx, versions, u64::MAX).unwrap();
    let _ = tx_id;
    ctx.record_counter("s2_fixture_rows", 1);
}

fn shape_subscription(schema: &JazzSchema, canvas: RowUuid) -> (ValidatedQuery, Binding) {
    let shape = Query::from(SHAPES)
        .filter(eq(col("canvas"), param("canvas")))
        .validate(schema)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "canvas".to_owned(),
            Value::Uuid(canvas.0),
        )]))
        .unwrap();
    (shape, binding)
}

fn hydrate(
    ctx: &mut dyn DriverContext,
    core: &mut NodeState<RocksDbStorage>,
    participant: &mut Participant,
    shape: &ValidatedQuery,
    binding: &Binding,
) {
    install_claims(core, participant.peer.identity());
    register_binding(ctx, core, &participant.name, shape, binding);
    apply_binding(
        &mut participant.node,
        shape,
        binding,
        participant.peer.identity(),
    );
    let update = block_on(participant.peer.rehydrate_query(core, shape, binding)).unwrap();
    ctx.send("core", &participant.name, update);
    let delivered = ctx.recv(&participant.name);
    apply_sync_message_settled(&mut participant.node, delivered.message).unwrap();
}

fn apply_binding(
    node: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
    identity: AuthorSubject,
) {
    apply_sync_message_settled(
        node,
        SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(shape),
            opts: RegisterShapeOptions::default(),
        },
    )
    .unwrap();
    let values = shape
        .params()
        .keys()
        .map(|name| binding.values().get(name).cloned().unwrap())
        .collect();
    apply_sync_message_settled(
        node,
        SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription: SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
                read_view: RegisterShapeOptions::default().read_view_key(),
            },
            values,
            known_state: None,
            delegated_session: Some(jazz::protocol::DelegatedSessionBinding {
                identity,
                claims: if identity == AuthorSubject::SYSTEM {
                    BTreeMap::new()
                } else {
                    raw_claims(identity)
                },
            }),
        }),
    )
    .unwrap();
}

fn register_binding(
    ctx: &mut dyn DriverContext,
    core: &mut NodeState<RocksDbStorage>,
    client: &str,
    shape: &ValidatedQuery,
    binding: &Binding,
) {
    apply_sync_message_settled(
        core,
        SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(shape),
            opts: RegisterShapeOptions::default(),
        },
    )
    .unwrap();
    let values = shape
        .params()
        .keys()
        .map(|name| binding.values().get(name).cloned().unwrap())
        .collect();
    apply_sync_message_settled(
        core,
        SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription: SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
                read_view: RegisterShapeOptions::default().read_view_key(),
            },
            values,
            known_state: None,
            delegated_session: None,
        }),
    )
    .unwrap();
    ctx.record_counter(&format!("s2_registered_{client}"), 1);
}

fn open_participants(config: &Config, schema: &JazzSchema) -> Vec<Participant> {
    (0..(config.active + config.passive))
        .map(|idx| {
            open_participant(
                &format!("p{idx}"),
                node(20 + idx as u8),
                schema.clone(),
                participant_author(idx),
            )
        })
        .collect()
}

fn open_participant(
    name: &str,
    node_uuid: NodeUuid,
    schema: JazzSchema,
    identity: AuthorSubject,
) -> Participant {
    let (dir, mut participant_node) = open_node(node_uuid, schema);
    install_claims(&mut participant_node, identity);
    Participant {
        name: name.to_owned(),
        node: participant_node,
        _dir: dir,
        peer: PeerState::client_link(identity),
    }
}

fn open_node(
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
    let node = block_on(NodeState::new_with_shared_test_catalogue(
        node_uuid, schema, storage,
    ))
    .unwrap();
    (dir, node)
}

fn reopen_node(
    dir: &tempfile::TempDir,
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> NodeState<RocksDbStorage> {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
    block_on(NodeState::new_with_shared_test_catalogue(
        node_uuid, schema, storage,
    ))
    .unwrap()
}

fn open_db(
    node_uuid: NodeUuid,
    author: AuthorSubject,
    schema: JazzSchema,
) -> (tempfile::TempDir, Db<RocksDbStorage>) {
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
    let db = block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: node_uuid,
            author,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(u64::from_le_bytes(
            node_uuid.as_bytes()[..8]
                .try_into()
                .expect("node seed bytes"),
        )))),
    }))
    .expect("db open");
    install_db_claims(&db, author);
    (dir, db)
}

fn open_history_complete_node(
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
    let node = block_on(NodeState::new_history_complete(node_uuid, schema, storage)).unwrap();
    (dir, node)
}

fn rows(
    node: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
) -> Vec<RowUuid> {
    block_on(node.query_rows(shape, binding, DurabilityTier::Global))
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect()
}

fn db_canvas_query(canvas: RowUuid) -> Query {
    Query::from(SHAPES).filter(eq(col("canvas"), lit(Value::Uuid(canvas.0))))
}

fn db_shape_state(
    db: &Db<RocksDbStorage>,
    schema: &JazzSchema,
    query: &Query,
) -> BTreeMap<RowUuid, (u64, u64)> {
    let prepared = db.prepare_query(query).expect("db prepare read shapes");
    db_rows_state(schema, db.read(&prepared).expect("db read shapes"))
}

fn apply_db_subscription_event(
    current: &mut BTreeMap<RowUuid, CurrentRow>,
    event: SubscriptionEvent,
) {
    match event {
        SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            ..
        } => {
            if reset {
                current.clear();
            }
            for row in removed {
                current.remove(&row.row_uuid);
            }
            for row in added.into_iter().chain(updated).map(|output| output.row) {
                current.insert(row.row_uuid(), row);
            }
        }
        SubscriptionEvent::Rejected { reason } => {
            panic!("subscription rejected unexpectedly: {reason:?}")
        }
        SubscriptionEvent::Closed => {}
    }
}

fn db_rows_state(schema: &JazzSchema, rows: Vec<CurrentRow>) -> BTreeMap<RowUuid, (u64, u64)> {
    let table = schema
        .tables
        .iter()
        .find(|table| table.name == SHAPES)
        .unwrap();
    rows.into_iter()
        .map(|row| {
            let x = match row.cell(table, "x").unwrap() {
                Value::F64(value) => value.to_bits(),
                other => panic!("unexpected x {other:?}"),
            };
            let y = match row.cell(table, "y").unwrap() {
                Value::F64(value) => value.to_bits(),
                other => panic!("unexpected y {other:?}"),
            };
            (row.row_uuid(), (x, y))
        })
        .collect()
}

fn shape_state(node: &mut NodeState<RocksDbStorage>) -> BTreeMap<RowUuid, (u64, u64)> {
    let table = schema()
        .tables()
        .iter()
        .find(|table| table.name == SHAPES)
        .unwrap()
        .clone();
    block_on(node.current_rows(SHAPES, DurabilityTier::Global))
        .unwrap()
        .into_iter()
        .map(|row| {
            let x = match row.cell(&table, "x").unwrap() {
                Value::F64(value) => value.to_bits(),
                other => panic!("unexpected x {other:?}"),
            };
            let y = match row.cell(&table, "y").unwrap() {
                Value::F64(value) => value.to_bits(),
                other => panic!("unexpected y {other:?}"),
            };
            (row.row_uuid(), (x, y))
        })
        .collect()
}

/// Linear history: Core mints at most one parentless, SYSTEM-authored fold
/// version per contended write, and never folds a fold, so `merges_of_merges`
/// is always zero. The pair is kept so the emitted summary shape is stable.
fn merge_counters(core: &mut NodeState<RocksDbStorage>, shapes: usize) -> (usize, usize) {
    let mut merges = 0;
    for idx in 0..shapes {
        for entry in block_on(core.row_history(SHAPES, shape_row(idx))).unwrap() {
            if entry.made_by() == AuthorSubject::SYSTEM {
                merges += 1;
            }
        }
    }
    (merges, 0)
}

fn canvas_cells() -> BTreeMap<String, Value> {
    BTreeMap::from([("name".to_owned(), Value::String("canvas".to_owned()))])
}

fn shape_cells(canvas: RowUuid, idx: usize, x: f64, y: f64) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("canvas".to_owned(), Value::Uuid(canvas.0)),
        ("type".to_owned(), Value::EnumTag((idx % 2) as u8)),
        ("text".to_owned(), Value::String(format!("shape-{idx}"))),
        ("x".to_owned(), Value::F64(x)),
        ("y".to_owned(), Value::F64(y)),
    ])
}

fn zipf_index(rng: &mut Lcg, len: usize) -> usize {
    let a = (rng.next_u64() as usize) % len;
    let b = (rng.next_u64() as usize) % len;
    a.min(b)
}

fn topology(config: &Config, profile: PeerProfile) -> Topology {
    let schema = schema();
    let latency_ms = client_core_latency_ms();
    let link = PeerProfile::new(
        format!("{}:client-core", profile.name),
        latency_ms,
        profile.jitter_ms,
        profile.per_message_overhead_ms,
    );
    let mut topology = Topology::default().node("core", schema.clone(), NodeRole::Core);
    let names = (0..config.active + config.passive)
        .map(|idx| format!("p{idx}"))
        .chain(["writer", "spy", "reconnect", "offline"].map(str::to_owned));
    for name in names {
        topology = topology
            .node(&name, schema.clone(), NodeRole::Reader)
            .link(&name, "core", link.clone())
            .link("core", &name, link.clone());
    }
    topology
}

fn client_core_latency_ms() -> u64 {
    env_u64("JAZZ_LINK_ONE_WAY_MS", 2)
}

fn emit_live_summary(
    driver: &str,
    coalesced: bool,
    config: &Config,
    summary: &LiveSummary,
    transport_metrics: serde_json::Map<String, JsonValue>,
) {
    let mut fields = metadata_fields("s2_canvas", driver, config.seed, &config.profile);
    fields.insert("phase".to_owned(), json!("live"));
    fields.insert(
        "transport_codec".to_owned(),
        json!(transport_codec_name(config.transport_codec)),
    );
    fields.insert("coalesced_16ms".to_owned(), json!(coalesced));
    fields.insert("commits".to_owned(), json!(summary.commits));
    fields.insert("participants".to_owned(), json!(summary.participants));
    fields.insert(
        "input_receipt_p50_us".to_owned(),
        json!(summary.latency.value_at_quantile(0.50)),
    );
    fields.insert(
        "input_receipt_p95_us".to_owned(),
        json!(summary.latency.value_at_quantile(0.95)),
    );
    fields.insert(
        "input_receipt_p99_us".to_owned(),
        json!(summary.latency.value_at_quantile(0.99)),
    );
    insert_stage_hist(&mut fields, "wall_receipt", &summary.wall_receipt);
    insert_stage_hist(&mut fields, "core_ingest_done", &summary.core_ingest_done);
    insert_stage_hist(
        &mut fields,
        "emission_construct",
        &summary.emission_construct,
    );
    insert_stage_hist(
        &mut fields,
        "link_handoff_to_delivered",
        &summary.link_handoff_to_delivered,
    );
    insert_stage_hist(
        &mut fields,
        "delivered_to_applied",
        &summary.delivered_to_applied,
    );
    fields.insert(
        "link_floor_us".to_owned(),
        json!(summary.link_one_way_floor_us),
    );
    fields.insert(
        "link_one_way_floor_us".to_owned(),
        json!(summary.link_one_way_floor_us),
    );
    fields.insert(
        "link_rtt_floor_us".to_owned(),
        json!(summary.link_rtt_floor_us),
    );
    fields.insert("bytes_total".to_owned(), json!(summary.bytes_total));
    fields.insert("peak_rss_bytes".to_owned(), json!(mem::peak_rss_bytes()));
    fields.insert("bytes_floor".to_owned(), json!(summary.bytes_floor));
    fields.insert("merge_versions".to_owned(), json!(summary.merge_versions));
    fields.insert(
        "merges_of_merges".to_owned(),
        json!(summary.merges_of_merges),
    );
    fields.insert(
        "core_tick_p50_us".to_owned(),
        json!(summary.core_tick.value_at_quantile(0.50)),
    );
    fields.insert(
        "history_rows_written".to_owned(),
        json!(summary.history_rows_written),
    );
    fields.extend(transport_metrics);
    emit_object(fields);
}

fn emit_concurrent_live_summary(coalesced: bool, config: &Config, summary: &ConcurrentLiveSummary) {
    let mut fields = metadata_fields("s2_canvas", "threaded", config.seed, &config.profile);
    fields.insert("phase".to_owned(), json!("live"));
    fields.insert("threading".to_owned(), json!("concurrent"));
    fields.insert(
        "transport_codec".to_owned(),
        json!(transport_codec_name(config.transport_codec)),
    );
    fields.insert("coalesced_16ms".to_owned(), json!(coalesced));
    fields.insert(
        "offered_commits_per_sec".to_owned(),
        json!(summary.offered_commits_per_sec),
    );
    fields.insert(
        "achieved_commits_per_sec".to_owned(),
        json!(summary.achieved_commits_per_sec),
    );
    fields.insert(
        "updates_delivered_per_sec".to_owned(),
        json!(summary.updates_delivered_per_sec),
    );
    fields.insert("offered_commits".to_owned(), json!(summary.offered_commits));
    fields.insert("commits".to_owned(), json!(summary.accepted_commits));
    fields.insert(
        "accepted_commits".to_owned(),
        json!(summary.accepted_commits),
    );
    fields.insert(
        "updates_delivered".to_owned(),
        json!(summary.updates_delivered),
    );
    fields.insert("participants".to_owned(), json!(summary.participants));
    fields.insert(
        "wall_duration_us".to_owned(),
        json!(summary.wall_duration_us),
    );
    insert_stage_hist(
        &mut fields,
        "local_commit_visibility",
        &summary.local_commit_visibility_us,
    );
    insert_stage_hist(&mut fields, "receipt_latency", &summary.receipt_latency_us);
    fields.insert("merge_versions".to_owned(), json!(summary.merge_versions));
    fields.insert(
        "merges_of_merges".to_owned(),
        json!(summary.merges_of_merges),
    );
    fields.insert(
        "history_rows_written".to_owned(),
        json!(summary.history_rows_written),
    );
    fields.insert(
        "core_tick_p50_us".to_owned(),
        json!(summary.core_tick.value_at_quantile(0.50)),
    );
    fields.insert(
        "core_tick_p95_us".to_owned(),
        json!(summary.core_tick.value_at_quantile(0.95)),
    );
    fields.insert("bytes_total".to_owned(), json!(summary.bytes_total));
    fields.insert("bytes_floor".to_owned(), json!(summary.bytes_floor));
    fields.insert("converged".to_owned(), json!(summary.converged));
    fields.insert("spy_rows".to_owned(), json!(summary.spy_rows));
    fields.insert("spy_updates".to_owned(), json!(summary.spy_updates));
    fields.insert("read_tier".to_owned(), json!("None"));
    fields.insert("write_wait_tier".to_owned(), json!("None"));
    fields.insert("peak_rss_bytes".to_owned(), json!(mem::peak_rss_bytes()));
    fields.extend(summary.transport_metrics.clone());
    emit_object(fields);
}

fn emit_historical_load_summary(coalesced: bool, config: &Config, summary: &HistoricalLoadSummary) {
    let mut fields = metadata_fields("s2_canvas", "deterministic", config.seed, &config.profile);
    fields.insert("phase".to_owned(), json!("historical_load"));
    fields.insert("coalesced_16ms".to_owned(), json!(coalesced));
    fields.insert("cut_percent".to_owned(), json!(summary.cut_percent));
    fields.insert("global_time".to_owned(), json!(summary.position.0));
    fields.insert("historical_load_us".to_owned(), json!(summary.latency_us));
    fields.insert("rows".to_owned(), json!(summary.rows));
    fields.insert(
        "correctness".to_owned(),
        json!("matched_accepted_history_prefix_replay"),
    );
    emit_object(fields);
}

fn emit_historical_load_gate(coalesced: bool, config: &Config, error: &impl std::fmt::Debug) {
    let mut fields = metadata_fields("s2_canvas", "deterministic", config.seed, &config.profile);
    fields.insert("phase".to_owned(), json!("historical_load"));
    fields.insert("status".to_owned(), json!("gated"));
    fields.insert(
        "needs".to_owned(),
        json!("historical-implicit-include-source-coverage"),
    );
    fields.insert("coalesced_16ms".to_owned(), json!(coalesced));
    fields.insert("error".to_owned(), json!(format!("{error:?}")));
    emit_object(fields);
}

fn is_known_historical_implicit_include_coverage_gap(error: &impl std::fmt::Debug) -> bool {
    let error = format!("{error:?}");
    error.contains("QueryCapability(")
        && error.contains("gaps: [Source(Coverage)]")
        && error.contains("HistoryCut")
        && error.contains("ImplicitRootReference")
}

fn emit_db_surface_summary(coalesced: bool, config: &Config, summary: &DbSurfaceSummary) {
    let mut fields = metadata_fields("s2_canvas", "db_surface", config.seed, &config.profile);
    fields.insert("phase".to_owned(), json!("db_surface_live"));
    fields.insert("coalesced_16ms".to_owned(), json!(coalesced));
    fields.insert("fixture_rows".to_owned(), json!(summary.fixture_rows));
    fields.insert("subscriptions".to_owned(), json!(summary.subscriptions));
    fields.insert("writes_applied".to_owned(), json!(summary.writes_applied));
    fields.insert("watch_changes".to_owned(), json!(summary.watch_changes));
    fields.insert("write_p50_us".to_owned(), json!(summary.write_p50_us));
    fields.insert("write_p95_us".to_owned(), json!(summary.write_p95_us));
    fields.insert("changed_p50_us".to_owned(), json!(summary.changed_p50_us));
    fields.insert("changed_p95_us".to_owned(), json!(summary.changed_p95_us));
    fields.insert("current_p50_us".to_owned(), json!(summary.current_p50_us));
    fields.insert("current_p95_us".to_owned(), json!(summary.current_p95_us));
    fields.insert("rows".to_owned(), json!(summary.rows));
    fields.insert("peak_rss_bytes".to_owned(), json!(mem::peak_rss_bytes()));
    emit_object(fields);
}

fn insert_stage_hist(
    fields: &mut serde_json::Map<String, JsonValue>,
    name: &str,
    hist: &Histogram<u64>,
) {
    fields.insert(
        format!("{name}_p50_us"),
        json!(hist.value_at_quantile(0.50)),
    );
    fields.insert(
        format!("{name}_p95_us"),
        json!(hist.value_at_quantile(0.95)),
    );
    fields.insert(
        format!("{name}_p99_us"),
        json!(hist.value_at_quantile(0.99)),
    );
    fields.insert(format!("{name}_max_us"), json!(hist.max()));
}

fn emit_failure_summary(
    config: &Config,
    summary: &FailureSummary,
    transport_metrics: serde_json::Map<String, JsonValue>,
) {
    let mut fields = metadata_fields(
        "s2_canvas_failure",
        "threaded",
        config.seed,
        &config.profile,
    );
    fields.insert("phase".to_owned(), json!("failure"));
    fields.insert(
        "transport_codec".to_owned(),
        json!(transport_codec_name(config.transport_codec)),
    );
    fields.insert(
        "recovery_to_convergence_us".to_owned(),
        json!(summary.recovery_to_convergence_us),
    );
    fields.insert("final_rows".to_owned(), json!(summary.final_rows));
    fields.insert("spy_rows".to_owned(), json!(summary.spy_rows));
    fields.insert(
        "disconnected_catchup_bytes".to_owned(),
        json!(summary.disconnected_catchup_bytes),
    );
    fields.insert("peak_rss_bytes".to_owned(), json!(mem::peak_rss_bytes()));
    fields.extend(transport_metrics);
    emit_object(fields);
}

fn emit_object(fields: serde_json::Map<String, JsonValue>) {
    let line = serde_json::to_string(&JsonValue::Object(fields)).unwrap();
    emit_json_line("s2_canvas", &line);
}

fn percentile(samples: &mut [u64], pct: u64) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    samples.sort_unstable();
    let idx = ((samples.len() as u64 * pct).div_ceil(100).saturating_sub(1)) as usize;
    samples[idx.min(samples.len() - 1)]
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn canvas_id() -> RowUuid {
    row(1)
}

fn row(idx: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[8..16].copy_from_slice(&(idx as u64 + 1).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn shape_row(idx: usize) -> RowUuid {
    row(1_000 + idx)
}

fn participant_author(idx: usize) -> AuthorSubject {
    AuthorSubject::for_test_uuid(row(20_000 + idx).0)
}

fn raw_claims(author: AuthorSubject) -> BTreeMap<String, Value> {
    let sub = author.test_uuid().to_string();
    BTreeMap::from([
        ("iss".to_owned(), Value::String("urn:jazz:test".to_owned())),
        (
            "issuer".to_owned(),
            Value::String("urn:jazz:test".to_owned()),
        ),
        ("sub".to_owned(), Value::String(sub.clone())),
        ("user_id".to_owned(), Value::String(sub)),
    ])
}

fn install_claims(node: &mut NodeState<RocksDbStorage>, author: AuthorSubject) {
    if author != AuthorSubject::SYSTEM {
        node.admit_test_session_claims(author, raw_claims(author));
    }
}

fn install_participant_claims(node: &mut NodeState<RocksDbStorage>, config: &Config) {
    for idx in 0..(config.active + config.passive) {
        install_claims(node, participant_author(idx));
    }
}

fn install_db_claims(db: &Db<RocksDbStorage>, author: AuthorSubject) {
    if author != AuthorSubject::SYSTEM {
        db.set_identity_claims(author, raw_claims(author));
    }
}

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
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

fn transport_codec_name(codec: SimulatorTransportCodec) -> &'static str {
    match codec {
        SimulatorTransportCodec::Native => "native",
        SimulatorTransportCodec::WireBytes => "wire_bytes",
        SimulatorTransportCodec::WireFrames => "wire_frames",
    }
}

fn transport_loopback(
    codec: SimulatorTransportCodec,
    message: SyncMessage,
    metrics: &SharedMetrics,
) -> SyncMessage {
    let mut metrics = metrics.lock().expect("transport metrics lock");
    loopback_transport_message(codec, message, &mut metrics)
}
