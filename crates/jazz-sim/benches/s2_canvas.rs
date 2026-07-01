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
use jazz::groove::records::{EnumSchema, Value};
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::{Durability, RocksDbStorage};
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::{CurrentRow, MergeableCommit, NodeState};
use jazz::peer::PeerState;
use jazz::protocol::{RegisterShapeOptions, ShapeAst, Subscribe, SubscriptionKey, SyncMessage};
use jazz::query::{Binding, Query, ValidatedQuery, claim, col, eq, lit, param};
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::time::GlobalSeq;
use jazz::tx::{DurabilityTier, Fate};
use jazz_sim::distributions::Lcg;
use jazz_sim::{
    DeterministicDriver, DriverContext, Metrics, NodeRole, PeerProfile, SimulatorTransportCodec,
    ThreadedDriver, Topology, bench_profile, emit_json_line, loopback_transport_message, mem,
    metadata_fields, scenario_transport_codec_env,
};
use serde_json::{Value as JsonValue, json};

const CANVASES: &str = "canvases";
const INVITES: &str = "canvasInvites";
const SHAPES: &str = "shapes";

type SharedMetrics = Arc<Mutex<Metrics>>;

fn main() {
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
    edge_acceptance: Histogram<u64>,
    edge_hydration_bytes: u64,
    edge_hydration_floor_bytes: u64,
    edge_hydration_rows: usize,
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
    edge_acceptance: Histogram<u64>,
    bytes_total: u64,
    bytes_floor: u64,
    edge_hydration_bytes: u64,
    edge_hydration_floor_bytes: u64,
    edge_hydration_rows: usize,
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
    position: GlobalSeq,
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
    edge: EdgeRoute,
}

struct EdgeRoute {
    name: String,
    node: NodeState<RocksDbStorage>,
    _dir: tempfile::TempDir,
    core_peer: PeerState,
    policy_peer: PeerState,
}

fn run_live(ctx: &mut dyn DriverContext, config: &Config, coalesced: bool) -> LiveSummary {
    let schema = schema();
    let canvas = canvas_id();
    let (_core_dir, mut core) = open_node(node(250), schema.clone());
    let (_writer_dir, mut writer) = open_node(node(1), schema.clone());
    seed_fixture(ctx, config, &mut writer, &mut core);

    let (shape, binding) = shape_subscription(&schema, canvas);
    let mut participants = open_participants(config, &schema);
    let mut spy = open_participant("spy", node(90), schema, AuthorId::from_bytes([0x55; 16]));
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
    let mut edge_acceptance = Histogram::new(3).unwrap();
    let mut bytes_total = 0_u64;
    let mut floor_bytes = 0_u64;
    let mut edge_hydration_bytes = 0_u64;
    let mut edge_hydration_floor_bytes = 0_u64;
    let mut edge_hydration_rows = 0_usize;
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
            let mut commit = MergeableCommit::new(SHAPES, row_uuid, 10_000 + commits as u64)
                .made_by(participant_author(active_idx))
                .cells(shape_cells(canvas, shape_idx, x, y));
            if let Some(parent) =
                current_content_parent(&mut participants[active_idx].node, row_uuid)
            {
                commit = commit.parents(vec![parent]);
            }
            let (tx_id, unit) = participants[active_idx]
                .node
                .commit_mergeable_unit(commit)
                .unwrap();
            let SyncMessage::CommitUnit { tx, versions } = unit else {
                unreachable!();
            };
            ctx.send(
                &participants[active_idx].name,
                &participants[active_idx].edge.name,
                SyncMessage::CommitUnit {
                    tx: tx.clone(),
                    versions: versions.clone(),
                },
            );
            let delivered_to_edge = ctx.recv(&participants[active_idx].edge.name);
            let SyncMessage::CommitUnit { tx, versions } = delivered_to_edge.message else {
                unreachable!();
            };
            let edge_start = ctx.now_ms();
            let active = &mut participants[active_idx];
            let updates = active
                .peer
                .ingest_edge_mergeable_commit_unit(&mut active.edge.node, tx, versions, u64::MAX)
                .expect("edge ingest");
            edge_acceptance
                .record((ctx.now_ms() - edge_start) * 1_000)
                .unwrap();
            core_ingest_done
                .record(submit_at.elapsed().as_micros() as u64)
                .unwrap();
            let _edge_fate_observed = updates.iter().any(|message| {
                matches!(
                    message,
                    SyncMessage::FateUpdate {
                        tx_id: seen,
                        fate: Fate::Accepted,
                        ..
                    } if *seen == tx_id
                )
            });
            let mut edge_commit = MergeableCommit::new(SHAPES, row_uuid, 20_000 + commits as u64)
                .made_by(participant_author(active_idx))
                .cells(shape_cells(canvas, shape_idx, x, y));
            if let Some(parent) = current_content_parent(&mut active.edge.node, row_uuid) {
                edge_commit = edge_commit.parents(vec![parent]);
            }
            let (_edge_tx_id, edge_unit) =
                active.edge.node.commit_mergeable_unit(edge_commit).unwrap();
            let SyncMessage::CommitUnit { tx, versions } = edge_unit else {
                unreachable!();
            };
            ctx.send(
                &active.edge.name,
                "core",
                SyncMessage::CommitUnit { tx, versions },
            );
            let delivered_to_core = ctx.recv("core");
            let SyncMessage::CommitUnit { tx, versions } = delivered_to_core.message else {
                unreachable!();
            };
            let core_start = Instant::now();
            core.ingest_commit_unit(tx, versions, u64::MAX)
                .expect("core ingest");
            core_tick
                .record(core_start.elapsed().as_micros() as u64)
                .unwrap();
            commits += 1;

            pending_receives.clear();
            for (idx, participant) in participants.iter_mut().enumerate() {
                let emit_start = Instant::now();
                let core_update = participant
                    .edge
                    .core_peer
                    .query_update(&mut core, &shape, &binding)
                    .expect("edge update");
                edge_hydration_bytes += view_update_bytes(&core_update);
                edge_hydration_floor_bytes += bytes_floor(&core_update);
                edge_hydration_rows += result_output_count(&core_update, SHAPES);
                ctx.send("core", &participant.edge.name, core_update);
                let delivered_to_edge = ctx.recv(&participant.edge.name);
                participant
                    .edge
                    .node
                    .apply_sync_message(delivered_to_edge.message)
                    .expect("edge apply");
                hydrate_edge_policy(ctx, &mut core, &mut participant.edge);
                let update = participant
                    .peer
                    .query_update(&mut participant.edge.node, &shape, &binding)
                    .expect("participant update");
                let emit_elapsed = emit_start.elapsed().as_micros() as u64;
                emission_construct.record(emit_elapsed).unwrap();
                bytes_total += view_update_bytes(&update);
                floor_bytes += bytes_floor(&update);
                let sent_at = Instant::now();
                ctx.send(&participant.edge.name, &participant.name, update);
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
                participant
                    .node
                    .apply_sync_message(delivered.message)
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
        let core_update = participant
            .edge
            .core_peer
            .rehydrate_query(&mut core, &shape, &binding)
            .expect("final edge rehydrate");
        edge_hydration_bytes += view_update_bytes(&core_update);
        edge_hydration_floor_bytes += bytes_floor(&core_update);
        edge_hydration_rows += result_output_count(&core_update, SHAPES);
        ctx.send("core", &participant.edge.name, core_update);
        let delivered_to_edge = ctx.recv(&participant.edge.name);
        participant
            .edge
            .node
            .apply_sync_message(delivered_to_edge.message)
            .expect("final edge apply");
        hydrate_edge_policy(ctx, &mut core, &mut participant.edge);
        let update = participant
            .peer
            .rehydrate_query(&mut participant.edge.node, &shape, &binding)
            .expect("final participant rehydrate");
        bytes_total += view_update_bytes(&update);
        floor_bytes += bytes_floor(&update);
        ctx.send(&participant.edge.name, &participant.name, update);
        let delivered = ctx.recv(&participant.name);
        participant
            .node
            .apply_sync_message(delivered.message)
            .expect("final participant apply");
    }
    let expected = shape_state(&mut core);
    for participant in &mut participants {
        assert_eq!(shape_state(&mut participant.node), expected);
    }
    assert!(rows(&mut spy.node, &shape, &binding).is_empty());
    assert_merges_are_concurrent(&mut core, config.shapes);
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
        edge_acceptance,
        edge_hydration_bytes,
        edge_hydration_floor_bytes,
        edge_hydration_rows,
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
    client_edge: Duration,
    edge_core: Duration,
}

#[derive(Clone, Copy, Debug)]
struct WorkItem {
    shape_idx: usize,
    x: f64,
    y: f64,
}

enum EdgeInbound {
    WriterCommit {
        writer_idx: usize,
        deliver_at: Instant,
        message: SyncMessage,
    },
    WriterDone {
        writer_idx: usize,
    },
    CoreUpdate {
        reader_idx: usize,
        deliver_at: Instant,
        message: SyncMessage,
        final_rehydrate: bool,
    },
    CoreDone,
}

enum CoreInbound {
    Commit {
        writer_idx: usize,
        deliver_at: Instant,
        message: Box<SyncMessage>,
    },
    Done,
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

struct ReaderEdgePeer {
    peer: PeerState,
    tx: mpsc::Sender<ReaderInbound>,
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

struct PendingCoreCommit {
    writer_idx: usize,
    unit: SyncMessage,
    parents: Vec<jazz::tx::TxId>,
    accepted: bool,
}

struct WriterResult {
    local_commit_visibility_us: Histogram<u64>,
}

struct EdgeResult {
    accepted_commits: usize,
    edge_acceptance: Histogram<u64>,
    bytes_total: u64,
    bytes_floor: u64,
    edge_hydration_bytes: u64,
    edge_hydration_floor_bytes: u64,
    edge_hydration_rows: usize,
}

struct CoreResult {
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
    seed_concurrent_fixture(&mut setup_ctx, config, &mut fixture_writer, &mut core);

    let (edge_dir, mut edge_node) = open_node(node(240), schema.clone());
    apply_core_binding(&mut core, &shape, &binding);
    apply_binding(&mut edge_node, &shape, &binding);
    let mut policy_peer = PeerState::relay();
    hydrate_edge_policy_direct(&mut core, &mut edge_node, &mut policy_peer);

    let mut writer_nodes = Vec::with_capacity(config.active);
    let mut writer_edge_peers = BTreeMap::new();
    for writer_idx in 0..config.active {
        let (dir, mut writer_node) = open_node(node(20 + writer_idx as u8), schema.clone());
        apply_binding(&mut writer_node, &shape, &binding);
        writer_nodes.push((dir, writer_node));
        writer_edge_peers.insert(
            writer_idx,
            PeerState::for_author(participant_author(writer_idx)),
        );
    }

    let mut reader_nodes = Vec::with_capacity(config.passive + 1);
    let mut passive_reader_positions = Vec::with_capacity(config.passive);
    for passive_idx in 0..config.passive {
        let participant_idx = config.active + passive_idx;
        let (dir, mut reader_node) = open_node(node(20 + participant_idx as u8), schema.clone());
        apply_binding(&mut reader_node, &shape, &binding);
        passive_reader_positions.push(reader_nodes.len());
        reader_nodes.push((
            format!("p{participant_idx}"),
            false,
            tiers.read_tier,
            dir,
            reader_node,
        ));
    }

    let mut initial_core_peer = PeerState::for_author(participant_author(0));
    let invited_core_update = initial_core_peer
        .rehydrate_query(&mut core, &shape, &binding)
        .expect("invited core rehydrate");
    edge_node
        .apply_sync_message(invited_core_update)
        .expect("edge apply invited initial");
    let mut writer_initial_edge_peer = PeerState::for_author(participant_author(0));
    let writer_initial_update = writer_initial_edge_peer
        .rehydrate_query(&mut edge_node, &shape, &binding)
        .expect("writer initial edge rehydrate");
    for (_, node) in &mut writer_nodes {
        node.apply_sync_message(writer_initial_update.clone())
            .expect("writer initial apply");
    }

    let mut reader_core_peers = Vec::with_capacity(config.passive);
    let mut reader_edge_peer_states = Vec::with_capacity(config.passive);
    for position in &passive_reader_positions {
        let mut core_peer = PeerState::for_author(participant_author(0));
        let _ = core_peer
            .rehydrate_query(&mut core, &shape, &binding)
            .expect("core reader initial rehydrate");
        reader_core_peers.push(ReaderCorePeer { peer: core_peer });

        let mut edge_peer = PeerState::for_author(participant_author(0));
        let reader_initial_update = edge_peer
            .rehydrate_query(&mut edge_node, &shape, &binding)
            .expect("edge reader initial rehydrate");
        let (_, _, _, _, node) = &mut reader_nodes[*position];
        node.apply_sync_message(reader_initial_update)
            .expect("reader initial apply");
        reader_edge_peer_states.push(edge_peer);
    }

    let (spy_dir, mut spy_node) = open_node(node(90), schema.clone());
    apply_binding(&mut spy_node, &shape, &binding);
    assert!(rows(&mut spy_node, &shape, &binding).is_empty());
    reader_nodes.push(("spy".to_owned(), true, tiers.read_tier, spy_dir, spy_node));

    let (edge_tx, edge_rx) = mpsc::channel::<EdgeInbound>();
    let (core_tx, core_rx) = mpsc::channel::<CoreInbound>();
    let mut reader_txs = Vec::with_capacity(reader_nodes.len());
    let mut reader_edge_peers = Vec::with_capacity(config.passive);
    let mut reader_edge_peer_states = reader_edge_peer_states.into_iter();
    let mut reader_handles = Vec::with_capacity(reader_nodes.len());
    while epoch.elapsed().as_millis() == 0 {
        thread::yield_now();
    }

    for (reader_idx, (name, is_spy, read_tier, dir, node)) in reader_nodes.into_iter().enumerate() {
        let (reader_tx, reader_rx) = mpsc::channel::<ReaderInbound>();
        if !is_spy {
            reader_edge_peers.push(ReaderEdgePeer {
                peer: reader_edge_peer_states
                    .next()
                    .expect("edge reader peer state"),
                tx: reader_tx.clone(),
            });
        }
        reader_txs.push(reader_tx);
        let reader_shape = shape.clone();
        let reader_binding = binding.clone();
        reader_handles.push(thread::spawn(move || {
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

    let edge_shape = shape.clone();
    let edge_binding = binding.clone();
    let edge_handle = thread::spawn({
        let core_tx = core_tx.clone();
        let writer_fate_txs = writer_fate_txs.clone();
        let transport_metrics = Arc::clone(&transport_metrics);
        move || {
            run_edge_actor(
                edge_dir,
                edge_node,
                writer_edge_peers,
                reader_edge_peers,
                reader_txs,
                edge_rx,
                core_tx,
                writer_fate_txs,
                edge_shape,
                edge_binding,
                links,
                transport_codec,
                transport_metrics,
                epoch,
                active_count,
            )
        }
    });

    let core_shape = shape.clone();
    let core_binding = binding.clone();
    let core_handle = thread::spawn({
        let edge_tx = edge_tx.clone();
        let writer_fate_txs = writer_fate_txs.clone();
        let transport_metrics = Arc::clone(&transport_metrics);
        move || {
            run_core_actor(
                core_dir,
                core,
                reader_core_peers,
                core_rx,
                edge_tx,
                writer_fate_txs,
                core_shape,
                core_binding,
                links,
                transport_codec,
                transport_metrics,
                epoch,
                shapes_count,
            )
        }
    });

    let start = Instant::now();
    let mut writer_handles = Vec::with_capacity(config.active);
    for (writer_idx, ((dir, node), items)) in writer_nodes.into_iter().zip(workload).enumerate() {
        let tx = edge_tx.clone();
        let fate_rx = writer_fate_rxs.remove(0);
        let transport_metrics = Arc::clone(&transport_metrics);
        let writer_shape = shape.clone();
        let writer_binding = binding.clone();
        let write_wait_tier = writer_write_wait_tiers[writer_idx];
        writer_handles.push(thread::spawn(move || {
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
    drop(edge_tx);
    drop(core_tx);
    drop(writer_fate_txs);

    let mut local_commit_visibility_us = Histogram::new(3).unwrap();
    for handle in writer_handles {
        let result = handle.join().expect("writer actor joined");
        merge_histogram(
            &mut local_commit_visibility_us,
            &result.local_commit_visibility_us,
        );
    }

    let edge_result = edge_handle.join().expect("edge actor joined");
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
    let accepted_commits = core_result
        .accepted_commits
        .min(edge_result.accepted_commits);
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
        edge_acceptance: edge_result.edge_acceptance,
        bytes_total: edge_result.bytes_total,
        bytes_floor: edge_result.bytes_floor,
        edge_hydration_bytes: edge_result.edge_hydration_bytes,
        edge_hydration_floor_bytes: edge_result.edge_hydration_floor_bytes,
        edge_hydration_rows: edge_result.edge_hydration_rows,
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
    edge_tx: mpsc::Sender<EdgeInbound>,
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
        let mut commit = MergeableCommit::new(SHAPES, row_uuid, made_at)
            .made_by(participant_author(writer_idx))
            .cells(shape_cells(canvas_id(), item.shape_idx, item.x, item.y));
        if let Some(parent) = current_content_parent(&mut node, row_uuid) {
            commit = commit.parents(vec![parent]);
        }
        let (tx_id, unit) = node.commit_mergeable_unit(commit).expect("writer commit");
        local_commit_visibility_us
            .record(intent.elapsed().as_micros() as u64)
            .expect("local visibility sample");
        edge_tx
            .send(EdgeInbound::WriterCommit {
                writer_idx,
                deliver_at: Instant::now() + links.client_edge,
                message: transport_loopback(transport_codec, unit, &transport_metrics),
            })
            .expect("edge actor open");
        await_write_tier(&mut node, tx_id, write_wait_tier, &fate_rx);
    }
    park_until(start + duration);
    let _ = edge_tx.send(EdgeInbound::WriterDone { writer_idx });
    WriterResult {
        local_commit_visibility_us,
    }
}

fn pending_core_commit(writer_idx: usize, unit: SyncMessage, accepted: bool) -> PendingCoreCommit {
    let SyncMessage::CommitUnit { versions, .. } = &unit else {
        unreachable!("core queue only stores commit units");
    };
    let parents = versions
        .iter()
        .flat_map(|version| version.parents())
        .collect();
    PendingCoreCommit {
        writer_idx,
        unit,
        parents,
        accepted,
    }
}

fn commit_is_ready_for_core(
    tx_id: jazz::tx::TxId,
    pending: &PendingCoreCommit,
    forwarded_to_core: &BTreeSet<jazz::tx::TxId>,
) -> bool {
    pending.accepted
        && pending
            .parents
            .iter()
            .all(|parent| parent.node != tx_id.node || forwarded_to_core.contains(parent))
}

fn drain_ready_core_commits(
    pending_for_core: &mut BTreeMap<jazz::tx::TxId, PendingCoreCommit>,
    forwarded_to_core: &mut BTreeSet<jazz::tx::TxId>,
    core_tx: &mpsc::Sender<CoreInbound>,
    links: LinkDurations,
    transport_codec: SimulatorTransportCodec,
    transport_metrics: &SharedMetrics,
) -> usize {
    let mut sent = 0;
    loop {
        let ready = pending_for_core.iter().find_map(|(tx_id, pending)| {
            commit_is_ready_for_core(*tx_id, pending, forwarded_to_core).then_some(*tx_id)
        });
        let Some(tx_id) = ready else {
            break;
        };
        let pending = pending_for_core
            .remove(&tx_id)
            .expect("ready commit still queued");
        send_core_commit(
            core_tx,
            pending.writer_idx,
            pending.unit,
            links.edge_core,
            transport_codec,
            transport_metrics,
        );
        forwarded_to_core.insert(tx_id);
        sent += 1;
    }
    sent
}

fn maybe_send_core_done(
    writer_done: &[bool],
    pending_for_core: &BTreeMap<jazz::tx::TxId, PendingCoreCommit>,
    sent_core_done: &mut bool,
    core_tx: &mpsc::Sender<CoreInbound>,
) {
    if !*sent_core_done && writer_done.iter().all(|done| *done) && pending_for_core.is_empty() {
        let _ = core_tx.send(CoreInbound::Done);
        *sent_core_done = true;
    }
}

#[allow(clippy::too_many_arguments)]
fn run_edge_actor(
    _dir: tempfile::TempDir,
    mut edge_node: NodeState<RocksDbStorage>,
    mut writer_peers: BTreeMap<usize, PeerState>,
    mut reader_peers: Vec<ReaderEdgePeer>,
    all_reader_txs: Vec<mpsc::Sender<ReaderInbound>>,
    edge_rx: mpsc::Receiver<EdgeInbound>,
    core_tx: mpsc::Sender<CoreInbound>,
    writer_fate_txs: Vec<mpsc::Sender<WriterFate>>,
    shape: ValidatedQuery,
    binding: Binding,
    links: LinkDurations,
    transport_codec: SimulatorTransportCodec,
    transport_metrics: SharedMetrics,
    epoch: Instant,
    writer_count: usize,
) -> EdgeResult {
    let mut accepted_commits = 0_usize;
    let mut edge_acceptance = Histogram::new(3).unwrap();
    let mut bytes_total = 0_u64;
    let mut bytes_floor_total = 0_u64;
    let mut edge_hydration_bytes = 0_u64;
    let mut edge_hydration_floor_bytes = 0_u64;
    let mut edge_hydration_rows = 0_usize;
    let mut writer_done = vec![false; writer_count];
    let mut pending_edge = Vec::new();
    let mut pending_for_core = BTreeMap::new();
    let mut forwarded_to_core = BTreeSet::new();
    let mut sent_core_done = false;

    while let Some(message) = recv_next_edge_message(&edge_rx, &mut pending_edge) {
        match message {
            EdgeInbound::WriterCommit {
                writer_idx,
                message,
                ..
            } => {
                let SyncMessage::CommitUnit { tx, versions } = message else {
                    unreachable!("writer sends commit units");
                };
                let tx_id = tx.tx_id;
                let core_unit = SyncMessage::CommitUnit {
                    tx: tx.clone(),
                    versions: versions.clone(),
                };
                let start = Instant::now();
                let writer_peer = writer_peers.get_mut(&writer_idx).expect("writer edge peer");
                let now_ms = epoch.elapsed().as_millis() as u64;
                let mut updates = writer_peer
                    .ingest_edge_mergeable_commit_unit(&mut edge_node, tx, versions, now_ms)
                    .expect("edge ingest");
                updates.extend(
                    writer_peer
                        .drain_deferred_edge_fates(&mut edge_node, now_ms)
                        .expect("drain deferred edge fate after ingest"),
                );
                edge_acceptance
                    .record(start.elapsed().as_micros() as u64)
                    .expect("edge acceptance sample");
                let accepted =
                    edge_fate_updates_accepted(writer_idx, tx_id, &updates, &writer_fate_txs);
                pending_for_core
                    .insert(tx_id, pending_core_commit(writer_idx, core_unit, accepted));
                accepted_commits += drain_ready_core_commits(
                    &mut pending_for_core,
                    &mut forwarded_to_core,
                    &core_tx,
                    links,
                    transport_codec,
                    &transport_metrics,
                );
                maybe_send_core_done(
                    &writer_done,
                    &pending_for_core,
                    &mut sent_core_done,
                    &core_tx,
                );
            }
            EdgeInbound::WriterDone { writer_idx } => {
                writer_done[writer_idx] = true;
                maybe_send_core_done(
                    &writer_done,
                    &pending_for_core,
                    &mut sent_core_done,
                    &core_tx,
                );
            }
            EdgeInbound::CoreUpdate {
                reader_idx,
                message,
                final_rehydrate,
                ..
            } => {
                edge_hydration_bytes += view_update_bytes(&message);
                edge_hydration_floor_bytes += bytes_floor(&message);
                edge_hydration_rows += result_output_count(&message, SHAPES);
                edge_node
                    .apply_sync_message(message)
                    .expect("edge apply core update");
                let now_ms = epoch.elapsed().as_millis() as u64;
                for writer_idx in 0..writer_count {
                    let drained = writer_peers
                        .get_mut(&writer_idx)
                        .expect("writer edge peer")
                        .drain_deferred_edge_fates(&mut edge_node, now_ms)
                        .expect("drain deferred edge fates");
                    for update in drained {
                        let tx_id = fate_tx_id(&update);
                        if edge_fate_update_accepted(writer_idx, &update, &writer_fate_txs)
                            && let Some(tx_id) = tx_id
                            && let Some(pending) = pending_for_core.get_mut(&tx_id)
                        {
                            pending.accepted = true;
                        }
                    }
                }
                accepted_commits += drain_ready_core_commits(
                    &mut pending_for_core,
                    &mut forwarded_to_core,
                    &core_tx,
                    links,
                    transport_codec,
                    &transport_metrics,
                );
                maybe_send_core_done(
                    &writer_done,
                    &pending_for_core,
                    &mut sent_core_done,
                    &core_tx,
                );
                let update = if final_rehydrate {
                    reader_peers[reader_idx]
                        .peer
                        .rehydrate_query(&mut edge_node, &shape, &binding)
                        .expect("edge reader final rehydrate")
                } else {
                    reader_peers[reader_idx]
                        .peer
                        .query_update(&mut edge_node, &shape, &binding)
                        .expect("edge reader query update")
                };
                bytes_total += view_update_bytes(&update);
                bytes_floor_total += bytes_floor(&update);
                let _ = reader_peers[reader_idx].tx.send(ReaderInbound::Update {
                    deliver_at: Instant::now() + links.client_edge,
                    message: Box::new(transport_loopback(
                        transport_codec,
                        update,
                        &transport_metrics,
                    )),
                });
            }
            EdgeInbound::CoreDone => {
                for reader in &all_reader_txs {
                    let _ = reader.send(ReaderInbound::Done);
                }
                break;
            }
        }
    }

    EdgeResult {
        accepted_commits,
        edge_acceptance,
        bytes_total,
        bytes_floor: bytes_floor_total,
        edge_hydration_bytes,
        edge_hydration_floor_bytes,
        edge_hydration_rows,
    }
}

#[allow(clippy::too_many_arguments)]
fn run_core_actor(
    _dir: tempfile::TempDir,
    mut core: NodeState<RocksDbStorage>,
    mut reader_peers: Vec<ReaderCorePeer>,
    core_rx: mpsc::Receiver<CoreInbound>,
    edge_tx: mpsc::Sender<EdgeInbound>,
    writer_fate_txs: Vec<mpsc::Sender<WriterFate>>,
    shape: ValidatedQuery,
    binding: Binding,
    links: LinkDurations,
    transport_codec: SimulatorTransportCodec,
    transport_metrics: SharedMetrics,
    epoch: Instant,
    shapes: usize,
) -> CoreResult {
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
                    unreachable!("edge sends commit units");
                };
                let start = Instant::now();
                let updates = core
                    .ingest_commit_unit(tx, versions, epoch.elapsed().as_millis() as u64)
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
                    let update = reader
                        .peer
                        .query_update(&mut core, &shape, &binding)
                        .expect("core reader query update");
                    let _ = edge_tx.send(EdgeInbound::CoreUpdate {
                        reader_idx,
                        deliver_at: Instant::now() + links.edge_core,
                        message: transport_loopback(transport_codec, update, &transport_metrics),
                        final_rehydrate: false,
                    });
                }
            }
            CoreInbound::Done => {
                for (reader_idx, reader) in reader_peers.iter_mut().enumerate() {
                    let update = reader
                        .peer
                        .rehydrate_query(&mut core, &shape, &binding)
                        .expect("core final reader rehydrate");
                    let _ = edge_tx.send(EdgeInbound::CoreUpdate {
                        reader_idx,
                        deliver_at: Instant::now() + links.edge_core,
                        message: transport_loopback(transport_codec, update, &transport_metrics),
                        final_rehydrate: true,
                    });
                }
                let _ = edge_tx.send(EdgeInbound::CoreDone);
                break;
            }
        }
    }
    assert_merges_are_concurrent(&mut core, shapes);
    let (merge_versions, merges_of_merges) = merge_counters(&mut core, shapes);
    let state = shape_state(&mut core);
    CoreResult {
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
                node.apply_sync_message(*message)
                    .expect("reader apply update");
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

fn hydrate_edge_policy_direct(
    core: &mut NodeState<RocksDbStorage>,
    edge_node: &mut NodeState<RocksDbStorage>,
    policy_peer: &mut PeerState,
) {
    let update = policy_peer
        .rehydrate_current_rows(core, INVITES)
        .expect("edge policy rehydrate");
    edge_node
        .apply_sync_message(update)
        .expect("edge policy apply");
}

fn apply_core_binding(
    core: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
) {
    core.apply_sync_message(SyncMessage::RegisterShape {
        shape_id: shape.shape_id(),
        ast: ShapeAst::from_validated(shape),
        opts: RegisterShapeOptions::default(),
    })
    .unwrap();
    let values = shape
        .params()
        .keys()
        .map(|name| binding.values().get(name).cloned().unwrap())
        .collect();
    core.apply_sync_message(SyncMessage::Subscribe(Subscribe {
        shape_id: shape.shape_id(),
        subscription: SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: RegisterShapeOptions::default().read_view_key(),
        },
        values,
    }))
    .unwrap();
}

fn link_durations(profile: &PeerProfile) -> LinkDurations {
    let (client_edge_ms, edge_core_ms) = profile_leg_ms(&profile.name);
    let overhead = profile.per_message_overhead_ms;
    LinkDurations {
        client_edge: Duration::from_millis(client_edge_ms + overhead),
        edge_core: Duration::from_millis(edge_core_ms + overhead),
    }
}

fn recv_next_edge_message(
    rx: &mpsc::Receiver<EdgeInbound>,
    pending: &mut Vec<EdgeInbound>,
) -> Option<EdgeInbound> {
    loop {
        while let Ok(message) = rx.try_recv() {
            pending.push(message);
        }
        if pending.is_empty() {
            match rx.recv() {
                Ok(message) => pending.push(message),
                Err(_) => return None,
            }
        }
        let (idx, deliver_at) = pending
            .iter()
            .enumerate()
            .min_by_key(|(_, message)| edge_deliver_at(message))
            .map(|(idx, message)| (idx, edge_deliver_at(message)))
            .expect("pending edge message");
        let now = Instant::now();
        if deliver_at <= now {
            return Some(pending.swap_remove(idx));
        }
        match rx.recv_timeout(deliver_at - now) {
            Ok(message) => pending.push(message),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Some(pending.swap_remove(idx));
            }
        }
    }
}

fn edge_deliver_at(message: &EdgeInbound) -> Instant {
    match message {
        EdgeInbound::WriterCommit { deliver_at, .. }
        | EdgeInbound::CoreUpdate { deliver_at, .. } => *deliver_at,
        EdgeInbound::WriterDone { .. } | EdgeInbound::CoreDone => Instant::now(),
    }
}

fn send_core_commit(
    core_tx: &mpsc::Sender<CoreInbound>,
    writer_idx: usize,
    message: SyncMessage,
    latency: Duration,
    transport_codec: SimulatorTransportCodec,
    transport_metrics: &SharedMetrics,
) {
    let _ = core_tx.send(CoreInbound::Commit {
        writer_idx,
        deliver_at: Instant::now() + latency,
        message: Box::new(transport_loopback(
            transport_codec,
            message,
            transport_metrics,
        )),
    });
}

fn edge_fate_updates_accepted(
    writer_idx: usize,
    tx_id: jazz::tx::TxId,
    updates: &[SyncMessage],
    writer_fate_txs: &[mpsc::Sender<WriterFate>],
) -> bool {
    updates.iter().any(|update| {
        let accepted = matches!(
            update,
            SyncMessage::FateUpdate {
                tx_id: seen,
                fate: Fate::Accepted,
                durability,
                ..
            } if *seen == tx_id && durability.is_some_and(|tier| tier >= DurabilityTier::Edge)
        );
        if matches!(update, SyncMessage::FateUpdate { .. }) {
            let _ = writer_fate_txs[writer_idx].send(WriterFate {
                message: update.clone(),
            });
        }
        accepted
    })
}

fn edge_fate_update_accepted(
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
        } if durability.is_some_and(|tier| tier >= DurabilityTier::Edge)
    )
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

fn fate_tx_id(update: &SyncMessage) -> Option<jazz::tx::TxId> {
    match update {
        SyncMessage::FateUpdate { tx_id, .. } => Some(*tx_id),
        _ => None,
    }
}

fn observed_shape_tx_ids(update: &SyncMessage, read_tier: DurabilityTier) -> Vec<jazz::tx::TxId> {
    if !observed_at_read_tier(update, read_tier) {
        return Vec::new();
    }
    match update {
        SyncMessage::ViewUpdate {
            result_member_adds, ..
        } => result_member_adds
            .iter()
            .filter_map(|entry| entry.as_row())
            .filter_map(|(table, _, tx_id)| (table.as_ref() == SHAPES).then_some(tx_id))
            .collect(),
        _ => Vec::new(),
    }
}

fn observed_at_read_tier(update: &SyncMessage, tier: DurabilityTier) -> bool {
    if tier == DurabilityTier::None {
        return true;
    }
    match update {
        SyncMessage::ViewUpdate {
            version_bundles, ..
        } => version_bundles
            .iter()
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
        DurabilityTier::Edge | DurabilityTier::Global => {}
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
        node.apply_sync_message(update.message)
            .expect("writer apply fate update");
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
            let mut commit = MergeableCommit::new(SHAPES, row_uuid, 10_000 + commits as u64)
                .made_by(participant_author(active_idx))
                .cells(shape_cells(canvas, shape_idx, x, y));
            if let Some(parent) = current_content_parent(&mut writer, row_uuid) {
                commit = commit.parents(vec![parent]);
            }
            let (_tx_id, unit) = writer.commit_mergeable_unit(commit).unwrap();
            let SyncMessage::CommitUnit { tx, versions } = unit else {
                unreachable!();
            };
            let tx_id = tx.tx_id;
            let updates = core
                .ingest_commit_unit(tx, versions, u64::MAX)
                .expect("core ingest");
            for update in updates {
                writer.apply_sync_message(update).unwrap();
            }
            commits += 1;
            expected.insert(row_uuid, (x.to_bits(), y.to_bits()));

            while next_cut < cut_targets.len() && commits >= cut_targets[next_cut].1 {
                let (percent, _) = cut_targets[next_cut];
                let position = core.transaction_record(tx_id).unwrap().global_seq.unwrap();
                let start = Instant::now();
                let rows = match core.at(position).read(&shape, &binding) {
                    Ok(rows) => rows,
                    Err(error) => {
                        emit_historical_load_gate(coalesced, config, &error);
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

    let canvas_write = db
        .insert_with_id(CANVASES, canvas, canvas_cells())
        .expect("db canvas insert");
    block_on(canvas_write.wait(DurabilityTier::Local)).expect("db canvas local wait");
    for idx in 0..(config.active + config.passive) {
        let invite = db
            .insert_with_id(
                INVITES,
                row(10_000 + idx),
                BTreeMap::from([
                    ("canvas".to_owned(), Value::Uuid(canvas.0)),
                    ("userID".to_owned(), Value::Uuid(participant_author(idx).0)),
                ]),
            )
            .expect("db invite insert");
        block_on(invite.wait(DurabilityTier::Local)).expect("db invite local wait");
    }

    let mut shape_rows = Vec::with_capacity(config.shapes);
    let mut expected = BTreeMap::new();
    for idx in 0..config.shapes {
        let write = db
            .insert(SHAPES, shape_cells(canvas, idx, idx as f64, idx as f64))
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
            let write = db.update(SHAPES, row_uuid, patch).expect("db shape update");
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
        AuthorId::from_bytes([0x44; 16]),
    );
    hydrate(ctx, &mut core, &mut spy, &shape, &binding);

    let start = ctx.now_ms();
    let mut catchup_bytes = 0;
    for idx in 0..(config.active * config.rate_per_sec.min(20)) {
        let row_uuid = shape_row(idx % config.shapes);
        let mut commit = MergeableCommit::new(SHAPES, row_uuid, 50_000 + idx as u64)
            .made_by(participant_author(0))
            .cells(shape_cells(canvas, idx, idx as f64, (idx * 2) as f64));
        if let Some(parent) = current_content_parent(&mut participant.node, row_uuid) {
            commit = commit.parents(vec![parent]);
        }
        let (_tx_id, unit) = participant.node.commit_mergeable_unit(commit).unwrap();
        let SyncMessage::CommitUnit { tx, versions } = unit else {
            unreachable!();
        };
        core.ingest_commit_unit(tx, versions, u64::MAX).unwrap();
        if idx == 5 {
            drop(core);
            let cfs = schema.column_families();
            let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
            let storage =
                RocksDbStorage::open_with_durability(core_dir.path(), &refs, Durability::WalNoSync)
                    .unwrap();
            core = NodeState::new(node(250), schema.clone(), storage).unwrap();
        }
    }
    let core_update = disconnected
        .edge
        .core_peer
        .rehydrate_query(&mut core, &shape, &binding)
        .expect("edge catch up rehydrate");
    ctx.send("core", &disconnected.edge.name, core_update);
    let delivered_to_edge = ctx.recv(&disconnected.edge.name);
    disconnected
        .edge
        .node
        .apply_sync_message(delivered_to_edge.message)
        .unwrap();
    hydrate_edge_policy(ctx, &mut core, &mut disconnected.edge);
    let update = disconnected
        .peer
        .rehydrate_query(&mut disconnected.edge.node, &shape, &binding)
        .expect("catch up rehydrate");
    catchup_bytes += view_update_bytes(&update);
    ctx.send(&disconnected.edge.name, &disconnected.name, update);
    let delivered = ctx.recv(&disconnected.name);
    disconnected
        .node
        .apply_sync_message(delivered.message)
        .unwrap();
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
    let shape_kind =
        ColumnType::Enum(EnumSchema::new("shape_type", ["circle", "rectangle"]).unwrap());
    let invite_policy = Policy::shape(Query::from(SHAPES).join_via_column(
        INVITES,
        "canvas",
        "canvas",
        [eq(col("userID"), claim("sub"))],
    ));
    JazzSchema::new([
        TableSchema::new(CANVASES, [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new(
            INVITES,
            [
                ColumnSchema::new("canvas", ColumnType::Uuid),
                ColumnSchema::new("userID", ColumnType::Uuid),
            ],
        )
        .with_reference("canvas", CANVASES),
        TableSchema::new(
            SHAPES,
            [
                ColumnSchema::new("canvas", ColumnType::Uuid),
                ColumnSchema::new("type", shape_kind),
                ColumnSchema::new("text", ColumnType::String),
                ColumnSchema::new("x", ColumnType::F64),
                ColumnSchema::new("y", ColumnType::F64),
            ],
        )
        .with_reference("canvas", CANVASES)
        .with_read_policy(invite_policy.clone())
        .with_write_policy(invite_policy),
    ])
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
        AuthorId::SYSTEM,
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
            AuthorId::SYSTEM,
            BTreeMap::from([
                ("canvas".to_owned(), Value::Uuid(canvas.0)),
                ("userID".to_owned(), Value::Uuid(participant_author(idx).0)),
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
            AuthorId::SYSTEM,
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
        AuthorId::SYSTEM,
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
            AuthorId::SYSTEM,
            BTreeMap::from([
                ("canvas".to_owned(), Value::Uuid(canvas.0)),
                ("userID".to_owned(), Value::Uuid(participant_author(idx).0)),
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
            AuthorId::SYSTEM,
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
    made_by: AuthorId,
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
    made_by: AuthorId,
    cells: BTreeMap<String, Value>,
    made_at: u64,
) {
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new(table, row_uuid, made_at)
                .made_by(made_by)
                .cells(cells),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        unreachable!();
    };
    core.ingest_commit_unit(tx, versions, u64::MAX).unwrap();
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
    register_binding(ctx, core, &participant.edge.name, shape, binding);
    apply_binding(&mut participant.edge.node, shape, binding);
    apply_binding(&mut participant.node, shape, binding);
    let core_update = participant
        .edge
        .core_peer
        .rehydrate_query(core, shape, binding)
        .unwrap();
    ctx.send("core", &participant.edge.name, core_update);
    let delivered_to_edge = ctx.recv(&participant.edge.name);
    participant
        .edge
        .node
        .apply_sync_message(delivered_to_edge.message)
        .unwrap();
    hydrate_edge_policy(ctx, core, &mut participant.edge);
    let update = participant
        .peer
        .rehydrate_query(&mut participant.edge.node, shape, binding)
        .unwrap();
    ctx.send(&participant.edge.name, &participant.name, update);
    let delivered = ctx.recv(&participant.name);
    participant
        .node
        .apply_sync_message(delivered.message)
        .unwrap();
}

fn hydrate_edge_policy(
    ctx: &mut dyn DriverContext,
    core: &mut NodeState<RocksDbStorage>,
    edge: &mut EdgeRoute,
) {
    let update = edge
        .policy_peer
        .rehydrate_current_rows(core, INVITES)
        .unwrap();
    ctx.send("core", &edge.name, update);
    let delivered = ctx.recv(&edge.name);
    edge.node.apply_sync_message(delivered.message).unwrap();
}

fn apply_binding(node: &mut NodeState<RocksDbStorage>, shape: &ValidatedQuery, binding: &Binding) {
    node.apply_sync_message(SyncMessage::RegisterShape {
        shape_id: shape.shape_id(),
        ast: ShapeAst::from_validated(shape),
        opts: RegisterShapeOptions::default(),
    })
    .unwrap();
    let values = shape
        .params()
        .keys()
        .map(|name| binding.values().get(name).cloned().unwrap())
        .collect();
    node.apply_sync_message(SyncMessage::Subscribe(Subscribe {
        shape_id: shape.shape_id(),
        subscription: SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: RegisterShapeOptions::default().read_view_key(),
        },
        values,
    }))
    .unwrap();
}

fn register_binding(
    ctx: &mut dyn DriverContext,
    core: &mut NodeState<RocksDbStorage>,
    client: &str,
    shape: &ValidatedQuery,
    binding: &Binding,
) {
    core.apply_sync_message(SyncMessage::RegisterShape {
        shape_id: shape.shape_id(),
        ast: ShapeAst::from_validated(shape),
        opts: RegisterShapeOptions::default(),
    })
    .unwrap();
    let values = shape
        .params()
        .keys()
        .map(|name| binding.values().get(name).cloned().unwrap())
        .collect();
    core.apply_sync_message(SyncMessage::Subscribe(Subscribe {
        shape_id: shape.shape_id(),
        subscription: SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: RegisterShapeOptions::default().read_view_key(),
        },
        values,
    }))
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
    identity: AuthorId,
) -> Participant {
    let edge_schema = schema.clone();
    let (dir, participant_node) = open_node(node_uuid, schema);
    let edge_uuid = node((node_uuid.as_bytes()[15]).saturating_add(100));
    let (edge_dir, edge_node) = open_node(edge_uuid, edge_schema);
    Participant {
        name: name.to_owned(),
        node: participant_node,
        _dir: dir,
        peer: PeerState::for_author(identity),
        edge: EdgeRoute {
            name: format!("{name}_edge"),
            node: edge_node,
            _dir: edge_dir,
            core_peer: PeerState::for_author(identity),
            policy_peer: PeerState::relay(),
        },
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
    let node = NodeState::new(node_uuid, schema, storage).unwrap();
    (dir, node)
}

fn open_db(
    node_uuid: NodeUuid,
    author: AuthorId,
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
        large_value_checkpoint_op_interval: 1024,
    }))
    .expect("db open");
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
    let node = NodeState::new_history_complete(node_uuid, schema, storage).unwrap();
    (dir, node)
}

fn rows(
    node: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
) -> Vec<RowUuid> {
    node.query_rows(shape, binding, DurabilityTier::Global)
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
        SubscriptionEvent::Opened { current: rows, .. }
        | SubscriptionEvent::Reset { current: rows, .. } => {
            current.clear();
            current.extend(rows.rows.into_iter().map(|row| (row.row_uuid(), row)));
        }
        SubscriptionEvent::Delta {
            added,
            updated,
            removed,
            ..
        } => {
            for row in added.into_iter().chain(updated) {
                current.insert(row.row_uuid(), row);
            }
            for row in removed {
                current.remove(&row.row_uuid);
            }
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
        .tables
        .into_iter()
        .find(|table| table.name == SHAPES)
        .unwrap();
    node.current_rows(SHAPES, DurabilityTier::Global)
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

fn current_content_parent(
    node: &mut NodeState<RocksDbStorage>,
    row_uuid: RowUuid,
) -> Option<jazz::tx::TxId> {
    node.row_history(SHAPES, row_uuid)
        .ok()?
        .into_iter()
        .find(|entry| {
            entry.deletion().is_none()
                && entry.is_locally_current()
                && !matches!(entry.fate(), Fate::Rejected(_))
        })
        .map(|entry| entry.tx_id())
}

fn merge_counters(core: &mut NodeState<RocksDbStorage>, shapes: usize) -> (usize, usize) {
    let mut merges = 0;
    let mut merges_of_merges = 0;
    for idx in 0..shapes {
        for entry in core.row_history(SHAPES, shape_row(idx)).unwrap() {
            if entry.parents().len() > 1 && entry.made_by() == AuthorId::SYSTEM {
                merges += 1;
                if entry.parents().iter().any(|parent| {
                    core.transaction_record(*parent)
                        .is_some_and(|record| record.made_by == AuthorId::SYSTEM)
                }) {
                    merges_of_merges += 1;
                }
            }
        }
    }
    (merges, merges_of_merges)
}

fn assert_merges_are_concurrent(core: &mut NodeState<RocksDbStorage>, shapes: usize) {
    for idx in 0..shapes {
        let history = core.row_history(SHAPES, shape_row(idx)).unwrap();
        let parents_by_tx = history
            .iter()
            .map(|entry| (entry.tx_id(), entry.parents()))
            .collect::<BTreeMap<_, _>>();
        for entry in &history {
            let parents = entry.parents();
            if parents.len() <= 1 || entry.made_by() != AuthorId::SYSTEM {
                continue;
            }
            for (left_idx, left) in parents.iter().enumerate() {
                for right in parents.iter().skip(left_idx + 1) {
                    assert!(
                        !is_ancestor(*left, *right, &parents_by_tx)
                            && !is_ancestor(*right, *left, &parents_by_tx),
                        "merge {:?} has non-concurrent parents {:?} and {:?}",
                        entry.tx_id(),
                        left,
                        right
                    );
                }
            }
        }
    }
}

fn is_ancestor(
    ancestor: jazz::tx::TxId,
    candidate: jazz::tx::TxId,
    parents_by_tx: &BTreeMap<jazz::tx::TxId, Vec<jazz::tx::TxId>>,
) -> bool {
    let mut stack = vec![candidate];
    while let Some(tx_id) = stack.pop() {
        if tx_id == ancestor {
            return true;
        }
        if let Some(parents) = parents_by_tx.get(&tx_id) {
            stack.extend(parents.iter().copied());
        }
    }
    false
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
                .map(version_bundle_bytes)
                .sum::<u64>()
                + (peer_payload_inventory.complete_tx_payloads.len() as u64 * tx_id_wire_bytes())
                + result_rows_bytes(result_member_adds)
                + result_rows_bytes(result_member_removes)
        }
        SyncMessage::CommitUnit { tx, versions } => {
            transaction_wire_bytes(tx) + versions.iter().map(version_record_bytes).sum::<u64>()
        }
        SyncMessage::FateUpdate { .. } => tx_id_wire_bytes() + 16,
        SyncMessage::ContentExtents { extents } => {
            extents.iter().map(|extent| extent.bytes.len() as u64).sum()
        }
        SyncMessage::RegisterShape { .. }
        | SyncMessage::Subscribe(_)
        | SyncMessage::PublishSchema { .. }
        | SyncMessage::PublishLens { .. }
        | SyncMessage::SetCurrentWriteSchema { .. }
        | SyncMessage::CatalogueAck(_)
        | SyncMessage::FetchContentExtent { .. }
        | SyncMessage::SessionClaims { .. }
        | SyncMessage::Unsubscribe { .. } => 0,
    }
}

fn bytes_floor(update: &SyncMessage) -> u64 {
    match update {
        SyncMessage::ViewUpdate {
            version_bundles, ..
        } => version_bundles
            .iter()
            .flat_map(|bundle| &bundle.versions)
            .map(version_record_bytes)
            .sum(),
        _ => 0,
    }
}

fn version_bundle_bytes(bundle: &jazz::protocol::VersionBundle) -> u64 {
    transaction_wire_bytes(&bundle.tx)
        + bundle
            .versions
            .iter()
            .map(version_record_bytes)
            .sum::<u64>()
        + 16
}

fn version_record_bytes(version: &jazz::protocol::VersionRecord) -> u64 {
    version.table().len() as u64 + version.record().raw().len() as u64
}

fn transaction_wire_bytes(tx: &jazz::tx::Transaction) -> u64 {
    tx_id_wire_bytes()
        + 4
        + 16
        + tx.user_metadata_json
            .as_ref()
            .map_or(0, |metadata| metadata.len() as u64)
}

fn result_rows_bytes(rows: &[jazz::protocol::ResultMemberEntry]) -> u64 {
    rows.iter()
        .filter_map(|entry| entry.as_row())
        .map(|(table, _, _)| table.len() as u64 + 16 + tx_id_wire_bytes())
        .sum()
}

fn tx_id_wire_bytes() -> u64 {
    8 + 16
}

fn result_output_count(update: &SyncMessage, table: &str) -> usize {
    match update {
        SyncMessage::ViewUpdate {
            result_member_adds, ..
        } => result_member_adds
            .iter()
            .filter_map(|entry| entry.as_row())
            .filter(|entry| entry.0.as_ref() == table)
            .count(),
        _ => 0,
    }
}

fn canvas_cells() -> BTreeMap<String, Value> {
    BTreeMap::from([("name".to_owned(), Value::String("canvas".to_owned()))])
}

fn shape_cells(canvas: RowUuid, idx: usize, x: f64, y: f64) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("canvas".to_owned(), Value::Uuid(canvas.0)),
        ("type".to_owned(), Value::Enum((idx % 2) as u8)),
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
    let (client_edge_ms, edge_core_ms) = profile_leg_ms(&profile.name);
    let client_edge = PeerProfile::new(
        format!("{}:client-edge", profile.name),
        client_edge_ms,
        profile.jitter_ms,
        profile.per_message_overhead_ms,
    );
    let edge_core = PeerProfile::new(
        format!("{}:edge-core", profile.name),
        edge_core_ms,
        profile.jitter_ms,
        profile.per_message_overhead_ms,
    );
    let mut topology = Topology::default()
        .node("writer", schema.clone(), NodeRole::Writer)
        .node("core", schema.clone(), NodeRole::Core)
        .link("writer", "core", edge_core.clone())
        .link("core", "writer", edge_core.clone());
    for idx in 0..(config.active + config.passive) {
        let name = format!("p{idx}");
        let edge = format!("{name}_edge");
        topology = topology
            .node(&name, schema.clone(), NodeRole::Reader)
            .node(&edge, schema.clone(), NodeRole::Edge)
            .client_edge_core_line(&name, &edge, "core", client_edge.clone(), edge_core.clone());
    }
    topology
        .node("spy", schema.clone(), NodeRole::Reader)
        .node("spy_edge", schema.clone(), NodeRole::Edge)
        .client_edge_core_line(
            "spy",
            "spy_edge",
            "core",
            client_edge.clone(),
            edge_core.clone(),
        )
        .node("reconnect", schema.clone(), NodeRole::Reader)
        .node("reconnect_edge", schema.clone(), NodeRole::Edge)
        .client_edge_core_line(
            "reconnect",
            "reconnect_edge",
            "core",
            client_edge.clone(),
            edge_core.clone(),
        )
        .node("offline", schema.clone(), NodeRole::Reader)
        .node("offline_edge", schema, NodeRole::Edge)
        .client_edge_core_line("offline", "offline_edge", "core", client_edge, edge_core)
}

fn profile_leg_ms(profile_name: &str) -> (u64, u64) {
    let total = env_u64("JAZZ_LINK_ONE_WAY_MS", 1);
    let client_edge = env_u64("JAZZ_CLIENT_EDGE_ONE_WAY_MS", total.min(1));
    let edge_core = env_u64(
        "JAZZ_EDGE_CORE_ONE_WAY_MS",
        total.saturating_sub(client_edge).max(1),
    );
    let _ = profile_name;
    (client_edge, edge_core)
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

    let mut edge_acceptance = metadata_fields("s2_canvas", driver, config.seed, &config.profile);
    edge_acceptance.insert("phase".to_owned(), json!("edge_mergeable_acceptance"));
    edge_acceptance.insert("coalesced_16ms".to_owned(), json!(coalesced));
    edge_acceptance.insert(
        "acceptance_p50_us".to_owned(),
        json!(summary.edge_acceptance.value_at_quantile(0.50)),
    );
    edge_acceptance.insert(
        "acceptance_p95_us".to_owned(),
        json!(summary.edge_acceptance.value_at_quantile(0.95)),
    );
    edge_acceptance.insert("durability_tier".to_owned(), json!("Edge"));
    emit_object(edge_acceptance);

    let mut edge_hydration = metadata_fields("s2_canvas", driver, config.seed, &config.profile);
    edge_hydration.insert("phase".to_owned(), json!("edge_permission_scope_hydration"));
    edge_hydration.insert("coalesced_16ms".to_owned(), json!(coalesced));
    edge_hydration.insert("scope".to_owned(), json!("canvas_shape"));
    edge_hydration.insert(
        "hydration_bytes".to_owned(),
        json!(summary.edge_hydration_bytes),
    );
    edge_hydration.insert(
        "hydration_floor_bytes".to_owned(),
        json!(summary.edge_hydration_floor_bytes),
    );
    edge_hydration.insert(
        "hydration_rows".to_owned(),
        json!(summary.edge_hydration_rows),
    );
    emit_object(edge_hydration);
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
    fields.insert(
        "edge_acceptance_p50_us".to_owned(),
        json!(summary.edge_acceptance.value_at_quantile(0.50)),
    );
    fields.insert(
        "edge_acceptance_p95_us".to_owned(),
        json!(summary.edge_acceptance.value_at_quantile(0.95)),
    );
    fields.insert("bytes_total".to_owned(), json!(summary.bytes_total));
    fields.insert("bytes_floor".to_owned(), json!(summary.bytes_floor));
    fields.insert(
        "edge_hydration_bytes".to_owned(),
        json!(summary.edge_hydration_bytes),
    );
    fields.insert(
        "edge_hydration_floor_bytes".to_owned(),
        json!(summary.edge_hydration_floor_bytes),
    );
    fields.insert(
        "edge_hydration_rows".to_owned(),
        json!(summary.edge_hydration_rows),
    );
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
    fields.insert("global_seq".to_owned(), json!(summary.position.0));
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

fn participant_author(idx: usize) -> AuthorId {
    AuthorId(row(20_000 + idx).0)
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
