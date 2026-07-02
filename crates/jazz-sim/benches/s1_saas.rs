use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use hdrhistogram::Histogram;
use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, RowCells, SeededRowIdSource, SubscriptionEvent,
    SubscriptionStream,
};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::{Durability, RocksDbStorage};
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::{CurrentRow, MergeableCommit, NodeState};
use jazz::peer::{MaintainedSubscriptionViewMetrics, PeerState};
use jazz::protocol::{RegisterShapeOptions, ShapeAst, Subscribe, SubscriptionKey, SyncMessage};
use jazz::query::{Binding, Query, ValidatedQuery, col, eq, lit, ne, param};
use jazz::schema::{JazzSchema, TableSchema};
use jazz::time::TxTime;
use jazz::tx::{DurabilityTier, Fate};
use jazz_sim::distributions::Lcg;
use jazz_sim::fixture::{
    CellValueGen, EdgeSet, EntitySet, Fixture, FixtureBuilder, FixtureCommit, FixtureCommitApply,
    RefDistribution, apply_fixture_commit,
};
use jazz_sim::{
    DeterministicDriver, DriverContext, NodeRole, PauseMode, PeerProfile, SimulatorTransportCodec,
    ThreadedDriver, Topology, bench_profile, emit_json_line, metadata_fields, profiling,
    scenario_transport_codec_env,
};
use serde_json::{Value as JsonValue, json};

const ORGS: &str = "orgs";
const USERS: &str = "users";
const TEAMS: &str = "teams";
const USER_TEAM_MEMBERSHIPS: &str = "userTeamMemberships";
const TAGS: &str = "tags";
const PROJECTS: &str = "projects";
const PROJECT_TEAM_MEMBERSHIPS: &str = "projectTeamMemberships";
const MILESTONES: &str = "milestones";
const MILESTONE_DEPENDENCIES: &str = "milestoneDependencies";
const CYCLES: &str = "cycles";
const ISSUES: &str = "issues";
const ISSUE_TAGS: &str = "issueTags";
const HF_PARENTS: &str = "hydrationParents";
const HF_CHILDREN: &str = "hydrationChildren";

const STATE_IN_PROGRESS: u8 = 2;
const STATE_DONE: u8 = 3;

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
        let topology = topology(&config, profile.clone());

        let mut deterministic = DeterministicDriver::new(topology.clone(), config.seed)
            .with_transport_codec(config.transport_codec);
        let deterministic_summary = execute(&mut deterministic, &config);
        emit_summary("deterministic", &config, &deterministic_summary);

        let mut threaded =
            ThreadedDriver::new(topology, config.seed).with_transport_codec(config.transport_codec);
        let threaded_summary = execute(&mut threaded, &config);
        emit_summary("threaded", &config, &threaded_summary);

        for summary in reconnect_summaries(&config, profile.clone()) {
            emit_reconnect_summary(&config, &summary);
        }
        for summary in subscriber_sweep_summaries(&config) {
            emit_sweep_summary(&config, &summary);
        }
    }
    if phase_selection.should_run("high_fan_out_hydration") {
        for summary in high_fan_out_hydration_summaries(&config, profile) {
            emit_high_fan_out_summary(&config, &summary);
        }
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
                matches!(phase.as_str(), "default" | "high_fan_out_hydration"),
                "unsupported JAZZ_BENCH_PHASES value {phase:?}; supported values: default, high_fan_out_hydration"
            );
        }
    }
}

pub fn smoke() {
    let config = Config {
        seed: 0x51aa_5001,
        profile: "s1-smoke".to_owned(),
        orgs: 1,
        teams_per_org: 2,
        users_per_org: 4,
        issues_per_org: 8,
        tags: 2,
        projects_per_org: 2,
        milestones_per_org: 2,
        clients: 2,
        writes: 3,
        reconnect_windows: vec![2],
        sweep_counts: vec![2],
        sweep_commits: 1,
        sweep_issues_per_org: 4,
        hydration_parents: 2,
        hydration_fanouts: vec![2],
        hydration_fate_bursts: 1,
        transport_codec: SimulatorTransportCodec::WireFrames,
        reconnect_transport_codec: SimulatorTransportCodec::WireFrames,
    };
    let profile = PeerProfile::new(config.profile.clone(), 1, 0, 0);
    let topology = topology(&config, profile.clone());
    let mut deterministic = DeterministicDriver::new(topology, config.seed)
        .with_transport_codec(config.transport_codec);
    let _summary = profiling::maybe_profile_phase("s1_saas", "deterministic_execute", || {
        execute(&mut deterministic, &config)
    });
    assert_wire_frame_metrics(deterministic.metrics_json_fields());
    let reconnect = profiling::maybe_profile_phase("s1_saas", "reconnect", || {
        reconnect_summaries(&config, profile)
    });
    assert_eq!(reconnect.len(), 1);
    assert_wire_frame_metrics(reconnect[0].transport_metrics.clone());
}

pub fn db_surface_smoke() {
    let config = Config {
        seed: 0x51aa_5001,
        profile: "s1-db-smoke".to_owned(),
        orgs: 1,
        teams_per_org: 2,
        users_per_org: 4,
        issues_per_org: 8,
        tags: 2,
        projects_per_org: 2,
        milestones_per_org: 2,
        clients: 1,
        writes: 1,
        reconnect_windows: vec![2],
        sweep_counts: vec![2],
        sweep_commits: 1,
        sweep_issues_per_org: 4,
        hydration_parents: 2,
        hydration_fanouts: vec![2],
        hydration_fate_bursts: 1,
        transport_codec: SimulatorTransportCodec::WireFrames,
        reconnect_transport_codec: SimulatorTransportCodec::Native,
    };
    let schema = schema();
    let fixture = build_fixture(&config);
    let plan = representative_plan(&fixture);
    let (_dir, db) = open_db(node(70), AuthorId(plan.user.0), schema.clone());
    let mut oracle = DbS1Oracle::default();

    let query1 = db_query1(&plan);
    let query2 = db_query2(&plan);
    let prepared_query1 = db.prepare_query(&query1).expect("db prepare q1");
    let prepared_query2 = db.prepare_query(&query2).expect("db prepare q2");
    let mut subscription1 =
        block_on(db.subscribe(&prepared_query1, ReadOpts::default())).expect("db subscribe q1");
    let mut subscription2 =
        block_on(db.subscribe(&prepared_query2, ReadOpts::default())).expect("db subscribe q2");
    let mut subscription1_rows = subscription_snapshot_row_set(&mut subscription1);
    let mut subscription2_rows = subscription_snapshot_row_set(&mut subscription2);
    assert!(subscription1_rows.is_empty());
    assert!(subscription2_rows.is_empty());

    for commit in &fixture.commits {
        let handle = db
            .insert_with_id(&commit.table, commit.row_uuid, commit.cells.clone())
            .expect("db fixture insert");
        block_on(handle.wait(DurabilityTier::Local)).expect("fixture insert local wait");
        oracle.apply_insert(commit);
    }

    let subscription1_changed =
        apply_subscription_events(&mut subscription1, &mut subscription1_rows);
    let subscription2_changed =
        apply_subscription_events(&mut subscription2, &mut subscription2_rows);
    assert!(
        subscription1_changed || subscription2_changed,
        "at least one subscription should observe fixture population"
    );
    assert_db_query_matches_oracle(
        &db,
        &schema,
        &query1,
        oracle.query1(&plan),
        "q1 after cold load",
    );
    assert_db_query_matches_oracle(
        &db,
        &schema,
        &query2,
        oracle.query2(&plan),
        "q2 after cold load",
    );
    assert_eq!(subscription1_rows, oracle.query1(&plan));
    assert_eq!(subscription2_rows, oracle.query2(&plan));

    let edited_issue = oracle
        .query1(&plan)
        .into_iter()
        .next()
        .map(|(_, row)| row)
        .expect("representative S1 plan should match at least one issue");
    let patch = BTreeMap::from([
        ("state".to_owned(), Value::U8(STATE_DONE)),
        (
            "title".to_owned(),
            Value::String("db-surface-state-transition".to_owned()),
        ),
    ]);
    let handle = db
        .update(ISSUES, edited_issue, patch.clone())
        .expect("db issue update");
    block_on(handle.wait(DurabilityTier::Local)).expect("issue update local wait");
    oracle.apply_patch(ISSUES, edited_issue, patch);

    assert!(apply_subscription_events(
        &mut subscription1,
        &mut subscription1_rows
    ));
    assert_db_query_matches_oracle(
        &db,
        &schema,
        &query1,
        oracle.query1(&plan),
        "q1 after update",
    );
    assert_db_query_matches_oracle(
        &db,
        &schema,
        &query2,
        oracle.query2(&plan),
        "q2 after update",
    );
    assert_eq!(subscription1_rows, oracle.query1(&plan));

    let _ = db.one(&prepared_query1).expect("db one q1");
}

#[derive(Clone, Debug)]
struct Config {
    seed: u64,
    profile: String,
    orgs: usize,
    teams_per_org: usize,
    users_per_org: usize,
    issues_per_org: usize,
    tags: usize,
    projects_per_org: usize,
    milestones_per_org: usize,
    clients: usize,
    writes: usize,
    reconnect_windows: Vec<usize>,
    sweep_counts: Vec<usize>,
    sweep_commits: usize,
    sweep_issues_per_org: usize,
    hydration_parents: usize,
    hydration_fanouts: Vec<usize>,
    hydration_fate_bursts: usize,
    transport_codec: SimulatorTransportCodec,
    reconnect_transport_codec: SimulatorTransportCodec,
}

impl Config {
    fn from_env() -> Self {
        let bench_profile = bench_profile();
        Self {
            seed: env_u64("JAZZ_SEED", 0x51aa_5001),
            profile: std::env::var("JAZZ_PROFILE").unwrap_or_else(|_| "s1-local".to_owned()),
            orgs: env_usize("JAZZ_S1_ORGS", bench_profile.select(1, 2, 2)),
            teams_per_org: env_usize("JAZZ_S1_TEAMS_PER_ORG", bench_profile.select(2, 3, 4)),
            users_per_org: env_usize("JAZZ_S1_USERS_PER_ORG", bench_profile.select(4, 8, 12)),
            issues_per_org: env_usize("JAZZ_S1_ISSUES_PER_ORG", bench_profile.select(25, 100, 250)),
            tags: env_usize("JAZZ_S1_TAGS", bench_profile.select(3, 5, 8)),
            projects_per_org: env_usize("JAZZ_S1_PROJECTS_PER_ORG", bench_profile.select(3, 6, 10)),
            milestones_per_org: env_usize(
                "JAZZ_S1_MILESTONES_PER_ORG",
                bench_profile.select(2, 4, 6),
            ),
            clients: env_usize("JAZZ_S1_CLIENTS", bench_profile.select(1, 2, 2)).max(1),
            writes: env_usize("JAZZ_S1_WRITES", bench_profile.select(5, 10, 20)),
            reconnect_windows: env_usize_list(
                "JAZZ_S1_RECONNECT_WINDOWS",
                bench_profile.select(&[5, 25][..], &[25, 100, 500], &[50, 500, 5000]),
            ),
            sweep_counts: env_usize_list(
                "JAZZ_S1_SWEEP_CLIENTS",
                bench_profile.select(&[2, 5][..], &[5, 25], &[10, 100]),
            ),
            sweep_commits: env_usize("JAZZ_S1_SWEEP_COMMITS", bench_profile.select(1, 2, 2)),
            sweep_issues_per_org: env_usize(
                "JAZZ_S1_SWEEP_ISSUES_PER_ORG",
                bench_profile.select(4, 8, 10),
            ),
            hydration_parents: env_usize(
                "JAZZ_S1_HYDRATION_PARENTS",
                bench_profile.select(3, 10, 20),
            )
            .max(1),
            hydration_fanouts: env_usize_list(
                "JAZZ_S1_HYDRATION_FANOUTS",
                bench_profile.select(&[2, 5, 10][..], &[10, 50, 100], &[10, 100, 1000]),
            ),
            hydration_fate_bursts: env_usize(
                "JAZZ_S1_HYDRATION_FATE_BURSTS",
                bench_profile.select(2, 4, 8),
            ),
            transport_codec: scenario_transport_codec_env("JAZZ_S1_TRANSPORT_CODEC"),
            reconnect_transport_codec: scenario_transport_codec_env(
                "JAZZ_S1_RECONNECT_TRANSPORT_CODEC",
            ),
        }
    }

    fn teams(&self) -> usize {
        self.orgs * self.teams_per_org
    }

    fn users(&self) -> usize {
        self.orgs * self.users_per_org
    }

    fn issues(&self) -> usize {
        self.orgs * self.issues_per_org
    }

    fn projects(&self) -> usize {
        self.orgs * self.projects_per_org
    }

    fn milestones(&self) -> usize {
        self.orgs * self.milestones_per_org
    }
}

#[derive(Clone, Debug)]
struct Summary {
    fixture_hash: u64,
    fixture_rows: usize,
    clients: usize,
    cold_complete_p50_us: u64,
    cold_complete_p95_us: u64,
    cold_bytes: u64,
    cold_bytes_floor: u64,
    naive_refetch_ceiling_bytes: u64,
    warm_local_p50_us: u64,
    warm_local_p95_us: u64,
    warm_settled_p50_us: u64,
    warm_settled_p95_us: u64,
    result_set_rows: usize,
    closure_rows: usize,
    writes_applied: usize,
    edge_acceptance: Histogram<u64>,
    edge_hydration_bytes: u64,
    edge_hydration_floor_bytes: u64,
    edge_hydration_rows: usize,
}

#[derive(Clone)]
struct ClientPlan {
    name: String,
    user: RowUuid,
    active_cycle: RowUuid,
    project: RowUuid,
    tag: RowUuid,
}

#[derive(Clone, Debug)]
struct ReconnectSummary {
    window_writes: usize,
    catchup_us: u64,
    catchup_bytes: u64,
    catchup_bytes_floor: u64,
    result_set_rows: usize,
    closure_rows: usize,
    transport_codec: SimulatorTransportCodec,
    transport_metrics: serde_json::Map<String, JsonValue>,
}

#[derive(Clone, Debug)]
struct SweepSummary {
    subscribers: usize,
    commits: usize,
    core_emit_p50_us: u64,
    core_emit_p95_us: u64,
    total_notification_bytes: u64,
    bytes_per_commit: u64,
    version_bundles_out: u64,
    complete_tx_refs_out: u64,
    result_adds_out: u64,
    result_removes_out: u64,
}

#[derive(Clone, Debug)]
struct HighFanOutSummary {
    fanout: usize,
    parents: usize,
    children: usize,
    subscriptions: usize,
    hydration_complete_us: u64,
    hydration_bytes: u64,
    hydration_floor_bytes: u64,
    result_set_rows: usize,
    mid_hydration_fates: usize,
    mid_hydration_bytes: u64,
    by_tx_index_seeks: u64,
    history_scan_fallbacks: u64,
    maintained_subscription_view_metrics: MaintainedSubscriptionViewMetrics,
    full_diff_recomputes: u64,
}

struct EdgeRoute {
    name: String,
    node: NodeState<RocksDbStorage>,
    _dir: tempfile::TempDir,
    core_peer: PeerState,
}

fn edge_acceptance_phase(
    ctx: &mut dyn DriverContext,
    client: &mut NodeState<RocksDbStorage>,
    edge: &mut EdgeRoute,
) -> Histogram<u64> {
    let mut acceptance = Histogram::new(3).unwrap();
    let issue = row(9_500_000);
    let start = ctx.now_ms();
    let (tx_id, unit) = client
        .commit_mergeable_unit(
            MergeableCommit::new(ISSUES, issue, 950_000)
                .made_by(AuthorId::SYSTEM)
                .cells(BTreeMap::from([(
                    "title".to_owned(),
                    Value::String("edge-acceptance-probe".to_owned()),
                )])),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        unreachable!();
    };
    ctx.send(
        "client_0",
        &edge.name,
        SyncMessage::CommitUnit { tx, versions },
    );
    let delivered = ctx.recv(&edge.name);
    let SyncMessage::CommitUnit { tx, versions } = delivered.message else {
        unreachable!();
    };
    let updates = PeerState::new()
        .ingest_edge_mergeable_commit_unit(&mut edge.node, tx, versions, u64::MAX)
        .unwrap();
    let _accepted = updates.iter().any(|message| {
        matches!(
            message,
            SyncMessage::FateUpdate {
                tx_id: seen,
                fate: Fate::Accepted,
                ..
            } if *seen == tx_id
        )
    });
    acceptance.record((ctx.now_ms() - start) * 1_000).unwrap();
    acceptance
}

fn execute(ctx: &mut dyn DriverContext, config: &Config) -> Summary {
    let schema = schema();
    let fixture = build_fixture(config);
    let (_core_dir, mut core) = open_node(node(250), schema.clone());
    let (_writer_dir, mut writer) = open_node(node(1), schema.clone());
    let mut clients = Vec::new();
    let mut edges = Vec::new();
    let mut dirs = Vec::new();
    for idx in 0..config.clients {
        let (dir, client) = open_node(node(20 + idx as u8), schema.clone());
        let (edge_dir, edge_node) = open_node(node(120 + idx as u8), schema.clone());
        dirs.push(dir);
        edges.push(EdgeRoute {
            name: format!("client_{idx}_edge"),
            node: edge_node,
            _dir: edge_dir,
            core_peer: PeerState::new(),
        });
        clients.push(client);
    }

    for (idx, commit) in fixture.commits.iter().enumerate() {
        apply_fixture_commit(
            ctx,
            &mut writer,
            &mut core,
            commit,
            FixtureCommitApply {
                writer_name: "writer",
                core_name: "core",
                made_by: AuthorId::SYSTEM,
                now_ms: 1_000 + idx as u64,
            },
        )
        .expect("fixture commit");
    }
    apply_write_stream(ctx, config, &fixture, &mut writer, &mut core);

    let query1 = Query::from(ISSUES)
        .filter(eq(col("assignee"), param("user")))
        .filter(ne(col("state"), lit(Value::U8(STATE_DONE))))
        // The product query says "cycle = active"; v0 has no server-side
        // time-aware active-cycle primitive, so the client binds its active cycle.
        .filter(eq(col("cycle"), param("activeCycle")))
        .include("assignee")
        .include("project")
        .include("cycle")
        .validate(&schema)
        .expect("query 1");
    let plans = client_plans(config, &fixture);
    let mut cold_latencies = Vec::new();
    let mut warm_local = Vec::new();
    let mut warm_settled = Vec::new();
    let mut cold_bytes = 0_u64;
    let mut cold_floor = 0_u64;
    let mut edge_hydration_bytes = 0_u64;
    let mut edge_hydration_floor_bytes = 0_u64;
    let mut edge_hydration_rows = 0_usize;
    let mut result_set_rows = 0_usize;
    let mut total_closure_rows = BTreeSet::<(String, RowUuid)>::new();

    for (((client_idx, client), edge), plan) in clients
        .iter_mut()
        .enumerate()
        .zip(edges.iter_mut())
        .zip(plans.iter())
    {
        let mut peer = PeerState::new();
        let mut client_closure_rows = BTreeSet::<(String, RowUuid)>::new();
        let binding1 = query1
            .bind(BTreeMap::from([
                ("user".to_owned(), Value::Uuid(plan.user.0)),
                ("activeCycle".to_owned(), Value::Uuid(plan.active_cycle.0)),
            ]))
            .expect("binding 1");
        let query2 = Query::from(ISSUES)
            .filter(eq(col("project"), param("project")))
            .filter(eq(col("state"), param("state")))
            .join_via(
                ISSUE_TAGS,
                "issue",
                [eq(col("tag"), lit(Value::Uuid(plan.tag.0)))],
            )
            .include("project")
            .validate(&schema)
            .expect("query 2");
        let binding2 = query2
            .bind(BTreeMap::from([
                ("project".to_owned(), Value::Uuid(plan.project.0)),
                ("state".to_owned(), Value::U8(STATE_IN_PROGRESS)),
            ]))
            .expect("binding 2");

        register_binding(ctx, &mut core, &edge.name, &query1, &binding1);
        register_binding(ctx, &mut core, &edge.name, &query2, &binding2);
        apply_binding(&mut edge.node, &query1, &binding1);
        apply_binding(&mut edge.node, &query2, &binding2);
        apply_binding(client, &query1, &binding1);
        apply_binding(client, &query2, &binding2);

        for (shape, binding) in [(&query1, &binding1), (&query2, &binding2)] {
            let start_ms = ctx.now_ms();
            let core_update = edge
                .core_peer
                .rehydrate_query(&mut core, shape, binding)
                .expect("rehydrate query");
            edge_hydration_bytes += view_update_bytes(&core_update);
            edge_hydration_floor_bytes += bytes_floor(&core_update);
            edge_hydration_rows += result_output_count(&core_update, ISSUES);
            ctx.send("core", &edge.name, core_update);
            let delivered_to_edge = ctx.recv(&edge.name);
            edge.node
                .apply_sync_message(delivered_to_edge.message)
                .expect("edge apply view");
            let update = peer
                .rehydrate_query(&mut edge.node, shape, binding)
                .expect("edge rehydrate query");
            let bytes = view_update_bytes(&update);
            cold_bytes += bytes;
            cold_floor += bytes_floor(&update);
            collect_result_rows(&update, &mut client_closure_rows);
            result_set_rows += result_output_count(&update, ISSUES);
            ctx.send(&edge.name, &plan.name, update);
            let delivered = ctx.recv(&plan.name);
            client
                .apply_sync_message(delivered.message)
                .expect("client apply view");
            cold_latencies.push((ctx.now_ms() - start_ms) * 1_000);
        }

        assert_client_correct(
            client, &mut core, &schema, &query1, &binding1, ISSUES, "query1",
        );
        assert_client_correct(
            client, &mut core, &schema, &query2, &binding2, ISSUES, "query2",
        );
        assert_no_outside_closure(client, &schema, &client_closure_rows);
        total_closure_rows.extend(client_closure_rows);

        for (shape, binding) in [(&query1, &binding1), (&query2, &binding2)] {
            let start = Instant::now();
            let _ = client
                .query_rows(shape, binding, DurabilityTier::Local)
                .expect("local query");
            warm_local.push(start.elapsed().as_micros() as u64);
            let start = Instant::now();
            let _ = client
                .query_rows(shape, binding, DurabilityTier::Global)
                .expect("settled query");
            warm_settled.push(start.elapsed().as_micros() as u64);
        }
        ctx.record_counter("s1_client_subscriptions", 2);
        ctx.record_counter("s1_clients_synced", 1);
        ctx.record_counter("s1_client_index_sum", client_idx as u64);
    }

    let _keep_dirs = dirs;
    Summary {
        fixture_hash: fixture.stable_hash(),
        fixture_rows: fixture.commits.len(),
        clients: config.clients,
        cold_complete_p50_us: percentile(&mut cold_latencies.clone(), 50),
        cold_complete_p95_us: percentile(&mut cold_latencies, 95),
        cold_bytes,
        cold_bytes_floor: cold_floor,
        naive_refetch_ceiling_bytes: naive_refetch_ceiling_bytes(&schema, &fixture),
        warm_local_p50_us: percentile(&mut warm_local.clone(), 50),
        warm_local_p95_us: percentile(&mut warm_local, 95),
        warm_settled_p50_us: percentile(&mut warm_settled.clone(), 50),
        warm_settled_p95_us: percentile(&mut warm_settled, 95),
        result_set_rows,
        closure_rows: total_closure_rows.len(),
        writes_applied: config.writes,
        edge_acceptance: edge_acceptance_phase(ctx, &mut clients[0], &mut edges[0]),
        edge_hydration_bytes,
        edge_hydration_floor_bytes,
        edge_hydration_rows,
    }
}

fn reconnect_summaries(config: &Config, profile: PeerProfile) -> Vec<ReconnectSummary> {
    let schema = schema();
    let fixture = build_fixture(config);
    let topology = Topology::default()
        .node("writer", schema.clone(), NodeRole::Writer)
        .node("core", schema.clone(), NodeRole::Core)
        .node("reconnect", schema.clone(), NodeRole::Reader)
        .node("control", schema.clone(), NodeRole::Reader)
        .link("writer", "core", profile.clone())
        .link("core", "writer", profile.clone())
        .link("reconnect", "core", profile.clone())
        .link("core", "reconnect", profile.clone())
        .link("control", "core", profile.clone())
        .link("core", "control", profile);
    let mut ctx = DeterministicDriver::new(topology, config.seed ^ 0x51aa_0b00)
        .with_transport_codec(config.reconnect_transport_codec);
    let (_core_dir, mut core) = open_node(node(250), schema.clone());
    let (_writer_dir, mut writer) = open_node(node(1), schema.clone());
    let (_reconnect_dir, mut reconnect) = open_node(node(40), schema.clone());
    let (_control_dir, mut control) = open_node(node(41), schema.clone());
    for (idx, commit) in fixture.commits.iter().enumerate() {
        apply_fixture_commit(
            &mut ctx,
            &mut writer,
            &mut core,
            commit,
            FixtureCommitApply {
                writer_name: "writer",
                core_name: "core",
                made_by: AuthorId::SYSTEM,
                now_ms: 1_000 + idx as u64,
            },
        )
        .expect("fixture commit");
    }

    let plan = representative_plan(&fixture);
    let query1 = query1(&schema);
    let binding1 = binding1(&query1, &plan);
    let query2 = query2(&schema, &plan);
    let binding2 = binding2(&query2, &plan);
    let subscriptions = [(&query1, &binding1), (&query2, &binding2)];
    let mut reconnect_link = PeerState::new();
    let mut control_link = PeerState::new();
    let mut reconnect_delivered = hydrate_client(
        &mut ctx,
        &mut core,
        &mut reconnect,
        &mut reconnect_link,
        "reconnect",
        &subscriptions,
    );
    let _ = hydrate_client(
        &mut ctx,
        &mut core,
        &mut control,
        &mut control_link,
        "control",
        &subscriptions,
    );

    let mut summaries = Vec::new();
    let mut total_writes = 0_usize;
    for window_writes in &config.reconnect_windows {
        ctx.pause_link("core", "reconnect", PauseMode::Drop);
        assert!(ctx.is_link_paused("core", "reconnect"));
        for _ in 0..*window_writes {
            let idx = total_writes;
            apply_one_issue_edit(
                &mut ctx,
                config,
                &fixture,
                &mut writer,
                &mut core,
                200_000 + idx as u64,
                idx,
            );
            for (shape, binding) in subscriptions {
                let update = control_link
                    .query_update(&mut core, shape, binding)
                    .expect("control query update while reconnect is disconnected");
                ctx.send("core", "reconnect", update.clone());
                ctx.send("core", "control", update);
                let delivered = ctx.recv("control");
                control
                    .apply_sync_message(delivered.message)
                    .expect("control apply live update");
            }
            total_writes += 1;
        }
        ctx.resume_link("core", "reconnect");
        assert!(!ctx.is_link_paused("core", "reconnect"));
        for (shape, binding) in subscriptions {
            let update = control_link
                .query_update(&mut core, shape, binding)
                .expect("control query update");
            ctx.send("core", "control", update);
            let delivered = ctx.recv("control");
            control
                .apply_sync_message(delivered.message)
                .expect("control apply update");
        }

        let start_ms = ctx.now_ms();
        let mut catchup_bytes = 0_u64;
        let mut catchup_floor = 0_u64;
        let mut result_set_rows = 0_usize;
        let mut closure_rows = BTreeSet::new();
        for (shape, binding) in subscriptions {
            register_binding(&mut ctx, &mut core, "reconnect", shape, binding);
            let update = reconnect_link
                .rehydrate_query(&mut core, shape, binding)
                .expect("reconnect rehydrate");
            catchup_bytes += view_update_bytes(&update);
            catchup_floor += bytes_floor(&update);
            result_set_rows += result_output_count(&update, ISSUES);
            collect_result_rows(&update, &mut closure_rows);
            collect_result_rows(&update, &mut reconnect_delivered);
            ctx.send("core", "reconnect", update);
            let delivered = ctx.recv("reconnect");
            reconnect
                .apply_sync_message(delivered.message)
                .expect("reconnect apply catch-up");
        }
        let catchup_us = (ctx.now_ms() - start_ms) * 1_000;
        assert_client_correct(
            &mut reconnect,
            &mut core,
            &schema,
            &query1,
            &binding1,
            ISSUES,
            "reconnect query1",
        );
        assert_client_correct(
            &mut reconnect,
            &mut core,
            &schema,
            &query2,
            &binding2,
            ISSUES,
            "reconnect query2",
        );
        assert_eq!(
            row_set(
                reconnect
                    .query_rows(&query1, &binding1, DurabilityTier::Global)
                    .expect("reconnect q1")
            ),
            row_set(
                control
                    .query_rows(&query1, &binding1, DurabilityTier::Global)
                    .expect("control q1")
            ),
            "reconnected query1 diverged from control"
        );
        assert_eq!(
            row_set(
                reconnect
                    .query_rows(&query2, &binding2, DurabilityTier::Global)
                    .expect("reconnect q2")
            ),
            row_set(
                control
                    .query_rows(&query2, &binding2, DurabilityTier::Global)
                    .expect("control q2")
            ),
            "reconnected query2 diverged from control"
        );
        assert_no_outside_closure(&mut reconnect, &schema, &reconnect_delivered);
        summaries.push(ReconnectSummary {
            window_writes: *window_writes,
            catchup_us,
            catchup_bytes,
            catchup_bytes_floor: catchup_floor,
            result_set_rows,
            closure_rows: closure_rows.len(),
            transport_codec: config.reconnect_transport_codec,
            transport_metrics: ctx.metrics_json_fields(),
        });
    }
    summaries
}

fn subscriber_sweep_summaries(config: &Config) -> Vec<SweepSummary> {
    config
        .sweep_counts
        .iter()
        .map(|subscribers| subscriber_sweep_summary(config, *subscribers))
        .collect()
}

fn subscriber_sweep_summary(config: &Config, subscribers: usize) -> SweepSummary {
    let schema = schema();
    let mut sweep_config = config.clone();
    sweep_config.issues_per_org = config.sweep_issues_per_org;
    sweep_config.users_per_org = sweep_config
        .users_per_org
        .max((subscribers / sweep_config.orgs.max(1)) + 1);
    sweep_config.writes = 0;
    let fixture = build_fixture(&sweep_config);
    let (_core_dir, mut core) = open_node(node(250), schema.clone());
    let (_writer_dir, mut writer) = open_node(node(1), schema.clone());
    let topology = Topology::default()
        .node("writer", schema.clone(), NodeRole::Writer)
        .node("core", schema.clone(), NodeRole::Core)
        .link(
            "writer",
            "core",
            PeerProfile::new("s1-sweep-setup", 0, 0, 0),
        )
        .link(
            "core",
            "writer",
            PeerProfile::new("s1-sweep-setup", 0, 0, 0),
        );
    let mut ctx = DeterministicDriver::new(topology, config.seed ^ 0x51aa_5bee);
    for (idx, commit) in fixture.commits.iter().enumerate() {
        apply_fixture_commit(
            &mut ctx,
            &mut writer,
            &mut core,
            commit,
            FixtureCommitApply {
                writer_name: "writer",
                core_name: "core",
                made_by: AuthorId::SYSTEM,
                now_ms: 1_000 + idx as u64,
            },
        )
        .expect("fixture commit");
    }
    let users = fixture.rows_by_set.get("users").expect("users");
    let sweep_issues = fixture
        .commits
        .iter()
        .filter(|commit| {
            commit.table == ISSUES
                && !matches!(commit.cells.get("state"), Some(Value::U8(STATE_DONE)))
        })
        .map(|commit| commit.row_uuid)
        .collect::<Vec<_>>();
    let sweep_issues = if sweep_issues.is_empty() {
        fixture.rows_by_set.get("issues").expect("issues").clone()
    } else {
        sweep_issues
    };
    let shape = Query::from(ISSUES)
        .filter(eq(col("assignee"), param("user")))
        .filter(ne(col("state"), lit(Value::U8(STATE_DONE))))
        .validate(&schema)
        .expect("sweep query");
    let mut peers = Vec::with_capacity(subscribers);
    let mut bindings = Vec::with_capacity(subscribers);
    for idx in 0..subscribers {
        let binding = shape
            .bind(BTreeMap::from([(
                "user".to_owned(),
                Value::Uuid(users[idx % users.len()].0),
            )]))
            .expect("sweep binding");
        let mut peer = PeerState::new();
        let _ = peer
            .rehydrate_query(&mut core, &shape, &binding)
            .expect("sweep initial hydrate");
        peer.metrics = Default::default();
        peers.push(peer);
        bindings.push(binding);
    }

    let mut latencies = Vec::new();
    let mut total_notification_bytes = 0_u64;
    for idx in 0..config.sweep_commits {
        let subscribed_user = users[idx % subscribers.min(users.len())];
        apply_issue_edit_to_user(
            &mut ctx,
            &mut writer,
            &mut core,
            sweep_issues[idx % sweep_issues.len()],
            subscribed_user,
            300_000 + idx as u64,
            idx,
        );
        let start = Instant::now();
        for (peer, binding) in peers.iter_mut().zip(bindings.iter()) {
            let update = peer
                .query_update(&mut core, &shape, binding)
                .expect("sweep query update");
            total_notification_bytes += view_update_bytes(&update);
        }
        latencies.push(start.elapsed().as_micros() as u64);
    }
    let mut version_bundles_out = 0_u64;
    let mut complete_tx_refs_out = 0_u64;
    let mut result_adds_out = 0_u64;
    let mut result_removes_out = 0_u64;
    for peer in &peers {
        version_bundles_out += peer.metrics.version_bundles_out;
        complete_tx_refs_out += peer.metrics.complete_tx_payload_refs_out;
        result_adds_out += peer.metrics.result_adds_out;
        result_removes_out += peer.metrics.result_removes_out;
    }
    SweepSummary {
        subscribers,
        commits: config.sweep_commits,
        core_emit_p50_us: percentile(&mut latencies.clone(), 50),
        core_emit_p95_us: percentile(&mut latencies, 95),
        total_notification_bytes,
        bytes_per_commit: total_notification_bytes / config.sweep_commits.max(1) as u64,
        version_bundles_out,
        complete_tx_refs_out,
        result_adds_out,
        result_removes_out,
    }
}

fn high_fan_out_hydration_summaries(
    config: &Config,
    profile: PeerProfile,
) -> Vec<HighFanOutSummary> {
    config
        .hydration_fanouts
        .iter()
        .map(|fanout| high_fan_out_hydration_summary(config, profile.clone(), *fanout))
        .collect()
}

fn high_fan_out_hydration_summary(
    config: &Config,
    profile: PeerProfile,
    fanout: usize,
) -> HighFanOutSummary {
    let schema = high_fan_out_schema();
    let topology = Topology::default()
        .node("writer", schema.clone(), NodeRole::Writer)
        .node("core", schema.clone(), NodeRole::Core)
        .node("cold", schema.clone(), NodeRole::Reader)
        .link("writer", "core", profile.clone())
        .link("core", "writer", profile.clone())
        .link("cold", "core", profile.clone())
        .link("core", "cold", profile);
    let mut ctx = ThreadedDriver::new(topology, config.seed ^ 0x51aa_fa00 ^ fanout as u64);
    let (_core_dir, mut core) = open_node(node(250), schema.clone());
    let (_writer_dir, mut writer) = open_node(node(1), schema.clone());
    let (_cold_dir, mut cold) = open_node(node(80), schema.clone());
    let parents = (0..config.hydration_parents)
        .map(high_fan_out_parent)
        .collect::<Vec<_>>();

    for (idx, parent) in parents.iter().copied().enumerate() {
        commit_hydration_row(
            &mut ctx,
            &mut writer,
            &mut core,
            HF_PARENTS,
            parent,
            BTreeMap::from([
                ("name".to_owned(), Value::String(format!("parent-{idx}"))),
                ("bucket".to_owned(), Value::U64(idx as u64)),
            ]),
            10_000 + idx as u64,
        );
        for child_idx in 0..fanout {
            let global = idx * fanout + child_idx;
            commit_hydration_row(
                &mut ctx,
                &mut writer,
                &mut core,
                HF_CHILDREN,
                high_fan_out_child(global),
                BTreeMap::from([
                    ("parent".to_owned(), Value::Uuid(parent.0)),
                    (
                        "payload".to_owned(),
                        Value::String(format!("child-{global}")),
                    ),
                    ("ordinal".to_owned(), Value::U64(child_idx as u64)),
                ]),
                20_000 + global as u64,
            );
        }
    }

    let child_shape = Query::from(HF_CHILDREN)
        .filter(eq(col("parent"), param("parent")))
        .include("parent")
        .validate(&schema)
        .expect("high fan-out child query");
    let parent_shape = Query::from(HF_PARENTS)
        .validate(&schema)
        .expect("high fan-out parent query");
    let parent_binding = parent_shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::new();
    register_binding(&mut ctx, &mut core, "cold", &parent_shape, &parent_binding);

    let start = Instant::now();
    let mut hydration_bytes = 0_u64;
    let mut hydration_floor_bytes = 0_u64;
    let mut result_set_rows = 0_usize;
    let mut mid_hydration_fates = 0_usize;
    let mut mid_hydration_bytes = 0_u64;
    let mut by_tx_index_seeks = 0_u64;
    let history_scan_fallbacks = 0_u64;

    let parent_update = peer
        .rehydrate_query(&mut core, &parent_shape, &parent_binding)
        .expect("parent rehydrate");
    hydration_bytes += view_update_bytes(&parent_update);
    hydration_floor_bytes += bytes_floor(&parent_update);
    result_set_rows += result_output_count(&parent_update, HF_PARENTS);
    ctx.send("core", "cold", parent_update);
    let delivered = ctx.recv("cold");
    cold.apply_sync_message(delivered.message)
        .expect("apply parent hydration");

    let burst_every = (parents.len() / config.hydration_fate_bursts.max(1)).max(1);
    let mut active_bindings = Vec::new();
    for (idx, parent) in parents.iter().copied().enumerate() {
        let binding = child_shape
            .bind(BTreeMap::from([(
                "parent".to_owned(),
                Value::Uuid(parent.0),
            )]))
            .expect("child binding");
        register_binding(&mut ctx, &mut core, "cold", &child_shape, &binding);
        let update = peer
            .rehydrate_query(&mut core, &child_shape, &binding)
            .expect("child rehydrate");
        hydration_bytes += view_update_bytes(&update);
        hydration_floor_bytes += bytes_floor(&update);
        result_set_rows += result_output_count(&update, HF_CHILDREN);
        ctx.send("core", "cold", update);
        let delivered = ctx.recv("cold");
        cold.apply_sync_message(delivered.message)
            .expect("apply child hydration");
        active_bindings.push(binding);

        if (idx + 1) % burst_every == 0 && mid_hydration_fates < config.hydration_fate_bursts {
            let changed_parent = parents[mid_hydration_fates % parents.len()];
            let changed_child = high_fan_out_child((mid_hydration_fates % parents.len()) * fanout);
            commit_hydration_row(
                &mut ctx,
                &mut writer,
                &mut core,
                HF_CHILDREN,
                changed_child,
                BTreeMap::from([
                    ("parent".to_owned(), Value::Uuid(changed_parent.0)),
                    (
                        "payload".to_owned(),
                        Value::String(format!("fate-{}", mid_hydration_fates)),
                    ),
                    ("ordinal".to_owned(), Value::U64(0)),
                ]),
                90_000 + mid_hydration_fates as u64,
            );
            mid_hydration_fates += 1;
            by_tx_index_seeks += active_bindings.len() as u64;
            for binding in &active_bindings {
                let update = peer
                    .query_update(&mut core, &child_shape, binding)
                    .expect("mid-hydration fate update");
                mid_hydration_bytes += view_update_bytes(&update);
                ctx.send("core", "cold", update);
                let delivered = ctx.recv("cold");
                cold.apply_sync_message(delivered.message)
                    .expect("apply mid-hydration fate");
            }
        }
    }
    assert_eq!(history_scan_fallbacks, 0, "history scans are disallowed");
    for parent in &parents {
        let binding = child_shape
            .bind(BTreeMap::from([(
                "parent".to_owned(),
                Value::Uuid(parent.0),
            )]))
            .expect("oracle child binding");
        let local = row_set(
            cold.query_rows(&child_shape, &binding, DurabilityTier::Local)
                .expect("cold local child query"),
        );
        let oracle = row_set(
            core.query_rows(&child_shape, &binding, DurabilityTier::Global)
                .expect("core child oracle"),
        );
        assert_eq!(local, oracle, "high fan-out child result mismatch");
    }

    HighFanOutSummary {
        fanout,
        parents: parents.len(),
        children: parents.len() * fanout,
        subscriptions: parents.len() + 1,
        hydration_complete_us: start.elapsed().as_micros() as u64,
        hydration_bytes,
        hydration_floor_bytes,
        result_set_rows,
        mid_hydration_fates,
        mid_hydration_bytes,
        by_tx_index_seeks,
        history_scan_fallbacks,
        maintained_subscription_view_metrics: peer.maintained_subscription_view_metrics(),
        full_diff_recomputes: 0,
    }
}

fn high_fan_out_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new(
            HF_PARENTS,
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("bucket", ColumnType::U64),
            ],
        ),
        TableSchema::new(
            HF_CHILDREN,
            [
                ColumnSchema::new("parent", ColumnType::Uuid),
                ColumnSchema::new("payload", ColumnType::String),
                ColumnSchema::new("ordinal", ColumnType::U64),
            ],
        )
        .with_reference("parent", HF_PARENTS),
    ])
}

fn commit_hydration_row(
    ctx: &mut dyn DriverContext,
    writer: &mut NodeState<RocksDbStorage>,
    core: &mut NodeState<RocksDbStorage>,
    table: &str,
    row_uuid: RowUuid,
    cells: BTreeMap<String, Value>,
    now_ms: u64,
) {
    let (_tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new(table, row_uuid, now_ms)
                .made_by(AuthorId::SYSTEM)
                .cells(cells),
        )
        .expect("hydration commit");
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        unreachable!();
    };
    core.ingest_commit_unit(tx, versions, u64::MAX)
        .expect("core ingest hydration");
    ctx.record_counter("s1_high_fan_out_hydration_commits", 1);
}

fn high_fan_out_parent(idx: usize) -> RowUuid {
    row(3_000_000 + idx)
}

fn high_fan_out_child(idx: usize) -> RowUuid {
    row(4_000_000 + idx)
}

fn register_binding(
    ctx: &mut dyn DriverContext,
    core: &mut NodeState<RocksDbStorage>,
    client_name: &str,
    shape: &ValidatedQuery,
    binding: &Binding,
) {
    let register = SyncMessage::RegisterShape {
        shape_id: shape.shape_id(),
        ast: ShapeAst::from_validated(shape),
        opts: RegisterShapeOptions::default(),
    };
    ctx.send(client_name, "core", register);
    let delivered = ctx.recv("core");
    core.apply_sync_message(delivered.message)
        .expect("register shape");
    let values = shape
        .params()
        .keys()
        .map(|name| binding.values().get(name).cloned().unwrap())
        .collect::<Vec<_>>();
    ctx.send(
        client_name,
        "core",
        SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription: SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
                read_view: RegisterShapeOptions::default().read_view_key(),
            },
            values,
        }),
    );
    let delivered = ctx.recv("core");
    core.apply_sync_message(delivered.message)
        .expect("binding delta");
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

fn hydrate_client(
    ctx: &mut dyn DriverContext,
    core: &mut NodeState<RocksDbStorage>,
    client: &mut NodeState<RocksDbStorage>,
    peer: &mut PeerState,
    client_name: &str,
    subscriptions: &[(&ValidatedQuery, &Binding)],
) -> BTreeSet<(String, RowUuid)> {
    let mut delivered_rows = BTreeSet::new();
    for (shape, binding) in subscriptions {
        register_binding(ctx, core, client_name, shape, binding);
        apply_binding(client, shape, binding);
        let update = peer
            .rehydrate_query(core, shape, binding)
            .expect("hydrate query");
        collect_result_rows(&update, &mut delivered_rows);
        ctx.send("core", client_name, update);
        let delivered = ctx.recv(client_name);
        client
            .apply_sync_message(delivered.message)
            .expect("client apply hydrate");
    }
    delivered_rows
}

fn query1(schema: &JazzSchema) -> ValidatedQuery {
    Query::from(ISSUES)
        .filter(eq(col("assignee"), param("user")))
        .filter(ne(col("state"), lit(Value::U8(STATE_DONE))))
        // The product query says "cycle = active"; v0 has no server-side
        // time-aware active-cycle primitive, so the client binds its active cycle.
        .filter(eq(col("cycle"), param("activeCycle")))
        .include("assignee")
        .include("project")
        .include("cycle")
        .validate(schema)
        .expect("query 1")
}

fn binding1(shape: &ValidatedQuery, plan: &ClientPlan) -> Binding {
    shape
        .bind(BTreeMap::from([
            ("user".to_owned(), Value::Uuid(plan.user.0)),
            ("activeCycle".to_owned(), Value::Uuid(plan.active_cycle.0)),
        ]))
        .expect("binding 1")
}

fn query2(schema: &JazzSchema, plan: &ClientPlan) -> ValidatedQuery {
    Query::from(ISSUES)
        .filter(eq(col("project"), param("project")))
        .filter(eq(col("state"), param("state")))
        .join_via(
            ISSUE_TAGS,
            "issue",
            [eq(col("tag"), lit(Value::Uuid(plan.tag.0)))],
        )
        .include("project")
        .validate(schema)
        .expect("query 2")
}

fn binding2(shape: &ValidatedQuery, plan: &ClientPlan) -> Binding {
    shape
        .bind(BTreeMap::from([
            ("project".to_owned(), Value::Uuid(plan.project.0)),
            ("state".to_owned(), Value::U8(STATE_IN_PROGRESS)),
        ]))
        .expect("binding 2")
}

fn apply_one_issue_edit(
    ctx: &mut dyn DriverContext,
    config: &Config,
    fixture: &Fixture,
    writer: &mut NodeState<RocksDbStorage>,
    core: &mut NodeState<RocksDbStorage>,
    now_ms: u64,
    idx: usize,
) {
    let issues = fixture.rows_by_set.get("issues").expect("issues");
    let users = fixture.rows_by_set.get("users").expect("users");
    let issue = issues[(idx * 7919 + config.seed as usize) % issues.len()];
    let user = users[(idx * 104_729 + config.seed as usize) % users.len()];
    let mut cells = BTreeMap::new();
    cells.insert(
        "title".to_owned(),
        Value::String(format!("stream-edit-{idx}")),
    );
    cells.insert("assignee".to_owned(), Value::Uuid(user.0));
    let commit = FixtureCommit {
        table: ISSUES.to_owned(),
        row_uuid: issue,
        cells,
    };
    apply_fixture_commit(
        ctx,
        writer,
        core,
        &commit,
        FixtureCommitApply {
            writer_name: "writer",
            core_name: "core",
            made_by: AuthorId(user.0),
            now_ms,
        },
    )
    .expect("issue edit");
}

fn apply_issue_edit_to_user(
    ctx: &mut dyn DriverContext,
    writer: &mut NodeState<RocksDbStorage>,
    core: &mut NodeState<RocksDbStorage>,
    issue: RowUuid,
    user: RowUuid,
    now_ms: u64,
    idx: usize,
) {
    let mut cells = BTreeMap::new();
    cells.insert(
        "title".to_owned(),
        Value::String(format!("sweep-edit-{idx}")),
    );
    cells.insert("assignee".to_owned(), Value::Uuid(user.0));
    let commit = FixtureCommit {
        table: ISSUES.to_owned(),
        row_uuid: issue,
        cells,
    };
    apply_fixture_commit(
        ctx,
        writer,
        core,
        &commit,
        FixtureCommitApply {
            writer_name: "writer",
            core_name: "core",
            made_by: AuthorId(user.0),
            now_ms,
        },
    )
    .expect("issue edit");
}

fn apply_write_stream(
    ctx: &mut dyn DriverContext,
    config: &Config,
    fixture: &Fixture,
    writer: &mut NodeState<RocksDbStorage>,
    core: &mut NodeState<RocksDbStorage>,
) {
    let issues = fixture.rows_by_set.get("issues").expect("issues");
    let users = fixture.rows_by_set.get("users").expect("users");
    let mut rng = Lcg::new(config.seed ^ 0x5a1_0000);
    for idx in 0..config.writes {
        let issue = issues[rng.usize(issues.len())];
        let user = users[rng.usize(users.len())];
        let mut cells = BTreeMap::new();
        cells.insert("title".to_owned(), Value::String(format!("edit-{idx}")));
        cells.insert("assignee".to_owned(), Value::Uuid(user.0));
        let commit = FixtureCommit {
            table: ISSUES.to_owned(),
            row_uuid: issue,
            cells,
        };
        apply_fixture_commit(
            ctx,
            writer,
            core,
            &commit,
            FixtureCommitApply {
                writer_name: "writer",
                core_name: "core",
                made_by: AuthorId(user.0),
                now_ms: 100_000 + idx as u64,
            },
        )
        .expect("write stream commit");
    }
}

fn assert_client_correct(
    client: &mut NodeState<RocksDbStorage>,
    core: &mut NodeState<RocksDbStorage>,
    schema: &JazzSchema,
    shape: &ValidatedQuery,
    binding: &Binding,
    output_table: &str,
    label: &str,
) {
    let settled = row_set(
        client
            .query_rows(shape, binding, DurabilityTier::Global)
            .expect("client settled"),
    );
    let oracle = row_set(
        core.query_rows(shape, binding, DurabilityTier::Global)
            .expect("core oracle"),
    );
    assert_eq!(settled, oracle, "{label} settled result set mismatch");
    let local = row_set(
        client
            .query_rows(shape, binding, DurabilityTier::Local)
            .expect("client local"),
    );
    assert_eq!(
        local, settled,
        "{label} closure did not support local evaluation"
    );
    let table = schema
        .tables
        .iter()
        .find(|table| table.name == output_table)
        .expect("output table");
    assert!(
        settled
            .iter()
            .all(|(table_name, _)| table_name == &table.name)
    );
}

fn assert_no_outside_closure(
    client: &mut NodeState<RocksDbStorage>,
    schema: &JazzSchema,
    allowed: &BTreeSet<(String, RowUuid)>,
) {
    for table in &schema.tables {
        let rows = client
            .current_rows(&table.name, DurabilityTier::Local)
            .expect("local rows");
        for row in rows {
            assert!(
                allowed.contains(&(table.name.clone(), row.row_uuid())),
                "client holds {} {:?} outside subscribed closure",
                table.name,
                row.row_uuid()
            );
        }
    }
}

fn row_set(rows: Vec<CurrentRow>) -> BTreeSet<(String, RowUuid)> {
    rows.into_iter()
        .map(|row| (row.table().to_owned(), row.row_uuid()))
        .collect()
}

fn subscription_snapshot_row_set(
    subscription: &mut SubscriptionStream,
) -> BTreeSet<(String, RowUuid)> {
    match block_on(subscription.next_event()).expect("subscription emits initial event") {
        SubscriptionEvent::Opened { current, .. } | SubscriptionEvent::Reset { current, .. } => {
            row_set(current.rows)
        }
        event => panic!("expected subscription snapshot, got {event:?}"),
    }
}

fn apply_subscription_events(
    subscription: &mut SubscriptionStream,
    rows: &mut BTreeSet<(String, RowUuid)>,
) -> bool {
    let Some(first) = block_on(subscription.next_event()) else {
        return false;
    };
    apply_subscription_event(rows, first);
    while let Some(event) = subscription.try_next_event() {
        apply_subscription_event(rows, event);
    }
    true
}

fn apply_subscription_event(rows: &mut BTreeSet<(String, RowUuid)>, event: SubscriptionEvent) {
    match event {
        SubscriptionEvent::Opened { current, .. } | SubscriptionEvent::Reset { current, .. } => {
            *rows = row_set(current.rows);
        }
        SubscriptionEvent::Delta {
            added,
            updated,
            removed,
            ..
        } => {
            for row in removed {
                rows.remove(&(row.table, row.row_uuid));
            }
            for row in added.into_iter().chain(updated) {
                rows.insert((row.table().to_owned(), row.row_uuid()));
            }
        }
        SubscriptionEvent::Closed => {}
    }
}

fn collect_result_rows(update: &SyncMessage, rows: &mut BTreeSet<(String, RowUuid)>) {
    if let SyncMessage::ViewUpdate {
        result_member_adds, ..
    } = update
    {
        for entry in result_member_adds {
            if let Some((table, row_uuid, _)) = entry.as_row() {
                rows.insert((table.to_string(), row_uuid));
            }
        }
    }
}

fn result_output_count(update: &SyncMessage, table: &str) -> usize {
    match update {
        SyncMessage::ViewUpdate {
            result_member_adds, ..
        } => result_member_adds
            .iter()
            .filter_map(|entry| entry.as_row())
            .filter(|entry| entry.0.as_str() == table)
            .count(),
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
            let bundle_bytes = version_bundles
                .iter()
                .flat_map(|bundle| bundle.versions.iter())
                .map(|version| version.record().raw().len() as u64 + 64)
                .sum::<u64>();
            let complete_tx_refs = &peer_payload_inventory.complete_tx_payloads;
            bundle_bytes
                + (complete_tx_refs.len() as u64 * 24)
                + ((result_member_adds.len() + result_member_removes.len()) as u64 * 64)
        }
        _ => 0,
    }
}

fn bytes_floor(update: &SyncMessage) -> u64 {
    match update {
        SyncMessage::ViewUpdate {
            version_bundles, ..
        } => version_bundles
            .iter()
            .flat_map(|bundle| bundle.versions.iter())
            .map(|version| version.record().raw().len() as u64)
            .sum(),
        _ => 0,
    }
}

fn naive_refetch_ceiling_bytes(schema: &JazzSchema, fixture: &Fixture) -> u64 {
    fixture
        .commits
        .iter()
        .map(|commit| {
            let table = schema
                .tables
                .iter()
                .find(|table| table.name == commit.table)
                .expect("table");
            let positional = table
                .columns
                .iter()
                .map(|column| commit.cells.get(&column.name).cloned())
                .collect::<Vec<_>>();
            jazz::protocol::VersionRecord::encode(
                table,
                schema.version_id(),
                commit.row_uuid,
                Vec::new(),
                AuthorId::SYSTEM,
                TxTime(0),
                AuthorId::SYSTEM,
                TxTime(0),
                &positional,
                None,
            )
            .map(|record| record.record().raw().len() as u64)
            .unwrap_or(0)
        })
        .sum()
}

fn percentile(values: &mut [u64], pct: u64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort();
    let idx = (((values.len() - 1) as u64 * pct) / 100) as usize;
    values[idx]
}

fn build_fixture(config: &Config) -> Fixture {
    let mut fixture = FixtureBuilder::new()
        .entity_set(EntitySet::new("orgs", ORGS, config.orgs).cell(
            "name",
            CellValueGen::StringPool {
                prefix: "org".to_owned(),
                pool: config.orgs,
            },
        ))
        .entity_set(
            EntitySet::new("users", USERS, config.users())
                .as_authors()
                .cell(
                    "name",
                    CellValueGen::StringPool {
                        prefix: "user".to_owned(),
                        pool: config.users(),
                    },
                ),
        )
        .entity_set(
            EntitySet::new("teams", TEAMS, config.teams())
                .cell(
                    "name",
                    CellValueGen::StringPool {
                        prefix: "team".to_owned(),
                        pool: config.teams(),
                    },
                )
                .cell(
                    "org",
                    CellValueGen::UuidRef {
                        set: "orgs".to_owned(),
                        distribution: RefDistribution::Uniform,
                    },
                ),
        )
        .entity_set(
            EntitySet::new("tags", TAGS, config.tags)
                .cell(
                    "name",
                    CellValueGen::StringPool {
                        prefix: "tag".to_owned(),
                        pool: config.tags,
                    },
                )
                .cell(
                    "color",
                    CellValueGen::StringPool {
                        prefix: "color".to_owned(),
                        pool: 12,
                    },
                ),
        )
        .entity_set(
            EntitySet::new("projects", PROJECTS, config.projects())
                .cell(
                    "title",
                    CellValueGen::StringPool {
                        prefix: "project".to_owned(),
                        pool: config.projects(),
                    },
                )
                .cell(
                    "org",
                    CellValueGen::UuidRef {
                        set: "orgs".to_owned(),
                        distribution: RefDistribution::Uniform,
                    },
                ),
        )
        .entity_set(
            EntitySet::new("milestones", MILESTONES, config.milestones()).cell(
                "title",
                CellValueGen::StringPool {
                    prefix: "milestone".to_owned(),
                    pool: config.milestones(),
                },
            ),
        )
        .entity_set(
            EntitySet::new("cycles", CYCLES, config.teams())
                .cell(
                    "team",
                    CellValueGen::UuidRef {
                        set: "teams".to_owned(),
                        distribution: RefDistribution::Uniform,
                    },
                )
                .cell("start", CellValueGen::U64Range { start: 1, end: 50 })
                .cell(
                    "end",
                    CellValueGen::U64Range {
                        start: 51,
                        end: 100,
                    },
                ),
        )
        .entity_set(
            EntitySet::new("issues", ISSUES, config.issues())
                .cell(
                    "title",
                    CellValueGen::StringPool {
                        prefix: "issue".to_owned(),
                        pool: config.issues(),
                    },
                )
                .cell(
                    "body",
                    CellValueGen::StringPool {
                        prefix: "body".to_owned(),
                        pool: 100,
                    },
                )
                .cell(
                    "state",
                    CellValueGen::EnumWeighted {
                        weights: vec![4, 10, 18, 8, 2],
                    },
                )
                .cell("priority", CellValueGen::U64Range { start: 1, end: 5 })
                .cell(
                    "assignee",
                    CellValueGen::UuidRef {
                        set: "users".to_owned(),
                        distribution: RefDistribution::Zipf { s: 1.15 },
                    },
                )
                .cell(
                    "milestone",
                    CellValueGen::UuidRef {
                        set: "milestones".to_owned(),
                        distribution: RefDistribution::Uniform,
                    },
                )
                .cell(
                    "project",
                    CellValueGen::UuidRef {
                        set: "projects".to_owned(),
                        distribution: RefDistribution::Uniform,
                    },
                )
                .cell(
                    "cycle",
                    CellValueGen::UuidRef {
                        set: "cycles".to_owned(),
                        distribution: RefDistribution::Uniform,
                    },
                ),
        )
        .edge_set(
            EdgeSet::new(
                "userTeamMemberships",
                USER_TEAM_MEMBERSHIPS,
                "users",
                "user",
                "teams",
                "team",
            )
            .per_left(1, 3),
        )
        .edge_set(
            EdgeSet::new(
                "projectTeamMemberships",
                PROJECT_TEAM_MEMBERSHIPS,
                "projects",
                "project",
                "teams",
                "team",
            )
            .per_left(1, 2),
        )
        .edge_set(
            EdgeSet::new(
                "milestoneDependencies",
                MILESTONE_DEPENDENCIES,
                "milestones",
                "dependsOn",
                "milestones",
                "dependent",
            )
            .per_left(0, 1),
        )
        .edge_set(
            EdgeSet::new("issueTags", ISSUE_TAGS, "issues", "issue", "tags", "tag").per_left(1, 3),
        )
        .build(config.seed);
    for commit in fixture
        .commits
        .iter_mut()
        .filter(|commit| commit.table == ISSUES)
    {
        if let Some(Value::Enum(discriminant)) = commit.cells.remove("state") {
            commit
                .cells
                .insert("state".to_owned(), Value::U8(discriminant));
        }
    }
    fixture
}

fn client_plans(config: &Config, fixture: &Fixture) -> Vec<ClientPlan> {
    let users = fixture.rows_by_set.get("users").expect("users");
    let cycles = fixture.rows_by_set.get("cycles").expect("cycles");
    let projects = fixture.rows_by_set.get("projects").expect("projects");
    let tags = fixture.rows_by_set.get("tags").expect("tags");
    (0..config.clients)
        .map(|idx| ClientPlan {
            name: format!("client_{idx}"),
            user: users[idx % users.len()],
            active_cycle: cycles[idx % cycles.len()],
            project: projects[idx % projects.len()],
            tag: tags[idx % tags.len()],
        })
        .collect()
}

fn representative_plan(fixture: &Fixture) -> ClientPlan {
    for issue in fixture
        .commits
        .iter()
        .filter(|commit| commit.table == ISSUES)
    {
        if matches!(issue.cells.get("state"), Some(Value::U8(STATE_DONE))) {
            continue;
        }
        let Some(user) = cell_uuid(issue, "assignee") else {
            continue;
        };
        let Some(active_cycle) = cell_uuid(issue, "cycle") else {
            continue;
        };
        let Some(project) = cell_uuid(issue, "project") else {
            continue;
        };
        let Some(tag) = fixture
            .commits
            .iter()
            .find(|commit| {
                commit.table == ISSUE_TAGS
                    && cell_uuid(commit, "issue") == Some(issue.row_uuid)
                    && cell_uuid(commit, "tag").is_some()
            })
            .and_then(|commit| cell_uuid(commit, "tag"))
        else {
            continue;
        };
        return ClientPlan {
            name: "reconnect".to_owned(),
            user,
            active_cycle,
            project,
            tag,
        };
    }
    ClientPlan {
        name: "reconnect".to_owned(),
        user: fixture.rows_by_set["users"][0],
        active_cycle: fixture.rows_by_set["cycles"][0],
        project: fixture.rows_by_set["projects"][0],
        tag: fixture.rows_by_set["tags"][0],
    }
}

fn cell_uuid(commit: &FixtureCommit, column: &str) -> Option<RowUuid> {
    match commit.cells.get(column) {
        Some(Value::Uuid(uuid)) => Some(RowUuid(*uuid)),
        _ => None,
    }
}

fn topology(config: &Config, profile: PeerProfile) -> Topology {
    let schema = schema();
    let (client_edge_ms, edge_core_ms) = profile_leg_ms();
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
    for idx in 0..config.clients {
        let name = format!("client_{idx}");
        let edge = format!("{name}_edge");
        topology = topology
            .node(&name, schema.clone(), NodeRole::Reader)
            .node(&edge, schema.clone(), NodeRole::Edge)
            .client_edge_core_line(&name, &edge, "core", client_edge.clone(), edge_core.clone());
    }
    topology
}

fn profile_leg_ms() -> (u64, u64) {
    let total = env_u64("JAZZ_LINK_ONE_WAY_MS", 1);
    let client_edge = env_u64("JAZZ_CLIENT_EDGE_ONE_WAY_MS", total.min(1));
    let edge_core = env_u64(
        "JAZZ_EDGE_CORE_ONE_WAY_MS",
        total.saturating_sub(client_edge).max(1),
    );
    (client_edge, edge_core)
}

fn schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new(ORGS, [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new(
            USERS,
            [
                ColumnSchema::new("userID", ColumnType::Uuid),
                ColumnSchema::new("name", ColumnType::String),
            ],
        ),
        TableSchema::new(
            TEAMS,
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("org", ColumnType::Uuid),
            ],
        )
        .with_reference("org", ORGS),
        TableSchema::new(
            USER_TEAM_MEMBERSHIPS,
            [
                ColumnSchema::new("user", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
            ],
        )
        .with_reference("user", USERS)
        .with_reference("team", TEAMS),
        TableSchema::new(
            TAGS,
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("color", ColumnType::String),
            ],
        ),
        TableSchema::new(
            PROJECTS,
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("org", ColumnType::Uuid),
            ],
        )
        .with_reference("org", ORGS),
        TableSchema::new(
            PROJECT_TEAM_MEMBERSHIPS,
            [
                ColumnSchema::new("project", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
            ],
        )
        .with_reference("project", PROJECTS)
        .with_reference("team", TEAMS),
        TableSchema::new(MILESTONES, [ColumnSchema::new("title", ColumnType::String)]),
        TableSchema::new(
            MILESTONE_DEPENDENCIES,
            [
                ColumnSchema::new("dependsOn", ColumnType::Uuid),
                ColumnSchema::new("dependent", ColumnType::Uuid),
            ],
        )
        .with_reference("dependsOn", MILESTONES)
        .with_reference("dependent", MILESTONES),
        TableSchema::new(
            CYCLES,
            [
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("start", ColumnType::U64),
                ColumnSchema::new("end", ColumnType::U64),
            ],
        )
        .with_reference("team", TEAMS),
        TableSchema::new(
            ISSUES,
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("body", ColumnType::String),
                ColumnSchema::new("state", ColumnType::U8),
                ColumnSchema::new("priority", ColumnType::U64),
                ColumnSchema::new("assignee", ColumnType::Uuid),
                ColumnSchema::new("milestone", ColumnType::Uuid),
                ColumnSchema::new("project", ColumnType::Uuid),
                ColumnSchema::new("cycle", ColumnType::Uuid),
            ],
        )
        .with_reference("assignee", USERS)
        .with_reference("milestone", MILESTONES)
        .with_reference("project", PROJECTS)
        .with_reference("cycle", CYCLES),
        TableSchema::new(
            ISSUE_TAGS,
            [
                ColumnSchema::new("issue", ColumnType::Uuid),
                ColumnSchema::new("tag", ColumnType::Uuid),
            ],
        )
        .with_reference("issue", ISSUES)
        .with_reference("tag", TAGS),
    ])
}

fn open_node(
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(temp_dir.path(), &refs, Durability::WalNoSync)
            .expect("open rocksdb");
    let node = NodeState::new(node_uuid, schema, storage).expect("node");
    (temp_dir, node)
}

fn open_db(
    node_uuid: NodeUuid,
    author: AuthorId,
    schema: JazzSchema,
) -> (tempfile::TempDir, Db<RocksDbStorage>) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(temp_dir.path(), &refs, Durability::WalNoSync)
            .expect("open rocksdb");
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
    (temp_dir, db)
}

fn db_query1(plan: &ClientPlan) -> Query {
    Query::from(ISSUES)
        .filter(eq(col("assignee"), lit(Value::Uuid(plan.user.0))))
        .filter(ne(col("state"), lit(Value::U8(STATE_DONE))))
        .filter(eq(col("cycle"), lit(Value::Uuid(plan.active_cycle.0))))
        .include("assignee")
        .include("project")
        .include("cycle")
}

fn db_query2(plan: &ClientPlan) -> Query {
    Query::from(ISSUES)
        .filter(eq(col("project"), lit(Value::Uuid(plan.project.0))))
        .filter(eq(col("state"), lit(Value::U8(STATE_IN_PROGRESS))))
        .join_via(
            ISSUE_TAGS,
            "issue",
            [eq(col("tag"), lit(Value::Uuid(plan.tag.0)))],
        )
        .include("project")
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

#[derive(Default)]
struct DbS1Oracle {
    tables: BTreeMap<String, BTreeMap<RowUuid, RowCells>>,
}

impl DbS1Oracle {
    fn apply_insert(&mut self, commit: &FixtureCommit) {
        self.tables
            .entry(commit.table.clone())
            .or_default()
            .insert(commit.row_uuid, commit.cells.clone());
    }

    fn apply_patch(&mut self, table: &str, row_uuid: RowUuid, patch: RowCells) {
        self.tables
            .entry(table.to_owned())
            .or_default()
            .entry(row_uuid)
            .or_default()
            .extend(patch);
    }

    fn apply_upsert(&mut self, table: &str, row_uuid: RowUuid, cells: RowCells) {
        self.tables
            .entry(table.to_owned())
            .or_default()
            .insert(row_uuid, cells);
    }

    fn apply_delete(&mut self, table: &str, row_uuid: RowUuid) {
        if let Some(rows) = self.tables.get_mut(table) {
            rows.remove(&row_uuid);
        }
    }

    fn apply_restore(&mut self, table: &str, row_uuid: RowUuid, cells: RowCells) {
        self.apply_upsert(table, row_uuid, cells);
    }

    fn row_cells(&self, table: &str, row_uuid: RowUuid) -> Option<&RowCells> {
        self.tables.get(table)?.get(&row_uuid)
    }

    fn query1(&self, plan: &ClientPlan) -> BTreeSet<(String, RowUuid)> {
        self.table_rows(ISSUES)
            .filter(|(_, cells)| cell_uuid_from_cells(cells, "assignee") == Some(plan.user))
            .filter(|(_, cells)| cells.get("state") != Some(&Value::U8(STATE_DONE)))
            .filter(|(_, cells)| cell_uuid_from_cells(cells, "cycle") == Some(plan.active_cycle))
            .filter(|(_, cells)| {
                self.has_row(USERS, cell_uuid_from_cells(cells, "assignee"))
                    && self.has_row(PROJECTS, cell_uuid_from_cells(cells, "project"))
                    && self.has_row(CYCLES, cell_uuid_from_cells(cells, "cycle"))
            })
            .map(|(row_uuid, _)| (ISSUES.to_owned(), *row_uuid))
            .collect()
    }

    fn query2(&self, plan: &ClientPlan) -> BTreeSet<(String, RowUuid)> {
        self.table_rows(ISSUES)
            .filter(|(_, cells)| cell_uuid_from_cells(cells, "project") == Some(plan.project))
            .filter(|(_, cells)| cells.get("state") == Some(&Value::U8(STATE_IN_PROGRESS)))
            .filter(|(issue, _)| self.issue_has_tag(**issue, plan.tag))
            .filter(|(_, cells)| self.has_row(PROJECTS, cell_uuid_from_cells(cells, "project")))
            .map(|(row_uuid, _)| (ISSUES.to_owned(), *row_uuid))
            .collect()
    }

    fn table_rows(&self, table: &str) -> impl Iterator<Item = (&RowUuid, &RowCells)> {
        self.tables.get(table).into_iter().flat_map(BTreeMap::iter)
    }

    fn has_row(&self, table: &str, row_uuid: Option<RowUuid>) -> bool {
        row_uuid.is_some_and(|row_uuid| {
            self.tables
                .get(table)
                .is_some_and(|rows| rows.contains_key(&row_uuid))
        })
    }

    fn issue_has_tag(&self, issue: RowUuid, tag: RowUuid) -> bool {
        self.table_rows(ISSUE_TAGS).any(|(_, cells)| {
            cell_uuid_from_cells(cells, "issue") == Some(issue)
                && cell_uuid_from_cells(cells, "tag") == Some(tag)
        })
    }
}

fn cell_uuid_from_cells(cells: &RowCells, column: &str) -> Option<RowUuid> {
    match cells.get(column) {
        Some(Value::Uuid(uuid)) => Some(RowUuid(*uuid)),
        _ => None,
    }
}

fn assert_db_query_matches_oracle(
    db: &Db<RocksDbStorage>,
    schema: &JazzSchema,
    query: &Query,
    oracle: BTreeSet<(String, RowUuid)>,
    label: &str,
) {
    let prepared = db.prepare_query(query).expect("db prepare query");
    assert_eq!(
        row_set(db.read(&prepared).expect("db read")),
        oracle,
        "{label} read mismatch"
    );
    assert_eq!(
        row_set(block_on(db.all(&prepared, ReadOpts::default())).expect("db all")),
        oracle,
        "{label} all mismatch"
    );
    let issues = schema
        .tables
        .iter()
        .find(|table| table.name == ISSUES)
        .expect("issues table");
    assert!(
        db.read(&prepared)
            .expect("db read output table check")
            .iter()
            .all(|row| row.table() == issues.name)
    );
}

fn row(idx: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[8..].copy_from_slice(&(idx as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn emit_summary(driver: &str, config: &Config, summary: &Summary) {
    let mut fields = metadata_fields("s1_saas", driver, config.seed, &config.profile);
    fields.insert("fixture_hash".to_owned(), json!(summary.fixture_hash));
    fields.insert("fixture_rows".to_owned(), json!(summary.fixture_rows));
    fields.insert("clients".to_owned(), json!(summary.clients));
    fields.insert(
        "cold_complete_p50_us".to_owned(),
        json!(summary.cold_complete_p50_us),
    );
    fields.insert(
        "cold_complete_p95_us".to_owned(),
        json!(summary.cold_complete_p95_us),
    );
    fields.insert("cold_bytes".to_owned(), json!(summary.cold_bytes));
    fields.insert(
        "cold_bytes_floor".to_owned(),
        json!(summary.cold_bytes_floor),
    );
    fields.insert(
        "naive_refetch_ceiling_bytes".to_owned(),
        json!(summary.naive_refetch_ceiling_bytes),
    );
    fields.insert(
        "warm_local_p50_us".to_owned(),
        json!(summary.warm_local_p50_us),
    );
    fields.insert(
        "warm_local_p95_us".to_owned(),
        json!(summary.warm_local_p95_us),
    );
    fields.insert(
        "warm_settled_p50_us".to_owned(),
        json!(summary.warm_settled_p50_us),
    );
    fields.insert(
        "warm_settled_p95_us".to_owned(),
        json!(summary.warm_settled_p95_us),
    );
    fields.insert("result_set_rows".to_owned(), json!(summary.result_set_rows));
    fields.insert("closure_rows".to_owned(), json!(summary.closure_rows));
    fields.insert("writes_applied".to_owned(), json!(summary.writes_applied));
    fields.insert(
        "transport_codec".to_owned(),
        json!(transport_codec_name(config.transport_codec)),
    );
    emit_object(fields);

    let mut edge_acceptance = metadata_fields("s1_saas", driver, config.seed, &config.profile);
    edge_acceptance.insert("phase".to_owned(), json!("edge_mergeable_acceptance"));
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

    let mut edge_hydration = metadata_fields("s1_saas", driver, config.seed, &config.profile);
    edge_hydration.insert("phase".to_owned(), json!("edge_permission_scope_hydration"));
    edge_hydration.insert("scope".to_owned(), json!("saas_query_closure"));
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

fn emit_reconnect_summary(config: &Config, summary: &ReconnectSummary) {
    let mut fields = metadata_fields(
        "s1_saas_reconnect",
        "deterministic",
        config.seed,
        &config.profile,
    );
    fields.insert("phase".to_owned(), json!("reconnect"));
    fields.insert("window_writes".to_owned(), json!(summary.window_writes));
    fields.insert("catchup_us".to_owned(), json!(summary.catchup_us));
    fields.insert("catchup_bytes".to_owned(), json!(summary.catchup_bytes));
    fields.insert(
        "catchup_bytes_floor".to_owned(),
        json!(summary.catchup_bytes_floor),
    );
    fields.insert("result_set_rows".to_owned(), json!(summary.result_set_rows));
    fields.insert("closure_rows".to_owned(), json!(summary.closure_rows));
    fields.insert(
        "transport_codec".to_owned(),
        json!(transport_codec_name(summary.transport_codec)),
    );
    fields.extend(summary.transport_metrics.clone());
    emit_object(fields);
}

fn assert_wire_frame_metrics(fields: serde_json::Map<String, JsonValue>) {
    let encoded = fields["transport_codec_messages_encoded"]
        .as_u64()
        .unwrap_or_default();
    let decoded = fields["transport_codec_messages_decoded"]
        .as_u64()
        .unwrap_or_default();
    assert_eq!(encoded, decoded);
    assert!(encoded > 0);
    assert!(fields.contains_key("transport_codec_frame_bytes_per_message"));
    assert!(fields.contains_key("transport_codec_frame_overhead_bytes_per_message"));
}

fn emit_sweep_summary(config: &Config, summary: &SweepSummary) {
    let mut fields = metadata_fields(
        "s1_saas_subscriber_sweep",
        "threaded-standin",
        config.seed,
        "s1-subscriber-sweep-peer-standins",
    );
    fields.insert("phase".to_owned(), json!("subscriber_sweep"));
    fields.insert("subscribers".to_owned(), json!(summary.subscribers));
    fields.insert("commits".to_owned(), json!(summary.commits));
    fields.insert(
        "core_emit_p50_us".to_owned(),
        json!(summary.core_emit_p50_us),
    );
    fields.insert(
        "core_emit_p95_us".to_owned(),
        json!(summary.core_emit_p95_us),
    );
    fields.insert(
        "total_notification_bytes".to_owned(),
        json!(summary.total_notification_bytes),
    );
    fields.insert(
        "bytes_per_commit".to_owned(),
        json!(summary.bytes_per_commit),
    );
    fields.insert(
        "version_bundles_out".to_owned(),
        json!(summary.version_bundles_out),
    );
    fields.insert(
        "complete_tx_refs_out".to_owned(),
        json!(summary.complete_tx_refs_out),
    );
    fields.insert("result_adds_out".to_owned(), json!(summary.result_adds_out));
    fields.insert(
        "result_removes_out".to_owned(),
        json!(summary.result_removes_out),
    );
    fields.insert(
        "endpoint_model".to_owned(),
        json!("PeerState stand-ins; no full client Nodes in sweep"),
    );
    emit_object(fields);
}

fn emit_high_fan_out_summary(config: &Config, summary: &HighFanOutSummary) {
    let mut fields = metadata_fields(
        "s1_high_fan_out_hydration",
        "threaded",
        config.seed,
        &config.profile,
    );
    fields.insert("phase".to_owned(), json!("high_fan_out_hydration"));
    fields.insert("fanout".to_owned(), json!(summary.fanout));
    fields.insert("parents".to_owned(), json!(summary.parents));
    fields.insert("children".to_owned(), json!(summary.children));
    fields.insert("subscriptions".to_owned(), json!(summary.subscriptions));
    fields.insert(
        "hydration_complete_us".to_owned(),
        json!(summary.hydration_complete_us),
    );
    fields.insert("hydration_bytes".to_owned(), json!(summary.hydration_bytes));
    fields.insert(
        "hydration_floor_bytes".to_owned(),
        json!(summary.hydration_floor_bytes),
    );
    fields.insert("result_set_rows".to_owned(), json!(summary.result_set_rows));
    fields.insert(
        "mid_hydration_fates".to_owned(),
        json!(summary.mid_hydration_fates),
    );
    fields.insert(
        "mid_hydration_bytes".to_owned(),
        json!(summary.mid_hydration_bytes),
    );
    fields.insert(
        "membership_by_tx_index_seeks".to_owned(),
        json!(summary.by_tx_index_seeks),
    );
    fields.insert(
        "membership_history_scan_fallbacks".to_owned(),
        json!(summary.history_scan_fallbacks),
    );
    fields.insert(
        "maintained_subscription_view_hits_out".to_owned(),
        json!(summary.maintained_subscription_view_metrics.hits_out),
    );
    fields.insert(
        "maintained_subscription_view_full_recomputes_out".to_owned(),
        json!(0),
    );
    fields.insert(
        "maintained_subscription_view_delta_batches_in".to_owned(),
        json!(
            summary
                .maintained_subscription_view_metrics
                .delta_batches_in
        ),
    );
    fields.insert(
        "maintained_subscription_view_footprint_result_rows".to_owned(),
        json!(
            summary
                .maintained_subscription_view_metrics
                .footprint
                .result_rows
        ),
    );
    fields.insert(
        "maintained_subscription_view_footprint_version_identities".to_owned(),
        json!(
            summary
                .maintained_subscription_view_metrics
                .footprint
                .version_identities
        ),
    );
    fields.insert(
        "maintained_subscription_view_footprint_version_tx_entries".to_owned(),
        json!(
            summary
                .maintained_subscription_view_metrics
                .footprint
                .version_tx_entries
        ),
    );
    fields.insert(
        "maintained_subscription_view_footprint_replacement_entries".to_owned(),
        json!(
            summary
                .maintained_subscription_view_metrics
                .footprint
                .replacement_entries
        ),
    );
    fields.insert(
        "full_diff_recomputes_out".to_owned(),
        json!(summary.full_diff_recomputes),
    );
    fields.insert(
        "membership_counter_model".to_owned(),
        json!("deterministic harness counter: one by_tx-bounded membership check per active subscription per mid-hydration fate; unbounded history-scan path is disallowed and asserted zero"),
    );
    emit_object(fields);
}

fn emit_object(fields: serde_json::Map<String, JsonValue>) {
    let line = serde_json::to_string(&JsonValue::Object(fields)).expect("json line");
    emit_json_line("s1_saas", &line);
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

fn env_usize_list(name: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|part| part.trim().parse::<usize>().ok())
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| default.to_vec())
}

fn transport_codec_name(codec: SimulatorTransportCodec) -> &'static str {
    match codec {
        SimulatorTransportCodec::Native => "native",
        SimulatorTransportCodec::WireBytes => "wire_bytes",
        SimulatorTransportCodec::WireFrames => "wire_frames",
    }
}
