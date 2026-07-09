use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use hdrhistogram::Histogram;
use jazz::db::{
    Db, DbConfig, DbIdentity, Node, ReadOpts, SeededRowIdSource, SubscriptionEvent,
    SubscriptionStream, Transport,
};
use jazz::groove::db::{
    StorageReadBucket, StorageReadMetrics, StorageWriteBucket, StorageWriteMetrics,
};
use jazz::groove::records::{EnumSchema, Value};
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::{Durability, RocksDbStorage};
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState};
use jazz::peer::PeerState;
use jazz::protocol::{
    RegisterShapeOptions, ResultRowEntry, ShapeAst, Subscribe, SubscriptionKey, SyncMessage,
    VersionRecord,
};
use jazz::query::{Binding, Query, ValidatedQuery, claim, col, eq, lit};
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::time::TxTime;
use jazz::tx::{DeletionEvent, DurabilityTier, Fate, Transaction, TxId, TxKind};
use jazz::wire::TransportError;
use jazz_sim::{
    DeterministicDriver, DriverContext, NodeRole, PeerProfile, ThreadedDriver, Topology,
    bench_profile, emit_json_line, metadata_fields, profiling,
};
use serde_json::{Value as JsonValue, json};

const ORGS: &str = "orgs";
const TEAMS: &str = "teams";
const MEMBERSHIPS: &str = "teamTeamMemberships";
const RESOURCES: &str = "resources";
const ACCESS: &str = "resourceAccess";
const PAGES: &str = "pages";
const BLOCKS: &str = "blocks";

const PERM_READ: u8 = 0;

const FULL_REVOKE_SIZES: &[usize] = &[1, 10, 100];
const PROFILE_REVOKE_SIZES: &[usize] = &[1, 10, 50];
const FAST_REVOKE_SIZES: &[usize] = &[1, 3];

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
    if config.headline_only {
        let headline = run_block_tree_cold_headline(&config, profile);
        emit_block_tree_cold_headline(&config, &headline);
        return;
    }
    let topology = topology(&config, profile.clone());

    let mut deterministic = DeterministicDriver::new(topology.clone(), config.seed);
    let deterministic_summary =
        profiling::maybe_profile_phase("s3_permissions", "deterministic_run", || {
            run(&mut deterministic, &config)
        });
    emit_summaries("deterministic", &config, &deterministic_summary);

    let mut threaded = ThreadedDriver::new(topology, config.seed);
    let threaded_summary = profiling::maybe_profile_phase("s3_permissions", "threaded_run", || {
        run(&mut threaded, &config)
    });
    emit_summaries("threaded", &config, &threaded_summary);

    let db_surface =
        profiling::maybe_profile_phase("s3_permissions", "db_surface", || run_db_surface(&config));
    emit_db_surface_summary(&config, &db_surface);
    if env_bool("JAZZ_S3_PERMISSIONS_ONLY") {
        return;
    }

    let block_summary = profiling::maybe_profile_phase("s3_permissions", "block_tree", || {
        run_block_tree_variant(&config, profile.clone())
    });
    emit_block_tree_summary(&config, &block_summary);
    let headline = profiling::maybe_profile_phase("s3_permissions", "block_tree_headline", || {
        run_block_tree_cold_headline(&config, profile)
    });
    emit_block_tree_cold_headline(&config, &headline);
}

pub fn smoke() {
    let config = Config {
        seed: 0x5300_0001,
        profile: "s3-smoke".to_owned(),
        orgs: 1,
        teams_per_org: 4,
        resources_per_org: 20,
        access_edges_per_resource: 1,
        revocation_sizes: vec![1, 3],
        block_pages: 2,
        block_blocks_per_page: 12,
        block_visible_rows: 8,
        headline_pages: 2,
        headline_blocks_per_page: 12,
        headline_visible_rows: 8,
        headline_bulk_chunk_rows: 4,
        headline_only: false,
        headline_progress: false,
        block_hot_pages: 1,
        block_bursts: 1,
    };
    let profile = PeerProfile::new(config.profile.clone(), 1, 0, 0);
    let topology = topology(&config, profile);
    let mut deterministic = DeterministicDriver::new(topology, config.seed);
    let summary = profiling::maybe_profile_phase("s3_permissions", "deterministic_run", || {
        run(&mut deterministic, &config)
    });
    emit_summaries("deterministic", &config, &summary);
    assert_eq!(summary.forbidden_deliveries, 0);
}

#[derive(Clone, Debug)]
struct Config {
    seed: u64,
    profile: String,
    orgs: usize,
    teams_per_org: usize,
    resources_per_org: usize,
    access_edges_per_resource: usize,
    revocation_sizes: Vec<usize>,
    block_pages: usize,
    block_blocks_per_page: usize,
    block_visible_rows: usize,
    headline_pages: usize,
    headline_blocks_per_page: usize,
    headline_visible_rows: usize,
    headline_bulk_chunk_rows: usize,
    headline_only: bool,
    headline_progress: bool,
    block_hot_pages: usize,
    block_bursts: usize,
}

impl Config {
    fn from_env() -> Self {
        let defaults = BenchDefaults::from_env();
        Self {
            seed: env_u64("JAZZ_SEED", 0x5300_0001),
            profile: std::env::var("JAZZ_PROFILE").unwrap_or_else(|_| "s3-local".to_owned()),
            orgs: env_usize("JAZZ_S3_ORGS", defaults.orgs).max(1),
            teams_per_org: env_usize("JAZZ_S3_TEAMS_PER_ORG", defaults.teams_per_org).max(4),
            resources_per_org: env_usize("JAZZ_S3_RESOURCES_PER_ORG", defaults.resources_per_org)
                .max(20),
            access_edges_per_resource: env_usize(
                "JAZZ_S3_ACCESS_EDGES_PER_RESOURCE",
                defaults.access_edges_per_resource,
            )
            .max(1),
            revocation_sizes: env_usize_list("JAZZ_S3_REVOKE_SIZES", defaults.revocation_sizes),
            block_pages: env_usize("JAZZ_S3_BLOCK_PAGES", defaults.block_pages).max(1),
            block_blocks_per_page: env_usize(
                "JAZZ_S3_BLOCKS_PER_PAGE",
                defaults.block_blocks_per_page,
            )
            .max(12),
            block_visible_rows: env_usize(
                "JAZZ_S3_BLOCK_VISIBLE_ROWS",
                defaults.block_visible_rows,
            )
            .max(1),
            headline_pages: env_usize("JAZZ_S3_HEADLINE_PAGES", defaults.headline_pages).max(1),
            headline_blocks_per_page: env_usize(
                "JAZZ_S3_HEADLINE_BLOCKS_PER_PAGE",
                defaults.headline_blocks_per_page,
            )
            .max(12),
            headline_visible_rows: env_usize(
                "JAZZ_S3_HEADLINE_VISIBLE_ROWS",
                defaults.headline_visible_rows,
            )
            .max(1),
            headline_bulk_chunk_rows: env_usize(
                "JAZZ_S3_HEADLINE_BULK_CHUNK_ROWS",
                defaults.headline_bulk_chunk_rows,
            )
            .max(1),
            headline_only: env_bool("JAZZ_S3_HEADLINE_ONLY"),
            headline_progress: env_bool("JAZZ_S3_HEADLINE_PROGRESS"),
            block_hot_pages: env_usize("JAZZ_S3_BLOCK_HOT_PAGES", defaults.block_hot_pages).max(1),
            block_bursts: env_usize("JAZZ_S3_BLOCK_BURSTS", defaults.block_bursts),
        }
    }

    fn resources(&self) -> usize {
        self.orgs * self.resources_per_org
    }
}

#[derive(Clone, Copy, Debug)]
struct BenchDefaults {
    orgs: usize,
    teams_per_org: usize,
    resources_per_org: usize,
    access_edges_per_resource: usize,
    revocation_sizes: &'static [usize],
    block_pages: usize,
    block_blocks_per_page: usize,
    block_visible_rows: usize,
    headline_pages: usize,
    headline_blocks_per_page: usize,
    headline_visible_rows: usize,
    headline_bulk_chunk_rows: usize,
    block_hot_pages: usize,
    block_bursts: usize,
}

impl BenchDefaults {
    fn from_env() -> Self {
        bench_profile().select(Self::fast(), Self::profile(), Self::full())
    }

    fn full() -> Self {
        Self {
            orgs: 2,
            teams_per_org: 12,
            resources_per_org: 800,
            access_edges_per_resource: 4,
            revocation_sizes: FULL_REVOKE_SIZES,
            block_pages: 100,
            block_blocks_per_page: 200,
            block_visible_rows: 20_000,
            headline_pages: 1_000,
            headline_blocks_per_page: 100,
            headline_visible_rows: 20_000,
            headline_bulk_chunk_rows: 1_000,
            block_hot_pages: 4,
            block_bursts: 20,
        }
    }

    fn fast() -> Self {
        Self {
            orgs: 1,
            teams_per_org: 4,
            resources_per_org: 20,
            access_edges_per_resource: 1,
            revocation_sizes: FAST_REVOKE_SIZES,
            block_pages: 2,
            block_blocks_per_page: 12,
            block_visible_rows: 8,
            headline_pages: 2,
            headline_blocks_per_page: 12,
            headline_visible_rows: 8,
            headline_bulk_chunk_rows: 4,
            block_hot_pages: 1,
            block_bursts: 1,
        }
    }

    fn profile() -> Self {
        Self {
            orgs: 1,
            teams_per_org: 8,
            resources_per_org: 200,
            access_edges_per_resource: 2,
            revocation_sizes: PROFILE_REVOKE_SIZES,
            block_pages: 20,
            block_blocks_per_page: 50,
            block_visible_rows: 2_000,
            headline_pages: 100,
            headline_blocks_per_page: 50,
            headline_visible_rows: 2_000,
            headline_bulk_chunk_rows: 250,
            block_hot_pages: 2,
            block_bursts: 5,
        }
    }
}

#[derive(Debug)]
struct Summary {
    fixture_rows: usize,
    simple_visible: usize,
    admin_visible: usize,
    cold_simple_us: u64,
    cold_admin_us: u64,
    cold_simple_bytes: u64,
    cold_admin_bytes: u64,
    cold_simple_floor_bytes: u64,
    cold_admin_floor_bytes: u64,
    cold_simple_core_reads: StorageReadMetrics,
    cold_simple_view_reads: StorageReadMetrics,
    cold_admin_core_reads: StorageReadMetrics,
    cold_admin_view_reads: StorageReadMetrics,
    grant_none_latency: Histogram<u64>,
    grant_global_latency: Histogram<u64>,
    revoke: Vec<RevokeSummary>,
    forbidden_deliveries: u64,
    link_rtt_floor_us: u64,
    client_edge_one_way_ms: u64,
    edge_core_one_way_ms: u64,
    edge_acceptance: EdgeAcceptanceSummary,
}

#[derive(Debug)]
struct DbSurfaceSummary {
    fixture_rows: usize,
    simple_visible: usize,
    admin_visible: usize,
    cold_simple_us: u64,
    cold_admin_us: u64,
    cold_spy_us: u64,
    cold_simple_bytes: u64,
    cold_admin_bytes: u64,
    grant_none_latency: Histogram<u64>,
    grant_global_latency: Histogram<u64>,
    revoke: Vec<DbRevokeSummary>,
    forbidden_deliveries: u64,
    client_edge_one_way_ms: u64,
    edge_core_one_way_ms: u64,
}

#[derive(Debug)]
struct EdgeAcceptanceSummary {
    acceptance_latency: Histogram<u64>,
    hydration_bytes: u64,
    hydration_floor_bytes: u64,
    hydration_rows: usize,
    scope_subscriptions_before_drain: usize,
    scope_subscriptions_after_drain: usize,
}

#[derive(Debug)]
struct RevokeSummary {
    hidden: usize,
    disappearance: Histogram<u64>,
    core_cpu: Histogram<u64>,
    query_update_us: u64,
    send_recv_us: u64,
    apply_us: u64,
    update_rows: usize,
}

#[derive(Debug)]
struct DbRevokeSummary {
    hidden: usize,
    disappearance: Histogram<u64>,
    tick_us: u64,
    update_rows: usize,
}

#[derive(Debug)]
struct BlockTreeSummary {
    pages: usize,
    blocks: usize,
    max_depth: usize,
    cold_visible_rows: usize,
    simple_visible_rows: usize,
    cold_complete_us: u64,
    cold_bytes: u64,
    cold_floor_bytes: u64,
    cold_view_reads: StorageReadMetrics,
    grant_subtree_rows: usize,
    grant_core_us: u64,
    grant_appearance_us: u64,
    grant_update_rows: usize,
    grant_view_reads: StorageReadMetrics,
    revoke_subtree_rows: usize,
    revoke_core_us: u64,
    revoke_disappearance_us: u64,
    revoke_update_rows: usize,
    revoke_view_reads: StorageReadMetrics,
    burst_commits: usize,
    burst_core_p95_us: u64,
}

#[derive(Debug)]
struct BlockTreeHeadlineSummary {
    pages: usize,
    blocks: usize,
    visible_rows: usize,
    fixture_population_us: u64,
    end_to_end_cold_us: u64,
    server_eval_ship_us: u64,
    server_rehydrate_query_us: u64,
    server_byte_accounting_us: u64,
    server_send_us: u64,
    client_ingest_materialize_us: u64,
    client_recv_us: u64,
    client_apply_sync_us: u64,
    client_materialize_read_us: u64,
    cold_bytes: u64,
    cold_floor_bytes: u64,
    client_storage_writes: StorageWriteMetrics,
    server_view_reads: StorageReadMetrics,
    client_materialize_reads: StorageReadMetrics,
}

#[derive(Clone, Debug)]
struct Fixture {
    simple_team: RowUuid,
    admin_team: RowUuid,
    visible_group: RowUuid,
    grant_group: RowUuid,
    revoke_groups: Vec<(RowUuid, usize)>,
    resources: Vec<RowUuid>,
    fixture_rows: usize,
}

struct Client {
    name: String,
    node: NodeState<RocksDbStorage>,
    _dir: tempfile::TempDir,
    peer: PeerState,
    registered_subscriptions: BTreeSet<SubscriptionKey>,
    visible_rows: BTreeSet<RowUuid>,
}

struct EdgeRoute {
    name: String,
    node: NodeState<RocksDbStorage>,
    _dir: tempfile::TempDir,
    core_peer: PeerState,
    policy_peer: PeerState,
}

struct DbClient {
    db: Db<RocksDbStorage>,
    _dir: tempfile::TempDir,
    server_to_client_bytes: Rc<Cell<u64>>,
    server_to_client_floor_bytes: Rc<Cell<u64>>,
    watch: Option<SubscriptionStream>,
    visible_rows: BTreeSet<RowUuid>,
}

#[derive(Clone, Debug)]
struct BlockTreeFixture {
    blocks: Vec<BlockInfo>,
    visible: BTreeSet<RowUuid>,
    grant_root: RowUuid,
    revoke_root: RowUuid,
}

#[derive(Clone, Debug)]
struct BlockInfo {
    row: RowUuid,
    page: RowUuid,
    parent: Option<RowUuid>,
    depth: usize,
    ordinal: usize,
}

fn run(ctx: &mut dyn DriverContext, config: &Config) -> Summary {
    let schema = schema();
    let (_core_dir, mut core) = open_node(node(250), schema.clone());
    let (_writer_dir, mut writer) = open_node(node(1), schema.clone());
    let fixture = seed_fixture(ctx, config, &mut writer, &mut core);
    let (shape, binding) = resource_subscription(&schema);

    let mut simple = open_client(
        "simple",
        node(20),
        schema.clone(),
        AuthorId(fixture.simple_team.0),
    );
    let mut simple_edge = open_edge(
        "simple_edge",
        node(120),
        schema.clone(),
        AuthorId(fixture.simple_team.0),
    );
    let mut admin = open_client(
        "admin",
        node(21),
        schema.clone(),
        AuthorId(fixture.admin_team.0),
    );
    let mut admin_edge = open_edge(
        "admin_edge",
        node(121),
        schema.clone(),
        AuthorId(fixture.admin_team.0),
    );
    let mut spy = open_client("spy", node(22), schema.clone(), AuthorId(row(9_900).0));
    let mut spy_edge = open_edge(
        "spy_edge",
        node(122),
        schema.clone(),
        AuthorId(row(9_900).0),
    );

    let simple_cold = hydrate(
        ctx,
        &mut core,
        &mut simple_edge,
        &mut simple,
        &shape,
        &binding,
    );
    let admin_cold = hydrate(
        ctx,
        &mut core,
        &mut admin_edge,
        &mut admin,
        &shape,
        &binding,
    );
    let spy_cold = hydrate(ctx, &mut core, &mut spy_edge, &mut spy, &shape, &binding);
    assert_eq!(spy.visible_rows.len(), 0);
    assert_eq!(spy_cold.output_rows, 0);

    let mut oracle = OracleState::from_core(&mut core, config, &fixture);
    assert_eq!(
        simple.visible_rows.clone(),
        oracle.visible_for(fixture.simple_team)
    );
    assert_eq!(
        admin.visible_rows.clone(),
        oracle.visible_for(fixture.admin_team)
    );

    let mut grant_none_latency = Histogram::new(3).unwrap();
    let mut grant_global_latency = Histogram::new(3).unwrap();
    let grant_samples = grant_phase(
        ctx,
        &mut writer,
        &mut core,
        &mut simple_edge,
        &mut simple,
        &shape,
        &binding,
        &fixture,
        &mut oracle,
    );
    for sample in grant_samples {
        grant_none_latency.record(sample).unwrap();
        grant_global_latency.record(sample).unwrap();
    }
    assert_eq!(
        simple.visible_rows.clone(),
        oracle.visible_for(fixture.simple_team)
    );

    let mut revoke_summaries = Vec::new();
    for (group, _requested) in fixture.revoke_groups.iter().copied() {
        let before = oracle.visible_for(fixture.simple_team);
        let membership = oracle
            .memberships
            .iter()
            .find(|(_, (member, parent, _))| *member == fixture.simple_team && *parent == group)
            .map(|(row, _)| *row)
            .expect("revoke membership exists");
        let mut expected_oracle = oracle.clone();
        expected_oracle.memberships.remove(&membership);
        let expected = expected_oracle.visible_for(fixture.simple_team);
        let hidden = before.difference(&expected).count();
        let summary = revoke_phase(
            ctx,
            &mut writer,
            &mut core,
            &mut simple_edge,
            &mut simple,
            &shape,
            &binding,
            &fixture,
            &mut oracle,
            group,
            hidden,
        );
        revoke_summaries.push(summary);
        assert_eq!(
            simple.visible_rows.clone(),
            oracle.visible_for(fixture.simple_team)
        );
    }

    let edge_acceptance = edge_acceptance_phase(
        ctx,
        &mut core,
        &mut simple_edge,
        &mut simple,
        fixture
            .resources
            .first()
            .copied()
            .expect("fixture has resources"),
        fixture.simple_team,
    );

    let forbidden_deliveries = forbidden_write_phase(
        ctx,
        &mut writer,
        &mut core,
        &mut spy_edge,
        &mut spy,
        &shape,
        &binding,
        config,
    );
    assert_eq!(forbidden_deliveries, 0);
    assert_eq!(spy.visible_rows.len(), 0);

    Summary {
        fixture_rows: fixture.fixture_rows,
        simple_visible: oracle.visible_for(fixture.simple_team).len(),
        admin_visible: oracle.visible_for(fixture.admin_team).len(),
        cold_simple_us: simple_cold.latency_us,
        cold_admin_us: admin_cold.latency_us,
        cold_simple_bytes: simple_cold.bytes,
        cold_admin_bytes: admin_cold.bytes,
        cold_simple_floor_bytes: simple_cold.floor_bytes,
        cold_admin_floor_bytes: admin_cold.floor_bytes,
        cold_simple_core_reads: simple_cold.core_read_metrics,
        cold_simple_view_reads: simple_cold.view_read_metrics,
        cold_admin_core_reads: admin_cold.core_read_metrics,
        cold_admin_view_reads: admin_cold.view_read_metrics,
        grant_none_latency,
        grant_global_latency,
        revoke: revoke_summaries,
        forbidden_deliveries,
        link_rtt_floor_us: 2
            * (profile_leg_ms(&config.profile).0 + profile_leg_ms(&config.profile).1)
            * 1_000,
        client_edge_one_way_ms: profile_leg_ms(&config.profile).0,
        edge_core_one_way_ms: profile_leg_ms(&config.profile).1,
        edge_acceptance,
    }
}

fn run_db_surface(config: &Config) -> DbSurfaceSummary {
    let schema = schema();
    let (_core_dir, core) = open_core_node(node(250), schema.clone());
    let fixture = seed_fixture_db(config, &core);
    let query = Query::from(RESOURCES);

    let mut simple = open_db_client(
        "simple",
        node(20),
        schema.clone(),
        AuthorId(fixture.simple_team.0),
        &core,
    );
    let mut admin = open_db_client(
        "admin",
        node(21),
        schema.clone(),
        AuthorId(fixture.admin_team.0),
        &core,
    );
    let mut spy = open_db_client(
        "spy",
        node(22),
        schema.clone(),
        AuthorId(row(9_900).0),
        &core,
    );

    let simple_cold = hydrate_db(&core, &mut simple, &query);
    let admin_cold = hydrate_db(&core, &mut admin, &query);
    let spy_cold = hydrate_db(&core, &mut spy, &query);
    assert_eq!(visible_rows_db_client(&spy).len(), 0);
    assert_eq!(spy_cold.output_rows, 0);

    let mut oracle = OracleState::from_fixture_db(config, &fixture);
    assert_eq!(
        visible_rows_db_client(&simple),
        oracle.visible_for(fixture.simple_team)
    );
    assert_eq!(
        visible_rows_db_client(&admin),
        oracle.visible_for(fixture.admin_team)
    );

    let mut grant_none_latency = Histogram::new(3).unwrap();
    let mut grant_global_latency = Histogram::new(3).unwrap();
    for sample in grant_phase_db(&core, &mut simple, &fixture, &mut oracle) {
        grant_none_latency.record(sample).unwrap();
        grant_global_latency.record(sample).unwrap();
    }
    assert_eq!(
        visible_rows_db_client(&simple),
        oracle.visible_for(fixture.simple_team)
    );

    let mut revoke = Vec::new();
    for (group, _requested) in fixture.revoke_groups.iter().copied() {
        let before = oracle.visible_for(fixture.simple_team);
        let membership = oracle
            .memberships
            .iter()
            .find(|(_, (member, parent, _))| *member == fixture.simple_team && *parent == group)
            .map(|(row, _)| *row)
            .expect("revoke membership exists");
        let mut expected_oracle = oracle.clone();
        expected_oracle.memberships.remove(&membership);
        let expected = expected_oracle.visible_for(fixture.simple_team);
        let hidden = before.difference(&expected).count();
        let summary = revoke_phase_db(&core, &mut simple, &fixture, &mut oracle, group, hidden);
        revoke.push(summary);
        assert_eq!(
            visible_rows_db_client(&simple),
            oracle.visible_for(fixture.simple_team)
        );
    }

    let forbidden_deliveries = forbidden_write_phase_db(&core, &mut spy, config);
    assert_eq!(visible_rows_db_client(&spy).len(), 0);

    DbSurfaceSummary {
        fixture_rows: fixture.fixture_rows,
        simple_visible: oracle.visible_for(fixture.simple_team).len(),
        admin_visible: oracle.visible_for(fixture.admin_team).len(),
        cold_simple_us: simple_cold.latency_us,
        cold_admin_us: admin_cold.latency_us,
        cold_spy_us: spy_cold.latency_us,
        cold_simple_bytes: simple_cold.bytes,
        cold_admin_bytes: admin_cold.bytes,
        grant_none_latency,
        grant_global_latency,
        revoke,
        forbidden_deliveries,
        client_edge_one_way_ms: profile_leg_ms(&config.profile).0,
        edge_core_one_way_ms: profile_leg_ms(&config.profile).1,
    }
}

#[allow(clippy::too_many_arguments)]
fn grant_phase(
    ctx: &mut dyn DriverContext,
    writer: &mut NodeState<RocksDbStorage>,
    core: &mut NodeState<RocksDbStorage>,
    edge: &mut EdgeRoute,
    client: &mut Client,
    shape: &ValidatedQuery,
    binding: &Binding,
    fixture: &Fixture,
    oracle: &mut OracleState,
) -> Vec<u64> {
    let mut samples = Vec::new();
    let resource = row(700_000);
    let start = ctx.now_ms();
    commit_global(
        ctx,
        writer,
        core,
        RESOURCES,
        resource,
        AuthorId::SYSTEM,
        resource_cells(777),
        700_000,
    );
    oracle.resources.insert(resource);
    let access_edge = row(700_001);
    commit_global(
        ctx,
        writer,
        core,
        ACCESS,
        access_edge,
        AuthorId::SYSTEM,
        access_cells(resource, fixture.visible_group, false),
        700_001,
    );
    oracle
        .access
        .insert(access_edge, (resource, fixture.visible_group, false));
    deliver_update(ctx, core, edge, client, shape, binding);
    samples.push((ctx.now_ms() - start) * 1_000);

    let resource2 = row(700_002);
    commit_global(
        ctx,
        writer,
        core,
        RESOURCES,
        resource2,
        AuthorId::SYSTEM,
        resource_cells(778),
        700_002,
    );
    oracle.resources.insert(resource2);
    let edge2 = row(700_003);
    commit_global(
        ctx,
        writer,
        core,
        ACCESS,
        edge2,
        AuthorId::SYSTEM,
        access_cells(resource2, fixture.grant_group, false),
        700_003,
    );
    oracle
        .access
        .insert(edge2, (resource2, fixture.grant_group, false));
    let membership = row(700_004);
    let start = ctx.now_ms();
    commit_global(
        ctx,
        writer,
        core,
        MEMBERSHIPS,
        membership,
        AuthorId::SYSTEM,
        membership_cells(fixture.simple_team, fixture.grant_group, false),
        700_004,
    );
    oracle.memberships.insert(
        membership,
        (fixture.simple_team, fixture.grant_group, false),
    );
    deliver_update(ctx, core, edge, client, shape, binding);
    samples.push((ctx.now_ms() - start) * 1_000);
    samples
}

fn grant_phase_db(
    core: &CoreDb,
    client: &mut DbClient,
    fixture: &Fixture,
    oracle: &mut OracleState,
) -> Vec<u64> {
    let mut samples = Vec::new();
    let resource = row(700_000);
    let start = Instant::now();
    seed_db(core, RESOURCES, resource, resource_cells(777));
    oracle.resources.insert(resource);
    let edge = row(700_001);
    seed_db(
        core,
        ACCESS,
        edge,
        access_cells(resource, fixture.visible_group, false),
    );
    oracle
        .access
        .insert(edge, (resource, fixture.visible_group, false));
    drive_db_round_trip(core, client);
    assert!(visible_rows_db_client(client).contains(&resource));
    samples.push(start.elapsed().as_micros() as u64);

    let resource2 = row(700_002);
    seed_db(core, RESOURCES, resource2, resource_cells(778));
    oracle.resources.insert(resource2);
    let edge2 = row(700_003);
    seed_db(
        core,
        ACCESS,
        edge2,
        access_cells(resource2, fixture.grant_group, false),
    );
    oracle
        .access
        .insert(edge2, (resource2, fixture.grant_group, false));
    let membership = row(700_004);
    let start = Instant::now();
    seed_db(
        core,
        MEMBERSHIPS,
        membership,
        membership_cells(fixture.simple_team, fixture.grant_group, false),
    );
    oracle.memberships.insert(
        membership,
        (fixture.simple_team, fixture.grant_group, false),
    );
    drive_db_round_trip(core, client);
    let watch_rows = visible_rows_db_client(client);
    let prepared = client.db.prepare_query(&Query::from(RESOURCES)).unwrap();
    let read_rows = client
        .db
        .read(&prepared)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    assert!(
        watch_rows.contains(&resource2),
        "resource2 missing after grant; watch_contains={} read_contains={} watch_len={} read_len={}",
        watch_rows.contains(&resource2),
        read_rows.contains(&resource2),
        watch_rows.len(),
        read_rows.len()
    );
    samples.push(start.elapsed().as_micros() as u64);
    samples
}

fn revoke_phase_db(
    core: &CoreDb,
    client: &mut DbClient,
    fixture: &Fixture,
    oracle: &mut OracleState,
    group: RowUuid,
    hidden: usize,
) -> DbRevokeSummary {
    let membership = oracle
        .memberships
        .iter()
        .find(|(_, (member, parent, _))| *member == fixture.simple_team && *parent == group)
        .map(|(row, _)| *row)
        .expect("revoke membership exists");
    let before = visible_rows_db_client(client);
    let start = Instant::now();
    delete_db(core, MEMBERSHIPS, membership);
    oracle.memberships.remove(&membership);
    let tick_start = Instant::now();
    drive_db_round_trip(core, client);
    let expected = oracle.visible_for(fixture.simple_team);
    let mut after = visible_rows_db_client(client);
    for _ in 0..env_usize("JAZZ_S3_DB_REVOKE_SETTLE_TICKS", 3) {
        if after == expected {
            break;
        }
        core.tick().unwrap();
        client.db.tick().unwrap();
        drain_db_subscription(client);
        after = visible_rows_db_client(client);
    }
    let tick_us = tick_start.elapsed().as_micros() as u64;
    assert_eq!(after, expected);
    let removed = before.difference(&after).count();
    assert!(
        removed >= hidden,
        "revocation expected at least {hidden} removals, got {removed}"
    );
    let mut disappearance = Histogram::new(3).unwrap();
    disappearance
        .record(start.elapsed().as_micros() as u64)
        .unwrap();
    DbRevokeSummary {
        hidden,
        disappearance,
        tick_us,
        update_rows: removed,
    }
}

#[allow(clippy::too_many_arguments)]
fn revoke_phase(
    ctx: &mut dyn DriverContext,
    writer: &mut NodeState<RocksDbStorage>,
    core: &mut NodeState<RocksDbStorage>,
    edge: &mut EdgeRoute,
    client: &mut Client,
    shape: &ValidatedQuery,
    binding: &Binding,
    fixture: &Fixture,
    oracle: &mut OracleState,
    group: RowUuid,
    hidden: usize,
) -> RevokeSummary {
    let membership = oracle
        .memberships
        .iter()
        .find(|(_, (member, parent, _))| *member == fixture.simple_team && *parent == group)
        .map(|(row, _)| *row)
        .expect("revoke membership exists");
    let start = ctx.now_ms();
    let cpu = Instant::now();
    delete_global(
        ctx,
        writer,
        core,
        MEMBERSHIPS,
        membership,
        800_000 + hidden as u64,
    );
    let cpu_us = cpu.elapsed().as_micros() as u64;
    oracle.memberships.remove(&membership);
    let query_start = Instant::now();
    hydrate_edge_policy(ctx, core, edge);
    let core_update = edge.core_peer.query_update(core, shape, binding).unwrap();
    ctx.send("core", &edge.name, core_update);
    let delivered_to_edge = ctx.recv(&edge.name);
    edge.node
        .apply_sync_message(delivered_to_edge.message)
        .unwrap();
    let update = client
        .peer
        .query_update(&mut edge.node, shape, binding)
        .unwrap();
    let query_update_us = query_start.elapsed().as_micros() as u64;
    let removed = match &update {
        SyncMessage::ViewUpdate {
            result_member_removes,
            ..
        } => result_member_removes.len(),
        _ => 0,
    };
    assert!(
        removed >= hidden,
        "revocation expected at least {hidden} removals, got {removed}: {update:?}"
    );
    let send_start = Instant::now();
    ctx.send(&edge.name, &client.name, update);
    let delivered = ctx.recv(&client.name);
    let send_recv_us = send_start.elapsed().as_micros() as u64;
    let apply_start = Instant::now();
    ensure_client_subscription_registered(client, shape, binding);
    apply_client_update(client, delivered.message);
    let apply_us = apply_start.elapsed().as_micros() as u64;
    let mut disappearance = Histogram::new(3).unwrap();
    disappearance
        .record((ctx.now_ms() - start) * 1_000)
        .unwrap();
    let mut core_cpu = Histogram::new(3).unwrap();
    core_cpu.record(cpu_us).unwrap();
    RevokeSummary {
        hidden,
        disappearance,
        core_cpu,
        query_update_us,
        send_recv_us,
        apply_us,
        update_rows: removed,
    }
}

#[allow(clippy::too_many_arguments)]
fn forbidden_write_phase(
    ctx: &mut dyn DriverContext,
    writer: &mut NodeState<RocksDbStorage>,
    core: &mut NodeState<RocksDbStorage>,
    edge: &mut EdgeRoute,
    spy: &mut Client,
    shape: &ValidatedQuery,
    binding: &Binding,
    config: &Config,
) -> u64 {
    let resource = row(900_000);
    commit_global(
        ctx,
        writer,
        core,
        RESOURCES,
        resource,
        AuthorId::SYSTEM,
        resource_cells(9_000),
        900_000,
    );
    let core_update = edge.core_peer.query_update(core, shape, binding).unwrap();
    ctx.send("core", &edge.name, core_update);
    let delivered_to_edge = ctx.recv(&edge.name);
    edge.node
        .apply_sync_message(delivered_to_edge.message)
        .unwrap();
    hydrate_edge_policy(ctx, core, edge);
    let update = spy
        .peer
        .query_update(&mut edge.node, shape, binding)
        .unwrap();
    let forbidden = result_rows(&update).len() as u64;
    ctx.send(&edge.name, &spy.name, update);
    let delivered = ctx.recv(&spy.name);
    ensure_client_subscription_registered(spy, shape, binding);
    apply_client_update(spy, delivered.message);
    for tick in 0..env_usize("JAZZ_S3_SPY_TICKS", 3) {
        commit_global(
            ctx,
            writer,
            core,
            RESOURCES,
            row(900_010 + tick),
            AuthorId::SYSTEM,
            resource_cells(config.resources() + tick),
            900_010 + tick as u64,
        );
        let core_update = edge.core_peer.query_update(core, shape, binding).unwrap();
        ctx.send("core", &edge.name, core_update);
        let delivered_to_edge = ctx.recv(&edge.name);
        edge.node
            .apply_sync_message(delivered_to_edge.message)
            .unwrap();
        hydrate_edge_policy(ctx, core, edge);
        let update = spy
            .peer
            .query_update(&mut edge.node, shape, binding)
            .unwrap();
        if !result_rows(&update).is_empty() {
            return forbidden + result_rows(&update).len() as u64;
        }
    }
    forbidden
}

fn forbidden_write_phase_db(core: &CoreDb, spy: &mut DbClient, config: &Config) -> u64 {
    let resource = row(900_000);
    seed_db(core, RESOURCES, resource, resource_cells(9_000));
    drive_db_round_trip(core, spy);
    let mut forbidden = visible_rows_db_client(spy).len() as u64;
    for tick in 0..env_usize("JAZZ_S3_SPY_TICKS", 3) {
        seed_db(
            core,
            RESOURCES,
            row(900_010 + tick),
            resource_cells(config.resources() + tick),
        );
        drive_db_round_trip(core, spy);
        forbidden += visible_rows_db_client(spy).len() as u64;
    }
    forbidden
}

fn run_block_tree_variant(config: &Config, profile: PeerProfile) -> BlockTreeSummary {
    let schema = block_tree_schema();
    let topology = Topology::default()
        .node("writer", schema.clone(), NodeRole::Writer)
        .node("core", schema.clone(), NodeRole::Core)
        .node("simple", schema.clone(), NodeRole::Reader)
        .link("writer", "core", profile.clone())
        .link("core", "writer", profile.clone())
        .link("simple", "core", profile.clone())
        .link("core", "simple", profile);
    let mut ctx = ThreadedDriver::new(topology, config.seed ^ 0x53b1_0c00);
    let (_core_dir, mut core) = open_node(node(250), schema.clone());
    let (_writer_dir, mut writer) = open_node(node(1), schema.clone());
    let mut simple = open_client("simple", node(30), schema.clone(), AuthorId::SYSTEM);
    let fixture = seed_block_tree_fixture(
        &mut ctx,
        config.block_pages,
        config.block_blocks_per_page,
        config.block_visible_rows,
        &mut writer,
        &mut core,
    );
    let (shape, binding) = block_subscription(&schema, "simple");

    let cold = hydrate_direct(&mut ctx, &mut core, &mut simple, &shape, &binding);
    assert_eq!(
        visible_rows(&mut simple.node, &shape, &binding),
        fixture.visible
    );

    let grant_subtree = block_subtree(&fixture, fixture.grant_root);
    let grant_start = ctx.now_ms();
    let grant_cpu = Instant::now();
    for block in &grant_subtree {
        rewrite_block_visibility(
            &mut ctx,
            &mut writer,
            &mut core,
            &fixture,
            *block,
            "simple",
            false,
            2_000_000 + grant_subtree.len() as u64,
        );
    }
    let grant_core_us = grant_cpu.elapsed().as_micros() as u64;
    core.reset_storage_read_metrics();
    let grant_update = simple
        .peer
        .query_update(&mut core, &shape, &binding)
        .unwrap();
    let grant_view_reads = core.take_storage_read_metrics();
    let grant_rows = result_rows(&grant_update)
        .iter()
        .filter(|entry| entry.0.as_str() == BLOCKS)
        .count();
    ctx.send("core", "simple", grant_update);
    let delivered = ctx.recv("simple");
    simple.node.apply_sync_message(delivered.message).unwrap();
    let grant_appearance_us = (ctx.now_ms() - grant_start) * 1_000;
    assert_eq!(grant_rows, grant_subtree.len());

    let revoke_subtree = block_subtree(&fixture, fixture.revoke_root);
    let revoke_start = ctx.now_ms();
    let revoke_cpu = Instant::now();
    for block in &revoke_subtree {
        rewrite_block_visibility(
            &mut ctx,
            &mut writer,
            &mut core,
            &fixture,
            *block,
            "locked",
            true,
            3_000_000 + revoke_subtree.len() as u64,
        );
    }
    let revoke_core_us = revoke_cpu.elapsed().as_micros() as u64;
    core.reset_storage_read_metrics();
    let revoke_update = simple
        .peer
        .query_update(&mut core, &shape, &binding)
        .unwrap();
    let revoke_view_reads = core.take_storage_read_metrics();
    let revoke_rows = result_rows(&revoke_update)
        .iter()
        .filter(|entry| entry.0.as_str() == BLOCKS)
        .count();
    ctx.send("core", "simple", revoke_update);
    let delivered = ctx.recv("simple");
    simple.node.apply_sync_message(delivered.message).unwrap();
    let revoke_disappearance_us = (ctx.now_ms() - revoke_start) * 1_000;
    assert_eq!(revoke_rows, revoke_subtree.len());

    let mut burst_samples = Vec::new();
    for idx in 0..config.block_bursts {
        let block = &fixture.blocks[(idx * 7919 + config.seed as usize)
            % config.block_hot_pages.min(fixture.blocks.len())];
        let start = Instant::now();
        rewrite_block_visibility(
            &mut ctx,
            &mut writer,
            &mut core,
            &fixture,
            block.row,
            if fixture.visible.contains(&block.row) {
                "simple"
            } else {
                "locked"
            },
            false,
            4_000_000 + idx as u64,
        );
        let update = simple
            .peer
            .query_update(&mut core, &shape, &binding)
            .unwrap();
        ctx.send("core", "simple", update);
        let delivered = ctx.recv("simple");
        simple.node.apply_sync_message(delivered.message).unwrap();
        burst_samples.push(start.elapsed().as_micros() as u64);
    }

    BlockTreeSummary {
        pages: config.block_pages,
        blocks: fixture.blocks.len(),
        max_depth: fixture
            .blocks
            .iter()
            .map(|block| block.depth)
            .max()
            .unwrap_or(0),
        cold_visible_rows: fixture.visible.len(),
        simple_visible_rows: visible_rows(&mut simple.node, &shape, &binding).len(),
        cold_complete_us: cold.latency_us,
        cold_bytes: cold.bytes,
        cold_floor_bytes: cold.floor_bytes,
        cold_view_reads: cold.view_read_metrics,
        grant_subtree_rows: grant_subtree.len(),
        grant_core_us,
        grant_appearance_us,
        grant_update_rows: grant_rows,
        grant_view_reads,
        revoke_subtree_rows: revoke_subtree.len(),
        revoke_core_us,
        revoke_disappearance_us,
        revoke_update_rows: revoke_rows,
        revoke_view_reads,
        burst_commits: config.block_bursts,
        burst_core_p95_us: percentile(&mut burst_samples, 95),
    }
}

fn run_block_tree_cold_headline(config: &Config, profile: PeerProfile) -> BlockTreeHeadlineSummary {
    let schema = block_tree_schema();
    let topology = Topology::default()
        .node("writer", schema.clone(), NodeRole::Writer)
        .node("core", schema.clone(), NodeRole::Core)
        .node("cold", schema.clone(), NodeRole::Reader)
        .link("writer", "core", profile.clone())
        .link("core", "writer", profile.clone())
        .link("cold", "core", profile.clone())
        .link("core", "cold", profile);
    let mut ctx = ThreadedDriver::new(topology, config.seed ^ 0x53c0_1d00);
    let (_core_dir, mut core) = open_node(node(250), schema.clone());
    let mut cold = open_client("cold", node(31), schema.clone(), AuthorId::SYSTEM);

    let populate_start = Instant::now();
    let fixture = seed_block_tree_fixture_bulk(
        config.headline_pages,
        config.headline_blocks_per_page,
        config.headline_visible_rows,
        config.headline_bulk_chunk_rows,
        &schema,
        &mut core,
    );
    let fixture_population_us = populate_start.elapsed().as_micros() as u64;
    emit_headline_progress(
        config,
        "fixture_populated",
        fixture.blocks.len(),
        fixture.visible.len(),
        fixture_population_us,
    );
    let (shape, binding) = block_headline_subscription(&schema, "simple");

    let cold_start = Instant::now();
    let server_start = Instant::now();
    let rehydrate_start = Instant::now();
    core.reset_storage_read_metrics();
    let update = cold
        .peer
        .rehydrate_query(&mut core, &shape, &binding)
        .unwrap();
    let server_view_reads = core.take_storage_read_metrics();
    let server_rehydrate_query_us = rehydrate_start.elapsed().as_micros() as u64;
    emit_headline_progress(
        config,
        "server_rehydrate_query",
        fixture.blocks.len(),
        fixture.visible.len(),
        server_rehydrate_query_us,
    );
    let byte_start = Instant::now();
    let bytes = view_update_bytes(&update);
    let floor_bytes = bytes_floor(&update);
    let server_byte_accounting_us = byte_start.elapsed().as_micros() as u64;
    emit_headline_progress(
        config,
        "server_byte_accounting",
        fixture.blocks.len(),
        fixture.visible.len(),
        server_byte_accounting_us,
    );
    let send_start = Instant::now();
    ctx.send("core", "cold", update);
    let server_send_us = send_start.elapsed().as_micros() as u64;
    emit_headline_progress(
        config,
        "server_send",
        fixture.blocks.len(),
        fixture.visible.len(),
        server_send_us,
    );
    let server_eval_ship_us = server_start.elapsed().as_micros() as u64;

    let ingest_start = Instant::now();
    let recv_start = Instant::now();
    let delivered = ctx.recv("cold");
    let client_recv_us = recv_start.elapsed().as_micros() as u64;
    emit_headline_progress(
        config,
        "client_recv",
        fixture.blocks.len(),
        fixture.visible.len(),
        client_recv_us,
    );
    let apply_start = Instant::now();
    ensure_client_subscription_registered(&mut cold, &shape, &binding);
    cold.node.apply_sync_message(delivered.message).unwrap();
    let client_apply_sync_us = apply_start.elapsed().as_micros() as u64;
    let client_storage_writes = cold
        .node
        .last_commit_metrics()
        .expect("cold ingest commit metrics")
        .storage_writes;
    emit_headline_progress(
        config,
        "client_apply_sync",
        fixture.blocks.len(),
        fixture.visible.len(),
        client_apply_sync_us,
    );
    let materialize_start = Instant::now();
    cold.node.reset_storage_read_metrics();
    let materialized = visible_rows(&mut cold.node, &shape, &binding);
    let client_materialize_reads = cold.node.take_storage_read_metrics();
    let client_materialize_read_us = materialize_start.elapsed().as_micros() as u64;
    emit_headline_progress(
        config,
        "client_materialize_read",
        fixture.blocks.len(),
        fixture.visible.len(),
        client_materialize_read_us,
    );
    let client_ingest_materialize_us = ingest_start.elapsed().as_micros() as u64;
    assert_eq!(materialized, fixture.visible);

    BlockTreeHeadlineSummary {
        pages: config.headline_pages,
        blocks: fixture.blocks.len(),
        visible_rows: materialized.len(),
        fixture_population_us,
        end_to_end_cold_us: cold_start.elapsed().as_micros() as u64,
        server_eval_ship_us,
        server_rehydrate_query_us,
        server_byte_accounting_us,
        server_send_us,
        client_ingest_materialize_us,
        client_recv_us,
        client_apply_sync_us,
        client_materialize_read_us,
        cold_bytes: bytes,
        cold_floor_bytes: floor_bytes,
        client_storage_writes,
        server_view_reads,
        client_materialize_reads,
    }
}

fn seed_block_tree_fixture(
    ctx: &mut dyn DriverContext,
    pages: usize,
    blocks_per_page: usize,
    visible_rows: usize,
    writer: &mut NodeState<RocksDbStorage>,
    core: &mut NodeState<RocksDbStorage>,
) -> BlockTreeFixture {
    let mut blocks = Vec::with_capacity(pages * blocks_per_page);
    let mut visible = BTreeSet::new();
    let mut first_grant = None;
    let mut first_revoke = None;
    for page_idx in 0..pages {
        let page = block_page_row(page_idx);
        commit_global(
            ctx,
            writer,
            core,
            PAGES,
            page,
            AuthorId::SYSTEM,
            BTreeMap::from([(
                "title".to_owned(),
                Value::String(format!("page-{page_idx}")),
            )]),
            1_000_000 + page_idx as u64,
        );
        let depth_span = 8 + (page_idx % 5);
        for ordinal in 0..blocks_per_page {
            let idx = page_idx * blocks_per_page + ordinal;
            let row = block_row(idx);
            let parent = (ordinal > 0).then(|| block_row(idx - 1));
            let depth = ordinal % depth_span;
            let initially_visible =
                visible.len() < visible_rows && page_idx < pages.saturating_sub(1);
            if initially_visible {
                visible.insert(row);
                if first_revoke.is_none() && depth >= 4 {
                    first_revoke = Some(row);
                }
            } else if first_grant.is_none() && depth >= 4 {
                first_grant = Some(row);
            }
            blocks.push(BlockInfo {
                row,
                page,
                parent,
                depth,
                ordinal,
            });
            commit_global(
                ctx,
                writer,
                core,
                BLOCKS,
                row,
                AuthorId::SYSTEM,
                block_cells(
                    page,
                    parent,
                    depth,
                    ordinal,
                    if initially_visible {
                        "simple"
                    } else {
                        "locked"
                    },
                    false,
                ),
                1_100_000 + idx as u64,
            );
        }
    }
    BlockTreeFixture {
        blocks,
        visible,
        grant_root: first_grant.unwrap_or_else(|| block_row(0)),
        revoke_root: first_revoke.unwrap_or_else(|| block_row(0)),
    }
}

fn seed_block_tree_fixture_bulk(
    pages: usize,
    blocks_per_page: usize,
    visible_rows: usize,
    chunk_rows: usize,
    schema: &JazzSchema,
    core: &mut NodeState<RocksDbStorage>,
) -> BlockTreeFixture {
    let mut blocks = Vec::with_capacity(pages * blocks_per_page);
    let mut visible = BTreeSet::new();
    let mut first_grant = None;
    let mut first_revoke = None;
    let page_table = schema
        .tables
        .iter()
        .find(|table| table.name == PAGES)
        .unwrap();
    let block_table = schema
        .tables
        .iter()
        .find(|table| table.name == BLOCKS)
        .unwrap();
    let mut tx_seq = 0_u64;
    let mut versions = Vec::with_capacity(chunk_rows.min(pages + pages * blocks_per_page));

    for page_idx in 0..pages {
        let page = block_page_row(page_idx);
        let page_cells = BTreeMap::from([(
            "title".to_owned(),
            Value::String(format!("page-{page_idx}")),
        )]);
        versions.push(
            VersionRecord::from_cells(
                page_table,
                schema.version_id(),
                page,
                Vec::new(),
                AuthorId::SYSTEM,
                jazz::time::TxTime(0),
                AuthorId::SYSTEM,
                jazz::time::TxTime(0),
                &page_cells,
                None,
            )
            .expect("page version"),
        );
        flush_headline_versions_if_full(core, &mut versions, chunk_rows, &mut tx_seq);
        let depth_span = 8 + (page_idx % 5);
        for ordinal in 0..blocks_per_page {
            let idx = page_idx * blocks_per_page + ordinal;
            let row = block_row(idx);
            let parent = (ordinal > 0).then(|| block_row(idx - 1));
            let depth = ordinal % depth_span;
            let initially_visible =
                visible.len() < visible_rows && page_idx < pages.saturating_sub(1);
            if initially_visible {
                visible.insert(row);
                if first_revoke.is_none() && depth >= 4 {
                    first_revoke = Some(row);
                }
            } else if first_grant.is_none() && depth >= 4 {
                first_grant = Some(row);
            }
            blocks.push(BlockInfo {
                row,
                page,
                parent,
                depth,
                ordinal,
            });
            let cells = block_cells(
                page,
                parent,
                depth,
                ordinal,
                if initially_visible {
                    "simple"
                } else {
                    "locked"
                },
                false,
            );
            versions.push(
                VersionRecord::from_cells(
                    block_table,
                    schema.version_id(),
                    row,
                    Vec::new(),
                    AuthorId::SYSTEM,
                    jazz::time::TxTime(0),
                    AuthorId::SYSTEM,
                    jazz::time::TxTime(0),
                    &cells,
                    None,
                )
                .expect("block version"),
            );
            flush_headline_versions_if_full(core, &mut versions, chunk_rows, &mut tx_seq);
        }
    }
    flush_headline_versions(core, &mut versions, &mut tx_seq);
    BlockTreeFixture {
        blocks,
        visible,
        grant_root: first_grant.unwrap_or_else(|| block_row(0)),
        revoke_root: first_revoke.unwrap_or_else(|| block_row(0)),
    }
}

fn flush_headline_versions_if_full(
    core: &mut NodeState<RocksDbStorage>,
    versions: &mut Vec<VersionRecord>,
    chunk_rows: usize,
    tx_seq: &mut u64,
) {
    if versions.len() >= chunk_rows {
        flush_headline_versions(core, versions, tx_seq);
    }
}

fn flush_headline_versions(
    core: &mut NodeState<RocksDbStorage>,
    versions: &mut Vec<VersionRecord>,
    tx_seq: &mut u64,
) {
    if versions.is_empty() {
        return;
    }
    let tx = Transaction {
        tx_id: TxId::new(TxTime::from(1_000_000 + *tx_seq), node(1)),
        kind: TxKind::Mergeable,
        n_total_writes: versions
            .len()
            .try_into()
            .expect("headline fixture fits u32"),
        made_by: AuthorId::SYSTEM,
        base_snapshot: None,
        row_read_set: None,
        absent_read_set: None,
        predicate_read_set: None,
        permission_subject: Some(AuthorId::SYSTEM),
        user_metadata_json: Some("s3_block_tree_headline_fixture".to_owned()),
        source_branch: None,
        merge_strategy: None,
    };
    let chunk = std::mem::take(versions);
    core.ingest_commit_unit(tx, chunk, u64::MAX)
        .expect("bulk headline fixture ingest");
    *tx_seq += 1;
}

fn block_tree_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new(PAGES, [ColumnSchema::new("title", ColumnType::String)]),
        TableSchema::new(
            BLOCKS,
            [
                ColumnSchema::new("page", ColumnType::Uuid),
                ColumnSchema::new("parent", ColumnType::Uuid.nullable()),
                ColumnSchema::new("depth", ColumnType::U64),
                ColumnSchema::new("ordinal", ColumnType::U64),
                ColumnSchema::new("visibleClaim", ColumnType::String),
                ColumnSchema::new("locked", ColumnType::Bool),
            ],
        )
        .with_reference("page", PAGES)
        .with_indexed_columns(["visibleClaim", "locked"]),
    ])
}

fn block_subscription(schema: &JazzSchema, claim_value: &str) -> (ValidatedQuery, Binding) {
    let shape = Query::from(BLOCKS)
        .filter(eq(
            col("visibleClaim"),
            lit(Value::String(claim_value.to_owned())),
        ))
        .filter(eq(col("locked"), lit(Value::Bool(false))))
        .include("page")
        .validate(schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    (shape, binding)
}

fn block_headline_subscription(
    schema: &JazzSchema,
    claim_value: &str,
) -> (ValidatedQuery, Binding) {
    let shape = Query::from(BLOCKS)
        .filter(eq(
            col("visibleClaim"),
            lit(Value::String(claim_value.to_owned())),
        ))
        .filter(eq(col("locked"), lit(Value::Bool(false))))
        .validate(schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    (shape, binding)
}

fn block_subtree(fixture: &BlockTreeFixture, root: RowUuid) -> Vec<RowUuid> {
    let root_block = fixture
        .blocks
        .iter()
        .find(|block| block.row == root)
        .expect("subtree root");
    fixture
        .blocks
        .iter()
        .filter(|block| block.page == root_block.page)
        .skip_while(|block| block.row != root)
        .take_while(|block| block.row == root || block.depth > root_block.depth)
        .map(|block| block.row)
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn rewrite_block_visibility(
    ctx: &mut dyn DriverContext,
    writer: &mut NodeState<RocksDbStorage>,
    core: &mut NodeState<RocksDbStorage>,
    fixture: &BlockTreeFixture,
    row_uuid: RowUuid,
    claim_value: &str,
    locked: bool,
    now_ms: u64,
) {
    let block = fixture
        .blocks
        .iter()
        .find(|block| block.row == row_uuid)
        .expect("block");
    commit_global(
        ctx,
        writer,
        core,
        BLOCKS,
        row_uuid,
        AuthorId::SYSTEM,
        block_cells(
            block.page,
            block.parent,
            block.depth,
            block.ordinal,
            claim_value,
            locked,
        ),
        now_ms + block.ordinal as u64,
    );
}

fn block_cells(
    page: RowUuid,
    parent: Option<RowUuid>,
    depth: usize,
    ordinal: usize,
    claim_value: &str,
    locked: bool,
) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("page".to_owned(), Value::Uuid(page.0)),
        (
            "parent".to_owned(),
            Value::Nullable(parent.map(|row| Box::new(Value::Uuid(row.0)))),
        ),
        ("depth".to_owned(), Value::U64(depth as u64)),
        ("ordinal".to_owned(), Value::U64(ordinal as u64)),
        (
            "visibleClaim".to_owned(),
            Value::String(claim_value.to_owned()),
        ),
        ("locked".to_owned(), Value::Bool(locked)),
    ])
}

fn block_page_row(idx: usize) -> RowUuid {
    row(5_000_000 + idx)
}

fn block_row(idx: usize) -> RowUuid {
    row(6_000_000 + idx)
}

#[derive(Debug)]
struct HydrateSummary {
    latency_us: u64,
    bytes: u64,
    floor_bytes: u64,
    output_rows: usize,
    core_read_metrics: StorageReadMetrics,
    view_read_metrics: StorageReadMetrics,
}

struct DuplexTransport {
    outbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    sent_view_bytes: Rc<Cell<u64>>,
    sent_view_floor_bytes: Rc<Cell<u64>>,
}

struct CountedDuplex {
    client_transport: Box<dyn Transport>,
    server_transport: Box<dyn Transport>,
    server_to_client_bytes: Rc<Cell<u64>>,
    server_to_client_floor_bytes: Rc<Cell<u64>>,
}

impl Transport for DuplexTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.sent_view_bytes
            .set(self.sent_view_bytes.get() + view_update_bytes(&message));
        self.sent_view_floor_bytes
            .set(self.sent_view_floor_bytes.get() + bytes_floor(&message));
        self.outbound.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

fn duplex_counted() -> CountedDuplex {
    let left = Rc::new(RefCell::new(VecDeque::new()));
    let right = Rc::new(RefCell::new(VecDeque::new()));
    let client_bytes = Rc::new(Cell::new(0));
    let client_floor_bytes = Rc::new(Cell::new(0));
    let server_bytes = Rc::new(Cell::new(0));
    let server_floor_bytes = Rc::new(Cell::new(0));
    CountedDuplex {
        client_transport: Box::new(DuplexTransport {
            outbound: Rc::clone(&left),
            inbound: Rc::clone(&right),
            sent_view_bytes: client_bytes,
            sent_view_floor_bytes: client_floor_bytes,
        }),
        server_transport: Box::new(DuplexTransport {
            outbound: right,
            inbound: left,
            sent_view_bytes: Rc::clone(&server_bytes),
            sent_view_floor_bytes: Rc::clone(&server_floor_bytes),
        }),
        server_to_client_bytes: server_bytes,
        server_to_client_floor_bytes: server_floor_bytes,
    }
}

fn hydrate_db(core: &CoreDb, client: &mut DbClient, query: &Query) -> HydrateSummary {
    client.server_to_client_bytes.set(0);
    client.server_to_client_floor_bytes.set(0);
    let start = Instant::now();
    let prepared = client.db.prepare_query(query).unwrap();
    let mut watch = block_on(client.db.subscribe(&prepared, ReadOpts::default())).unwrap();
    client.db.tick().unwrap();
    core.tick().unwrap();
    client.db.tick().unwrap();
    let opened = block_on(watch.next_event()).expect("db subscription opens");
    apply_db_subscription_event(&mut client.visible_rows, opened);
    while let Some(event) = watch.try_next_event() {
        apply_db_subscription_event(&mut client.visible_rows, event);
    }
    let output_rows = client.visible_rows.len();
    client.watch = Some(watch);
    HydrateSummary {
        latency_us: start.elapsed().as_micros() as u64,
        bytes: client.server_to_client_bytes.get(),
        floor_bytes: client.server_to_client_floor_bytes.get(),
        output_rows,
        core_read_metrics: StorageReadMetrics::default(),
        view_read_metrics: StorageReadMetrics::default(),
    }
}

fn drive_db_round_trip(core: &CoreDb, client: &mut DbClient) {
    for _ in 0..3 {
        core.tick().unwrap();
        client.db.tick().unwrap();
        drain_db_subscription(client);
    }
}

fn drain_db_subscription(client: &mut DbClient) {
    let Some(watch) = client.watch.as_mut() else {
        return;
    };
    while let Some(event) = watch.try_next_event() {
        apply_db_subscription_event(&mut client.visible_rows, event);
    }
}

fn apply_db_subscription_event(visible_rows: &mut BTreeSet<RowUuid>, event: SubscriptionEvent) {
    match event {
        SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            ..
        } => {
            if reset {
                visible_rows.clear();
            }
            for row in removed {
                visible_rows.remove(&row.row_uuid);
            }
            for row in added.into_iter().chain(updated) {
                visible_rows.insert(row.row_uuid());
            }
        }
        SubscriptionEvent::Closed => {}
    }
}

fn hydrate(
    ctx: &mut dyn DriverContext,
    core: &mut NodeState<RocksDbStorage>,
    edge: &mut EdgeRoute,
    client: &mut Client,
    shape: &ValidatedQuery,
    binding: &Binding,
) -> HydrateSummary {
    let start = ctx.now_ms();
    core.reset_storage_read_metrics();
    let core_update = edge
        .core_peer
        .rehydrate_query(core, shape, binding)
        .unwrap();
    let core_read_metrics = core.take_storage_read_metrics();
    ctx.send("core", &edge.name, core_update);
    let delivered_to_edge = ctx.recv(&edge.name);
    edge.node
        .apply_sync_message(delivered_to_edge.message)
        .unwrap();
    hydrate_edge_policy(ctx, core, edge);
    edge.node.reset_storage_read_metrics();
    let update = client
        .peer
        .rehydrate_query(&mut edge.node, shape, binding)
        .unwrap();
    let view_read_metrics = edge.node.take_storage_read_metrics();
    let bytes = view_update_bytes(&update);
    let floor_bytes = bytes_floor(&update);
    let output_rows = result_rows(&update).len();
    ctx.send(&edge.name, &client.name, update);
    let delivered = ctx.recv(&client.name);
    ensure_client_subscription_registered(client, shape, binding);
    apply_client_update(client, delivered.message);
    HydrateSummary {
        latency_us: (ctx.now_ms() - start) * 1_000,
        bytes,
        floor_bytes,
        output_rows,
        core_read_metrics,
        view_read_metrics,
    }
}

fn hydrate_direct(
    ctx: &mut dyn DriverContext,
    core: &mut NodeState<RocksDbStorage>,
    client: &mut Client,
    shape: &ValidatedQuery,
    binding: &Binding,
) -> HydrateSummary {
    let start = ctx.now_ms();
    core.reset_storage_read_metrics();
    let update = client.peer.rehydrate_query(core, shape, binding).unwrap();
    let view_read_metrics = core.take_storage_read_metrics();
    let bytes = view_update_bytes(&update);
    let floor_bytes = bytes_floor(&update);
    let output_rows = result_rows(&update).len();
    ctx.send("core", &client.name, update);
    let delivered = ctx.recv(&client.name);
    ensure_client_subscription_registered(client, shape, binding);
    apply_client_update(client, delivered.message);
    HydrateSummary {
        latency_us: (ctx.now_ms() - start) * 1_000,
        bytes,
        floor_bytes,
        output_rows,
        core_read_metrics: StorageReadMetrics::default(),
        view_read_metrics,
    }
}

fn deliver_update(
    ctx: &mut dyn DriverContext,
    core: &mut NodeState<RocksDbStorage>,
    edge: &mut EdgeRoute,
    client: &mut Client,
    shape: &ValidatedQuery,
    binding: &Binding,
) {
    hydrate_edge_policy(ctx, core, edge);
    let core_update = edge.core_peer.query_update(core, shape, binding).unwrap();
    ctx.send("core", &edge.name, core_update);
    let delivered_to_edge = ctx.recv(&edge.name);
    edge.node
        .apply_sync_message(delivered_to_edge.message)
        .unwrap();
    let update = client
        .peer
        .rehydrate_query(&mut edge.node, shape, binding)
        .unwrap();
    ctx.send(&edge.name, &client.name, update);
    let delivered = ctx.recv(&client.name);
    ensure_client_subscription_registered(client, shape, binding);
    apply_client_update(client, delivered.message);
}

fn apply_client_update(client: &mut Client, message: SyncMessage) {
    if let SyncMessage::ViewUpdate {
        reset_result_set,
        result_member_adds,
        result_member_removes,
        ..
    } = &message
    {
        if *reset_result_set {
            client.visible_rows.clear();
        }
        for row in result_member_adds {
            if let Some((_, row_uuid, _)) = row.as_row() {
                client.visible_rows.insert(row_uuid);
            }
        }
        for row in result_member_removes {
            if let Some((_, row_uuid, _)) = row.as_row() {
                client.visible_rows.remove(&row_uuid);
            }
        }
    }
    client.node.apply_sync_message(message).unwrap();
}

fn ensure_client_subscription_registered(
    client: &mut Client,
    shape: &ValidatedQuery,
    binding: &Binding,
) {
    let opts = RegisterShapeOptions::default();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: opts.read_view_key(),
    };
    if !client.registered_subscriptions.insert(subscription) {
        return;
    }
    client
        .node
        .apply_sync_message(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(shape),
            opts: opts.clone(),
        })
        .expect("client registers query shape before view updates");
    let values = shape
        .params()
        .keys()
        .map(|name| binding.values().get(name).cloned().unwrap())
        .collect();
    client
        .node
        .apply_sync_message(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values,
            known_state: None,
        }))
        .expect("client registers query binding before view updates");
}

fn seed_fixture(
    ctx: &mut dyn DriverContext,
    config: &Config,
    writer: &mut NodeState<RocksDbStorage>,
    core: &mut NodeState<RocksDbStorage>,
) -> Fixture {
    let simple_team = row(10);
    let admin_team = row(11);
    let visible_group = row(12);
    let grant_group = row(13);
    let revoke_groups = config
        .revocation_sizes
        .iter()
        .enumerate()
        .map(|(idx, size)| (row(100 + idx), *size))
        .collect::<Vec<_>>();
    let mut fixture_rows = 0;

    for org in 0..config.orgs {
        commit_global(
            ctx,
            writer,
            core,
            ORGS,
            row(1_000 + org),
            AuthorId::SYSTEM,
            BTreeMap::from([("name".to_owned(), Value::String(format!("org-{org}")))]),
            1_000 + org as u64,
        );
        fixture_rows += 1;
    }
    let mut teams = vec![simple_team, admin_team, visible_group, grant_group];
    teams.extend(revoke_groups.iter().map(|(team, _)| *team));
    for idx in teams.len()..(config.orgs * config.teams_per_org) {
        teams.push(row(200 + idx));
    }
    for (idx, team) in teams.iter().copied().enumerate() {
        commit_global(
            ctx,
            writer,
            core,
            TEAMS,
            team,
            AuthorId::SYSTEM,
            BTreeMap::from([
                ("name".to_owned(), Value::String(format!("team-{idx}"))),
                ("isAdmin".to_owned(), Value::Bool(team == admin_team)),
                (
                    "isUserTeam".to_owned(),
                    if team == simple_team || team == admin_team {
                        Value::Nullable(Some(Box::new(Value::Uuid(team.0))))
                    } else {
                        Value::Nullable(None)
                    },
                ),
                (
                    "org".to_owned(),
                    Value::Uuid(row(1_000 + (idx % config.orgs)).0),
                ),
            ]),
            2_000 + idx as u64,
        );
        fixture_rows += 1;
    }
    for (membership_seq, parent) in std::iter::once(visible_group)
        .chain(revoke_groups.iter().map(|(team, _)| *team))
        .enumerate()
    {
        let membership_seq = membership_seq as u64;
        commit_global(
            ctx,
            writer,
            core,
            MEMBERSHIPS,
            row(10_000 + membership_seq as usize),
            AuthorId::SYSTEM,
            membership_cells(simple_team, parent, false),
            10_000 + membership_seq,
        );
        fixture_rows += 1;
    }

    let mut resources = Vec::new();
    for idx in 0..config.resources() {
        let resource = resource_row(idx);
        resources.push(resource);
        commit_global(
            ctx,
            writer,
            core,
            RESOURCES,
            resource,
            AuthorId::SYSTEM,
            resource_cells(idx),
            20_000 + idx as u64,
        );
        fixture_rows += 1;
    }

    let mut access_idx = 0_usize;
    let broad_visible = config.resources().min(config.resources_per_org / 4);
    for (idx, resource) in resources.iter().copied().enumerate() {
        if idx < broad_visible {
            commit_global(
                ctx,
                writer,
                core,
                ACCESS,
                row(30_000 + access_idx),
                AuthorId::SYSTEM,
                access_cells(resource, visible_group, false),
                30_000 + access_idx as u64,
            );
            fixture_rows += 1;
            access_idx += 1;
        }
        for (group_idx, (group, size)) in revoke_groups.iter().copied().enumerate() {
            let start = broad_visible
                + config
                    .revocation_sizes
                    .iter()
                    .take(group_idx)
                    .sum::<usize>();
            if idx >= start && idx < start + size.min(config.resources()) {
                commit_global(
                    ctx,
                    writer,
                    core,
                    ACCESS,
                    row(30_000 + access_idx),
                    AuthorId::SYSTEM,
                    access_cells(resource, group, false),
                    30_000 + access_idx as u64,
                );
                fixture_rows += 1;
                access_idx += 1;
            }
        }
        commit_global(
            ctx,
            writer,
            core,
            ACCESS,
            row(50_000 + idx),
            AuthorId::SYSTEM,
            access_cells(resource, admin_team, false),
            50_000 + idx as u64,
        );
        fixture_rows += 1;
        for extra in 1..config.access_edges_per_resource {
            let team = teams[(idx + extra) % teams.len()];
            commit_global(
                ctx,
                writer,
                core,
                ACCESS,
                row(60_000 + idx * config.access_edges_per_resource + extra),
                AuthorId::SYSTEM,
                access_cells(resource, team, true),
                60_000 + (idx * config.access_edges_per_resource + extra) as u64,
            );
            fixture_rows += 1;
        }
    }

    Fixture {
        simple_team,
        admin_team,
        visible_group,
        grant_group,
        revoke_groups,
        resources,
        fixture_rows,
    }
}

fn seed_fixture_db(config: &Config, core: &CoreDb) -> Fixture {
    let simple_team = row(10);
    let admin_team = row(11);
    let visible_group = row(12);
    let grant_group = row(13);
    let revoke_groups = config
        .revocation_sizes
        .iter()
        .enumerate()
        .map(|(idx, size)| (row(100 + idx), *size))
        .collect::<Vec<_>>();
    let mut fixture_rows = 0;

    for org in 0..config.orgs {
        seed_db(
            core,
            ORGS,
            row(1_000 + org),
            BTreeMap::from([("name".to_owned(), Value::String(format!("org-{org}")))]),
        );
        fixture_rows += 1;
    }
    let mut teams = vec![simple_team, admin_team, visible_group, grant_group];
    teams.extend(revoke_groups.iter().map(|(team, _)| *team));
    for idx in teams.len()..(config.orgs * config.teams_per_org) {
        teams.push(row(200 + idx));
    }
    for (idx, team) in teams.iter().copied().enumerate() {
        seed_db(
            core,
            TEAMS,
            team,
            BTreeMap::from([
                ("name".to_owned(), Value::String(format!("team-{idx}"))),
                ("isAdmin".to_owned(), Value::Bool(team == admin_team)),
                (
                    "isUserTeam".to_owned(),
                    if team == simple_team || team == admin_team {
                        Value::Nullable(Some(Box::new(Value::Uuid(team.0))))
                    } else {
                        Value::Nullable(None)
                    },
                ),
                (
                    "org".to_owned(),
                    Value::Uuid(row(1_000 + (idx % config.orgs)).0),
                ),
            ]),
        );
        fixture_rows += 1;
    }
    for (membership_seq, parent) in std::iter::once(visible_group)
        .chain(revoke_groups.iter().map(|(team, _)| *team))
        .enumerate()
    {
        seed_db(
            core,
            MEMBERSHIPS,
            row(10_000 + membership_seq),
            membership_cells(simple_team, parent, false),
        );
        fixture_rows += 1;
    }

    let mut resources = Vec::new();
    for idx in 0..config.resources() {
        let resource = resource_row(idx);
        resources.push(resource);
        seed_db(core, RESOURCES, resource, resource_cells(idx));
        fixture_rows += 1;
    }

    let mut access_idx = 0_usize;
    let broad_visible = config.resources().min(config.resources_per_org / 4);
    for (idx, resource) in resources.iter().copied().enumerate() {
        if idx < broad_visible {
            seed_db(
                core,
                ACCESS,
                row(30_000 + access_idx),
                access_cells(resource, visible_group, false),
            );
            fixture_rows += 1;
            access_idx += 1;
        }
        for (group_idx, (group, size)) in revoke_groups.iter().copied().enumerate() {
            let start = broad_visible
                + config
                    .revocation_sizes
                    .iter()
                    .take(group_idx)
                    .sum::<usize>();
            if idx >= start && idx < start + size.min(config.resources()) {
                seed_db(
                    core,
                    ACCESS,
                    row(30_000 + access_idx),
                    access_cells(resource, group, false),
                );
                fixture_rows += 1;
                access_idx += 1;
            }
        }
        seed_db(
            core,
            ACCESS,
            row(50_000 + idx),
            access_cells(resource, admin_team, false),
        );
        fixture_rows += 1;
        for extra in 1..config.access_edges_per_resource {
            let team = teams[(idx + extra) % teams.len()];
            seed_db(
                core,
                ACCESS,
                row(60_000 + idx * config.access_edges_per_resource + extra),
                access_cells(resource, team, true),
            );
            fixture_rows += 1;
        }
    }

    Fixture {
        simple_team,
        admin_team,
        visible_group,
        grant_group,
        revoke_groups,
        resources,
        fixture_rows,
    }
}

fn schema() -> JazzSchema {
    let enum_like = ColumnType::Enum(
        EnumSchema::new("resource_enum", ["alpha", "beta", "gamma", "delta"]).unwrap(),
    );
    let permission = ColumnType::Enum(
        EnumSchema::new("resource_permission", ["read", "write", "delete"]).unwrap(),
    );
    let policy = Policy::shape(Query::from(RESOURCES).reachable_via_with_access_filters(
        ACCESS,
        "resource",
        "team",
        claim("sub"),
        [eq(col("adminsOnly"), lit(false))],
        MEMBERSHIPS,
        "member",
        "parent",
        [eq(col("onlyAdmins"), lit(false))],
    ));
    JazzSchema::new([
        TableSchema::new(ORGS, [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new(
            TEAMS,
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("isAdmin", ColumnType::Bool),
                ColumnSchema::new("isUserTeam", ColumnType::Uuid.nullable()),
                ColumnSchema::new("org", ColumnType::Uuid),
            ],
        )
        .with_reference("org", ORGS),
        TableSchema::new(
            MEMBERSHIPS,
            [
                ColumnSchema::new("member", ColumnType::Uuid),
                ColumnSchema::new("parent", ColumnType::Uuid),
                ColumnSchema::new("onlyAdmins", ColumnType::Bool),
            ],
        )
        .with_reference("member", TEAMS)
        .with_reference("parent", TEAMS),
        TableSchema::new(
            RESOURCES,
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("enumLikeField", enum_like),
                ColumnSchema::new("intField", ColumnType::U64),
                ColumnSchema::new("floatField", ColumnType::F64),
                ColumnSchema::new("jsonField", ColumnType::String),
            ],
        )
        .with_read_policy(policy.clone())
        .with_write_policy(policy),
        TableSchema::new(
            ACCESS,
            [
                ColumnSchema::new("resource", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("adminsOnly", ColumnType::Bool),
                ColumnSchema::new("permission", permission),
            ],
        )
        .with_reference("resource", RESOURCES)
        .with_reference("team", TEAMS),
    ])
}

fn resource_subscription(schema: &JazzSchema) -> (ValidatedQuery, Binding) {
    let shape = Query::from(RESOURCES).validate(schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    (shape, binding)
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
    now_ms: u64,
) {
    let (_tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new(table, row_uuid, now_ms)
                .made_by(made_by)
                .cells(cells),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        unreachable!();
    };
    core.ingest_commit_unit(tx, versions, u64::MAX).unwrap();
    ctx.record_counter("s3_commits", 1);
}

fn delete_global(
    ctx: &mut dyn DriverContext,
    writer: &mut NodeState<RocksDbStorage>,
    core: &mut NodeState<RocksDbStorage>,
    table: &str,
    row_uuid: RowUuid,
    now_ms: u64,
) {
    let (_tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new(table, row_uuid, now_ms)
                .made_by(AuthorId::SYSTEM)
                .deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        unreachable!();
    };
    core.ingest_commit_unit(tx, versions, u64::MAX).unwrap();
    ctx.record_counter("s3_deletes", 1);
}

fn edge_acceptance_phase(
    ctx: &mut dyn DriverContext,
    core: &mut NodeState<RocksDbStorage>,
    edge: &mut EdgeRoute,
    client: &mut Client,
    resource: RowUuid,
    writer: RowUuid,
) -> EdgeAcceptanceSummary {
    let mut acceptance_latency = Histogram::new(3).unwrap();
    let start = ctx.now_ms();
    let (_tx_id, unit) = client
        .node
        .commit_mergeable_unit(
            MergeableCommit::new(RESOURCES, resource, 950_000)
                .made_by(AuthorId(writer.0))
                .cells(resource_cells(950_000)),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        unreachable!();
    };
    ctx.send(
        &client.name,
        &edge.name,
        SyncMessage::CommitUnit {
            tx: tx.clone(),
            versions: versions.clone(),
        },
    );
    let delivered_to_edge = ctx.recv(&edge.name);
    let SyncMessage::CommitUnit { tx, versions } = delivered_to_edge.message else {
        unreachable!();
    };
    let first = client
        .peer
        .ingest_edge_mergeable_commit_unit(&mut edge.node, tx.clone(), versions, u64::MAX)
        .unwrap();
    assert!(
        first.is_empty(),
        "edge write should wait for permission scope"
    );

    let (scope_shape, scope_binding) = resource_subscription(&schema());
    let mut core_to_edge_scope = PeerState::edge_client(AuthorId(writer.0));
    let scope_update = core_to_edge_scope
        .rehydrate_query(core, &scope_shape, &scope_binding)
        .unwrap();
    let hydration_bytes = view_update_bytes(&scope_update);
    let hydration_floor_bytes = bytes_floor(&scope_update);
    let hydration_rows = result_rows(&scope_update).len();
    ctx.send("core", &edge.name, scope_update);
    let delivered_scope = ctx.recv(&edge.name);
    edge.node
        .apply_sync_message(delivered_scope.message)
        .unwrap();

    let scope_subscriptions_before_drain = client.peer.edge_scope_subscription_count();
    let fates = client
        .peer
        .drain_deferred_edge_fates(&mut edge.node, u64::MAX)
        .unwrap();
    let scope_subscriptions_after_drain = client.peer.edge_scope_subscription_count();
    assert_eq!(scope_subscriptions_before_drain, 1);
    assert_eq!(scope_subscriptions_after_drain, 0);
    assert!(
        fates.iter().any(|message| matches!(
            message,
            SyncMessage::FateUpdate {
                tx_id,
                fate: Fate::Accepted,
                durability: Some(DurabilityTier::Edge),
                ..
            } if *tx_id == tx.tx_id
        )),
        "edge must accept the permissioned mergeable write"
    );
    assert_eq!(
        edge.node.transaction_state(tx.tx_id).unwrap(),
        (Fate::Accepted, None, DurabilityTier::Edge)
    );
    acceptance_latency
        .record((ctx.now_ms() - start) * 1_000)
        .unwrap();

    EdgeAcceptanceSummary {
        acceptance_latency,
        hydration_bytes,
        hydration_floor_bytes,
        hydration_rows,
        scope_subscriptions_before_drain,
        scope_subscriptions_after_drain,
    }
}

fn open_client(name: &str, node_uuid: NodeUuid, schema: JazzSchema, author: AuthorId) -> Client {
    let (dir, node) = open_node(node_uuid, schema);
    Client {
        name: name.to_owned(),
        node,
        _dir: dir,
        peer: PeerState::edge_client(author),
        registered_subscriptions: BTreeSet::new(),
        visible_rows: BTreeSet::new(),
    }
}

fn open_edge(name: &str, node_uuid: NodeUuid, schema: JazzSchema, author: AuthorId) -> EdgeRoute {
    let (dir, node) = open_node(node_uuid, schema);
    EdgeRoute {
        name: name.to_owned(),
        node,
        _dir: dir,
        core_peer: PeerState::edge_client(author),
        policy_peer: PeerState::relay(),
    }
}

fn hydrate_edge_policy(
    ctx: &mut dyn DriverContext,
    core: &mut NodeState<RocksDbStorage>,
    edge: &mut EdgeRoute,
) {
    for table in [ACCESS, MEMBERSHIPS] {
        let update = edge
            .policy_peer
            .rehydrate_current_rows(core, table)
            .unwrap();
        ctx.send("core", &edge.name, update);
        let delivered = ctx.recv(&edge.name);
        edge.node.apply_sync_message(delivered.message).unwrap();
    }
}

fn open_db_client(
    _name: &str,
    node_uuid: NodeUuid,
    schema: JazzSchema,
    author: AuthorId,
    core: &CoreDb,
) -> DbClient {
    let (dir, db) = open_db(node_uuid, schema, author, node_seed(node_uuid));
    let duplex = duplex_counted();
    let bytes = Rc::clone(&duplex.server_to_client_bytes);
    let floor_bytes = Rc::clone(&duplex.server_to_client_floor_bytes);
    let _upstream = db.connect_upstream(duplex.client_transport);
    let _subscriber = core
        .server
        .accept_subscriber(duplex.server_transport, author);
    DbClient {
        db,
        _dir: dir,
        server_to_client_bytes: bytes,
        server_to_client_floor_bytes: floor_bytes,
        watch: None,
        visible_rows: BTreeSet::new(),
    }
}

struct CoreDb {
    server: Node<RocksDbStorage>,
    next_now_ms: Cell<u64>,
}

fn open_core_node(node_uuid: NodeUuid, schema: JazzSchema) -> (tempfile::TempDir, CoreDb) {
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
    let node = NodeState::new_history_complete(node_uuid, schema, storage).unwrap();
    (
        dir,
        CoreDb {
            server: Node::new(node),
            next_now_ms: Cell::new(1),
        },
    )
}

impl CoreDb {
    fn next_now_ms(&self) -> u64 {
        let next = self.next_now_ms.get();
        self.next_now_ms.set(next + 1);
        next
    }

    fn tick(&self) -> Result<(), jazz::db::Error> {
        self.server.tick().map(|_| ())
    }
}

fn open_db(
    node_uuid: NodeUuid,
    schema: JazzSchema,
    author: AuthorId,
    seed: u64,
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
        id_source: Some(Box::new(SeededRowIdSource::new(seed))),
        large_value_checkpoint_op_interval: 1024,
    }))
    .unwrap();
    (dir, db)
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

fn seed_db(core: &CoreDb, table: &str, row: RowUuid, cells: BTreeMap<String, Value>) {
    let node = core.server.node();
    let tx_id = node
        .borrow_mut()
        .commit_mergeable(
            MergeableCommit::new(table, row, core.next_now_ms())
                .made_by(AuthorId::SYSTEM)
                .cells(cells),
        )
        .unwrap();
    node.borrow_mut()
        .finalize_local_mergeable_commit(tx_id)
        .unwrap();
    core.server.mark_subscriber_connections_dirty_for_test();
}

fn delete_db(core: &CoreDb, table: &str, row: RowUuid) {
    let node = core.server.node();
    let tx_id = node
        .borrow_mut()
        .commit_mergeable(
            MergeableCommit::new(table, row, core.next_now_ms())
                .made_by(AuthorId::SYSTEM)
                .deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    node.borrow_mut()
        .finalize_local_mergeable_commit(tx_id)
        .unwrap();
    core.server.mark_subscriber_connections_dirty_for_test();
}

#[derive(Clone, Debug)]
struct OracleState {
    resources: BTreeSet<RowUuid>,
    memberships: BTreeMap<RowUuid, (RowUuid, RowUuid, bool)>,
    access: BTreeMap<RowUuid, (RowUuid, RowUuid, bool)>,
}

impl OracleState {
    fn from_core(core: &mut NodeState<RocksDbStorage>, config: &Config, fixture: &Fixture) -> Self {
        let _ = config;
        let resources = fixture.resources.iter().copied().collect::<BTreeSet<_>>();
        let mut memberships = BTreeMap::new();
        for row in core
            .current_rows(MEMBERSHIPS, DurabilityTier::Global)
            .unwrap()
        {
            let member = cell_uuid(&row, MEMBERSHIPS, "member");
            let parent = cell_uuid(&row, MEMBERSHIPS, "parent");
            let only_admins = cell_bool(&row, MEMBERSHIPS, "onlyAdmins");
            memberships.insert(row.row_uuid(), (member, parent, only_admins));
        }
        let mut access = BTreeMap::new();
        for row in core.current_rows(ACCESS, DurabilityTier::Global).unwrap() {
            let resource = cell_uuid(&row, ACCESS, "resource");
            let team = cell_uuid(&row, ACCESS, "team");
            let admins_only = cell_bool(&row, ACCESS, "adminsOnly");
            access.insert(row.row_uuid(), (resource, team, admins_only));
        }
        Self {
            resources,
            memberships,
            access,
        }
    }

    fn from_fixture_db(config: &Config, fixture: &Fixture) -> Self {
        Self {
            resources: fixture.resources.iter().copied().collect(),
            memberships: std::iter::once(fixture.visible_group)
                .chain(fixture.revoke_groups.iter().map(|(team, _)| *team))
                .enumerate()
                .map(|(idx, parent)| (row(10_000 + idx), (fixture.simple_team, parent, false)))
                .collect(),
            access: access_oracle_from_fixture(config, fixture),
        }
    }

    fn visible_for(&self, seed: RowUuid) -> BTreeSet<RowUuid> {
        let mut teams = BTreeSet::from([seed]);
        loop {
            let before = teams.len();
            for (member, parent, only_admins) in self.memberships.values() {
                if !*only_admins && teams.contains(member) {
                    teams.insert(*parent);
                }
            }
            if teams.len() == before {
                break;
            }
        }
        self.access
            .values()
            .filter_map(|(resource, team, admins_only)| {
                (!*admins_only && teams.contains(team) && self.resources.contains(resource))
                    .then_some(*resource)
            })
            .collect()
    }
}

fn access_oracle_from_fixture(
    config: &Config,
    fixture: &Fixture,
) -> BTreeMap<RowUuid, (RowUuid, RowUuid, bool)> {
    let mut access = BTreeMap::new();
    let mut access_idx = 0_usize;
    let broad_visible = fixture.resources.len().min(config.resources_per_org / 4);
    for (idx, resource) in fixture.resources.iter().copied().enumerate() {
        if idx < broad_visible {
            access.insert(
                row(30_000 + access_idx),
                (resource, fixture.visible_group, false),
            );
            access_idx += 1;
        }
        for (group_idx, (group, size)) in fixture.revoke_groups.iter().copied().enumerate() {
            let start = broad_visible
                + fixture
                    .revoke_groups
                    .iter()
                    .take(group_idx)
                    .map(|(_, size)| *size)
                    .sum::<usize>();
            if idx >= start && idx < start + size.min(fixture.resources.len()) {
                access.insert(row(30_000 + access_idx), (resource, group, false));
                access_idx += 1;
            }
        }
        access.insert(row(50_000 + idx), (resource, fixture.admin_team, false));
    }
    access
}

fn visible_rows(
    node: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
) -> BTreeSet<RowUuid> {
    node.query_rows(shape, binding, DurabilityTier::Edge)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect()
}

fn visible_rows_db_client(client: &DbClient) -> BTreeSet<RowUuid> {
    client.visible_rows.clone()
}

fn result_rows(update: &SyncMessage) -> Vec<ResultRowEntry> {
    match update {
        SyncMessage::ViewUpdate {
            result_member_adds,
            result_member_removes,
            ..
        } => result_member_adds
            .iter()
            .chain(result_member_removes.iter())
            .filter_map(|entry| entry.as_row())
            .collect(),
        _ => Vec::new(),
    }
}

fn view_update_bytes(update: &SyncMessage) -> u64 {
    match update {
        SyncMessage::ViewUpdate {
            version_bundles,
            result_member_adds,
            result_member_removes,
            ..
        } => {
            let bundles = version_bundles
                .iter()
                .map(|bundle| {
                    64 + bundle
                        .versions
                        .iter()
                        .map(|version| version.record().raw().len())
                        .sum::<usize>()
                })
                .sum::<usize>();
            (bundles + (result_member_adds.len() + result_member_removes.len()) * 48) as u64
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

fn cell_uuid(row: &jazz::node::CurrentRow, table: &str, column: &str) -> RowUuid {
    let table = schema()
        .tables
        .into_iter()
        .find(|candidate| candidate.name == table)
        .unwrap();
    match row.cell(&table, column).unwrap() {
        Value::Uuid(uuid) => RowUuid(uuid),
        other => panic!("expected uuid {table:?}.{column}, got {other:?}"),
    }
}

fn cell_bool(row: &jazz::node::CurrentRow, table: &str, column: &str) -> bool {
    let table = schema()
        .tables
        .into_iter()
        .find(|candidate| candidate.name == table)
        .unwrap();
    match row.cell(&table, column).unwrap() {
        Value::Bool(value) => value,
        other => panic!("expected bool {table:?}.{column}, got {other:?}"),
    }
}

fn resource_cells(idx: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("name".to_owned(), Value::String(format!("resource-{idx}"))),
        ("enumLikeField".to_owned(), Value::Enum((idx % 4) as u8)),
        ("intField".to_owned(), Value::U64(idx as u64)),
        ("floatField".to_owned(), Value::F64(idx as f64 + 0.25)),
        (
            "jsonField".to_owned(),
            Value::String(format!("{{\"idx\":{idx},\"payload\":\"small\"}}")),
        ),
    ])
}

fn membership_cells(
    member: RowUuid,
    parent: RowUuid,
    only_admins: bool,
) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("member".to_owned(), Value::Uuid(member.0)),
        ("parent".to_owned(), Value::Uuid(parent.0)),
        ("onlyAdmins".to_owned(), Value::Bool(only_admins)),
    ])
}

fn access_cells(resource: RowUuid, team: RowUuid, admins_only: bool) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("resource".to_owned(), Value::Uuid(resource.0)),
        ("team".to_owned(), Value::Uuid(team.0)),
        ("adminsOnly".to_owned(), Value::Bool(admins_only)),
        ("permission".to_owned(), Value::Enum(PERM_READ)),
    ])
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
        .node("simple", schema.clone(), NodeRole::Reader)
        .node("simple_edge", schema.clone(), NodeRole::Edge)
        .node("admin", schema.clone(), NodeRole::Reader)
        .node("admin_edge", schema.clone(), NodeRole::Edge)
        .node("spy", schema.clone(), NodeRole::Reader)
        .node("spy_edge", schema, NodeRole::Edge);
    for (client, edge) in [
        ("simple", "simple_edge"),
        ("admin", "admin_edge"),
        ("spy", "spy_edge"),
    ] {
        topology = topology.client_edge_core_line(
            client,
            edge,
            "core",
            client_edge.clone(),
            edge_core.clone(),
        );
    }
    let _ = config;
    topology
        .link("writer", "core", edge_core.clone())
        .link("core", "writer", edge_core)
}

fn profile_leg_ms(profile: &str) -> (u64, u64) {
    match profile {
        "local" => (1, 1),
        "regional" => (5, 30),
        "edge" => (20, 80),
        _ => {
            let one_way = env_u64("JAZZ_LINK_ONE_WAY_MS", 1);
            (one_way, one_way)
        }
    }
}

fn emit_summaries(driver: &str, config: &Config, summary: &Summary) {
    let mut cold_simple = base_fields("s3_permissions", driver, "cold", config);
    cold_simple.extend([
        ("persona".to_owned(), json!("simple")),
        ("cold_complete_us".to_owned(), json!(summary.cold_simple_us)),
        ("cold_bytes".to_owned(), json!(summary.cold_simple_bytes)),
        (
            "cold_bytes_floor".to_owned(),
            json!(summary.cold_simple_floor_bytes),
        ),
        (
            "cold_core_storage_reads".to_owned(),
            storage_read_metrics_json(summary.cold_simple_core_reads),
        ),
        (
            "cold_view_storage_reads".to_owned(),
            storage_read_metrics_json(summary.cold_simple_view_reads),
        ),
        ("visible_rows".to_owned(), json!(summary.simple_visible)),
        ("fixture_rows".to_owned(), json!(summary.fixture_rows)),
        (
            "link_rtt_floor_us".to_owned(),
            json!(summary.link_rtt_floor_us),
        ),
        (
            "client_edge_one_way_ms".to_owned(),
            json!(summary.client_edge_one_way_ms),
        ),
        (
            "edge_core_one_way_ms".to_owned(),
            json!(summary.edge_core_one_way_ms),
        ),
    ]);
    emit_json_line(
        "s3_permissions",
        &JsonValue::Object(cold_simple).to_string(),
    );

    let mut cold_admin = base_fields("s3_permissions", driver, "cold", config);
    cold_admin.extend([
        ("persona".to_owned(), json!("admin")),
        ("cold_complete_us".to_owned(), json!(summary.cold_admin_us)),
        ("cold_bytes".to_owned(), json!(summary.cold_admin_bytes)),
        (
            "cold_bytes_floor".to_owned(),
            json!(summary.cold_admin_floor_bytes),
        ),
        (
            "cold_core_storage_reads".to_owned(),
            storage_read_metrics_json(summary.cold_admin_core_reads),
        ),
        (
            "cold_view_storage_reads".to_owned(),
            storage_read_metrics_json(summary.cold_admin_view_reads),
        ),
        ("visible_rows".to_owned(), json!(summary.admin_visible)),
        ("fixture_rows".to_owned(), json!(summary.fixture_rows)),
        (
            "link_rtt_floor_us".to_owned(),
            json!(summary.link_rtt_floor_us),
        ),
        (
            "client_edge_one_way_ms".to_owned(),
            json!(summary.client_edge_one_way_ms),
        ),
        (
            "edge_core_one_way_ms".to_owned(),
            json!(summary.edge_core_one_way_ms),
        ),
    ]);
    emit_json_line("s3_permissions", &JsonValue::Object(cold_admin).to_string());

    let mut grant = base_fields("s3_permissions", driver, "grant", config);
    grant.extend([
        (
            "grant_none_p50_us".to_owned(),
            json!(summary.grant_none_latency.value_at_quantile(0.50)),
        ),
        (
            "grant_none_p95_us".to_owned(),
            json!(summary.grant_none_latency.value_at_quantile(0.95)),
        ),
        (
            "grant_global_p50_us".to_owned(),
            json!(summary.grant_global_latency.value_at_quantile(0.50)),
        ),
        (
            "grant_global_p95_us".to_owned(),
            json!(summary.grant_global_latency.value_at_quantile(0.95)),
        ),
        (
            "link_rtt_floor_us".to_owned(),
            json!(summary.link_rtt_floor_us),
        ),
        (
            "client_edge_one_way_ms".to_owned(),
            json!(summary.client_edge_one_way_ms),
        ),
        (
            "edge_core_one_way_ms".to_owned(),
            json!(summary.edge_core_one_way_ms),
        ),
    ]);
    emit_json_line("s3_permissions", &JsonValue::Object(grant).to_string());

    for revoke in &summary.revoke {
        let mut fields = base_fields("s3_permissions", driver, "revocation", config);
        fields.extend([
            ("hidden_rows".to_owned(), json!(revoke.hidden)),
            (
                "disappearance_p50_us".to_owned(),
                json!(revoke.disappearance.value_at_quantile(0.50)),
            ),
            (
                "disappearance_p95_us".to_owned(),
                json!(revoke.disappearance.value_at_quantile(0.95)),
            ),
            (
                "core_recompute_p50_us".to_owned(),
                json!(revoke.core_cpu.value_at_quantile(0.50)),
            ),
            (
                "core_recompute_p95_us".to_owned(),
                json!(revoke.core_cpu.value_at_quantile(0.95)),
            ),
            ("query_update_us".to_owned(), json!(revoke.query_update_us)),
            ("send_recv_us".to_owned(), json!(revoke.send_recv_us)),
            ("apply_us".to_owned(), json!(revoke.apply_us)),
            ("update_rows".to_owned(), json!(revoke.update_rows)),
            (
                "link_rtt_floor_us".to_owned(),
                json!(summary.link_rtt_floor_us),
            ),
            (
                "client_edge_one_way_ms".to_owned(),
                json!(summary.client_edge_one_way_ms),
            ),
            (
                "edge_core_one_way_ms".to_owned(),
                json!(summary.edge_core_one_way_ms),
            ),
        ]);
        emit_json_line("s3_permissions", &JsonValue::Object(fields).to_string());
    }

    let mut forbidden = base_fields("s3_permissions", driver, "forbidden_writes", config);
    forbidden.extend([
        (
            "forbidden_deliveries".to_owned(),
            json!(summary.forbidden_deliveries),
        ),
        (
            "link_rtt_floor_us".to_owned(),
            json!(summary.link_rtt_floor_us),
        ),
        (
            "client_edge_one_way_ms".to_owned(),
            json!(summary.client_edge_one_way_ms),
        ),
        (
            "edge_core_one_way_ms".to_owned(),
            json!(summary.edge_core_one_way_ms),
        ),
    ]);
    emit_json_line("s3_permissions", &JsonValue::Object(forbidden).to_string());

    let mut edge_acceptance = base_fields(
        "s3_permissions",
        driver,
        "edge_mergeable_acceptance",
        config,
    );
    edge_acceptance.extend([
        (
            "acceptance_p50_us".to_owned(),
            json!(
                summary
                    .edge_acceptance
                    .acceptance_latency
                    .value_at_quantile(0.50)
            ),
        ),
        (
            "acceptance_p95_us".to_owned(),
            json!(
                summary
                    .edge_acceptance
                    .acceptance_latency
                    .value_at_quantile(0.95)
            ),
        ),
        ("durability_tier".to_owned(), json!("Edge")),
        (
            "client_edge_one_way_ms".to_owned(),
            json!(summary.client_edge_one_way_ms),
        ),
        (
            "edge_core_one_way_ms".to_owned(),
            json!(summary.edge_core_one_way_ms),
        ),
    ]);
    emit_json_line(
        "s3_permissions",
        &JsonValue::Object(edge_acceptance).to_string(),
    );

    let mut edge_hydration = base_fields(
        "s3_permissions",
        driver,
        "edge_permission_scope_hydration",
        config,
    );
    edge_hydration.extend([
        (
            "scope".to_owned(),
            json!("narrow(policy_shape, writer_claim)"),
        ),
        (
            "hydration_bytes".to_owned(),
            json!(summary.edge_acceptance.hydration_bytes),
        ),
        (
            "hydration_floor_bytes".to_owned(),
            json!(summary.edge_acceptance.hydration_floor_bytes),
        ),
        (
            "hydration_rows".to_owned(),
            json!(summary.edge_acceptance.hydration_rows),
        ),
        (
            "edge_scope_subscription_count_before_drain".to_owned(),
            json!(summary.edge_acceptance.scope_subscriptions_before_drain),
        ),
        (
            "edge_scope_subscription_count_after_drain".to_owned(),
            json!(summary.edge_acceptance.scope_subscriptions_after_drain),
        ),
        (
            "whole_table_scope".to_owned(),
            json!("not hydrated; bench reports the narrow B2 scope only"),
        ),
        (
            "client_edge_one_way_ms".to_owned(),
            json!(summary.client_edge_one_way_ms),
        ),
        (
            "edge_core_one_way_ms".to_owned(),
            json!(summary.edge_core_one_way_ms),
        ),
    ]);
    emit_json_line(
        "s3_permissions",
        &JsonValue::Object(edge_hydration).to_string(),
    );
}

fn emit_db_surface_summary(config: &Config, summary: &DbSurfaceSummary) {
    let mut cold_simple = base_fields("s3_permissions", "db_surface", "cold", config);
    cold_simple.extend([
        ("persona".to_owned(), json!("simple")),
        ("cold_complete_us".to_owned(), json!(summary.cold_simple_us)),
        ("cold_bytes".to_owned(), json!(summary.cold_simple_bytes)),
        ("visible_rows".to_owned(), json!(summary.simple_visible)),
        ("fixture_rows".to_owned(), json!(summary.fixture_rows)),
        (
            "client_edge_one_way_ms".to_owned(),
            json!(summary.client_edge_one_way_ms),
        ),
        (
            "edge_core_one_way_ms".to_owned(),
            json!(summary.edge_core_one_way_ms),
        ),
    ]);
    emit_json_line(
        "s3_permissions",
        &JsonValue::Object(cold_simple).to_string(),
    );

    let mut cold_admin = base_fields("s3_permissions", "db_surface", "cold", config);
    cold_admin.extend([
        ("persona".to_owned(), json!("admin")),
        ("cold_complete_us".to_owned(), json!(summary.cold_admin_us)),
        ("cold_bytes".to_owned(), json!(summary.cold_admin_bytes)),
        ("visible_rows".to_owned(), json!(summary.admin_visible)),
        ("fixture_rows".to_owned(), json!(summary.fixture_rows)),
        (
            "client_edge_one_way_ms".to_owned(),
            json!(summary.client_edge_one_way_ms),
        ),
        (
            "edge_core_one_way_ms".to_owned(),
            json!(summary.edge_core_one_way_ms),
        ),
    ]);
    emit_json_line("s3_permissions", &JsonValue::Object(cold_admin).to_string());

    let mut cold_spy = base_fields("s3_permissions", "db_surface", "cold", config);
    cold_spy.extend([
        ("persona".to_owned(), json!("spy")),
        ("cold_complete_us".to_owned(), json!(summary.cold_spy_us)),
        ("visible_rows".to_owned(), json!(0)),
        ("fixture_rows".to_owned(), json!(summary.fixture_rows)),
        (
            "client_edge_one_way_ms".to_owned(),
            json!(summary.client_edge_one_way_ms),
        ),
        (
            "edge_core_one_way_ms".to_owned(),
            json!(summary.edge_core_one_way_ms),
        ),
    ]);
    emit_json_line("s3_permissions", &JsonValue::Object(cold_spy).to_string());

    let mut grant = base_fields("s3_permissions", "db_surface", "grant", config);
    grant.extend([
        (
            "grant_none_p50_us".to_owned(),
            json!(summary.grant_none_latency.value_at_quantile(0.50)),
        ),
        (
            "grant_none_p95_us".to_owned(),
            json!(summary.grant_none_latency.value_at_quantile(0.95)),
        ),
        (
            "grant_global_p50_us".to_owned(),
            json!(summary.grant_global_latency.value_at_quantile(0.50)),
        ),
        (
            "grant_global_p95_us".to_owned(),
            json!(summary.grant_global_latency.value_at_quantile(0.95)),
        ),
        (
            "client_edge_one_way_ms".to_owned(),
            json!(summary.client_edge_one_way_ms),
        ),
        (
            "edge_core_one_way_ms".to_owned(),
            json!(summary.edge_core_one_way_ms),
        ),
    ]);
    emit_json_line("s3_permissions", &JsonValue::Object(grant).to_string());

    for revoke in &summary.revoke {
        let mut fields = base_fields("s3_permissions", "db_surface", "revocation", config);
        fields.extend([
            ("hidden_rows".to_owned(), json!(revoke.hidden)),
            (
                "disappearance_p50_us".to_owned(),
                json!(revoke.disappearance.value_at_quantile(0.50)),
            ),
            (
                "disappearance_p95_us".to_owned(),
                json!(revoke.disappearance.value_at_quantile(0.95)),
            ),
            ("tick_us".to_owned(), json!(revoke.tick_us)),
            ("update_rows".to_owned(), json!(revoke.update_rows)),
            (
                "client_edge_one_way_ms".to_owned(),
                json!(summary.client_edge_one_way_ms),
            ),
            (
                "edge_core_one_way_ms".to_owned(),
                json!(summary.edge_core_one_way_ms),
            ),
        ]);
        emit_json_line("s3_permissions", &JsonValue::Object(fields).to_string());
    }

    let mut forbidden = base_fields("s3_permissions", "db_surface", "forbidden_writes", config);
    forbidden.extend([(
        "forbidden_deliveries".to_owned(),
        json!(summary.forbidden_deliveries),
    )]);
    forbidden.extend([
        (
            "client_edge_one_way_ms".to_owned(),
            json!(summary.client_edge_one_way_ms),
        ),
        (
            "edge_core_one_way_ms".to_owned(),
            json!(summary.edge_core_one_way_ms),
        ),
    ]);
    emit_json_line("s3_permissions", &JsonValue::Object(forbidden).to_string());
}

fn emit_block_tree_summary(config: &Config, summary: &BlockTreeSummary) {
    let mut fields = metadata_fields(
        "s3_block_tree_permissions",
        "threaded",
        config.seed,
        &config.profile,
    );
    fields.insert("phase".to_owned(), json!("block_tree_variant"));
    fields.insert("pages".to_owned(), json!(summary.pages));
    fields.insert("blocks".to_owned(), json!(summary.blocks));
    fields.insert("max_depth".to_owned(), json!(summary.max_depth));
    fields.insert(
        "simple_visible_rows".to_owned(),
        json!(summary.simple_visible_rows),
    );
    fields.insert(
        "cold_visible_rows".to_owned(),
        json!(summary.cold_visible_rows),
    );
    fields.insert(
        "cold_complete_us".to_owned(),
        json!(summary.cold_complete_us),
    );
    fields.insert("cold_bytes".to_owned(), json!(summary.cold_bytes));
    fields.insert(
        "cold_floor_bytes".to_owned(),
        json!(summary.cold_floor_bytes),
    );
    fields.insert(
        "cold_view_storage_reads".to_owned(),
        storage_read_metrics_json(summary.cold_view_reads),
    );
    fields.insert(
        "grant_subtree_rows".to_owned(),
        json!(summary.grant_subtree_rows),
    );
    fields.insert("grant_core_us".to_owned(), json!(summary.grant_core_us));
    fields.insert(
        "grant_appearance_us".to_owned(),
        json!(summary.grant_appearance_us),
    );
    fields.insert(
        "grant_update_rows".to_owned(),
        json!(summary.grant_update_rows),
    );
    fields.insert(
        "grant_view_storage_reads".to_owned(),
        storage_read_metrics_json(summary.grant_view_reads),
    );
    fields.insert(
        "revoke_subtree_rows".to_owned(),
        json!(summary.revoke_subtree_rows),
    );
    fields.insert("revoke_core_us".to_owned(), json!(summary.revoke_core_us));
    fields.insert(
        "revoke_disappearance_us".to_owned(),
        json!(summary.revoke_disappearance_us),
    );
    fields.insert(
        "revoke_update_rows".to_owned(),
        json!(summary.revoke_update_rows),
    );
    fields.insert(
        "revoke_view_storage_reads".to_owned(),
        storage_read_metrics_json(summary.revoke_view_reads),
    );
    fields.insert("burst_commits".to_owned(), json!(summary.burst_commits));
    fields.insert(
        "burst_core_p95_us".to_owned(),
        json!(summary.burst_core_p95_us),
    );
    fields.insert(
        "policy_model".to_owned(),
        json!("v0 materializes inherited block visibility into visibleClaim/locked rows; grant/revoke rewrites exactly the affected subtree so core cost is reported against affected rows, not optimized away"),
    );
    fields.insert(
        "headline".to_owned(),
        json!("inner-block grant/revoke reflows exactly its subtree"),
    );
    emit_json_line(
        "s3_block_tree_permissions",
        &JsonValue::Object(fields).to_string(),
    );
}

fn emit_block_tree_cold_headline(config: &Config, summary: &BlockTreeHeadlineSummary) {
    let target_us = 1_000_000_u64;
    let mut fields = metadata_fields(
        "s3_block_tree_cold_headline",
        "threaded",
        config.seed,
        &config.profile,
    );
    fields.insert("phase".to_owned(), json!("joint_cold_hydration_headline"));
    fields.insert("pages".to_owned(), json!(summary.pages));
    fields.insert("blocks".to_owned(), json!(summary.blocks));
    fields.insert(
        "visible_rows_materialized".to_owned(),
        json!(summary.visible_rows),
    );
    fields.insert(
        "end_to_end_cold_us".to_owned(),
        json!(summary.end_to_end_cold_us),
    );
    fields.insert(
        "server_eval_ship_us".to_owned(),
        json!(summary.server_eval_ship_us),
    );
    fields.insert(
        "server_rehydrate_query_us".to_owned(),
        json!(summary.server_rehydrate_query_us),
    );
    fields.insert(
        "server_byte_accounting_us".to_owned(),
        json!(summary.server_byte_accounting_us),
    );
    fields.insert("server_send_us".to_owned(), json!(summary.server_send_us));
    fields.insert(
        "client_ingest_materialize_us".to_owned(),
        json!(summary.client_ingest_materialize_us),
    );
    fields.insert("client_recv_us".to_owned(), json!(summary.client_recv_us));
    fields.insert(
        "client_apply_sync_us".to_owned(),
        json!(summary.client_apply_sync_us),
    );
    fields.insert(
        "client_materialize_read_us".to_owned(),
        json!(summary.client_materialize_read_us),
    );
    fields.insert(
        "fixture_population_us".to_owned(),
        json!(summary.fixture_population_us),
    );
    fields.insert("cold_bytes".to_owned(), json!(summary.cold_bytes));
    fields.insert(
        "cold_floor_bytes".to_owned(),
        json!(summary.cold_floor_bytes),
    );
    fields.insert(
        "client_storage_writes".to_owned(),
        storage_write_metrics_json(summary.client_storage_writes),
    );
    fields.insert(
        "server_view_storage_reads".to_owned(),
        storage_read_metrics_json(summary.server_view_reads),
    );
    fields.insert(
        "client_materialize_storage_reads".to_owned(),
        storage_read_metrics_json(summary.client_materialize_reads),
    );
    fields.insert("target_us".to_owned(), json!(target_us));
    fields.insert(
        "target_description".to_owned(),
        json!("cold client sync + materialize approximately 20k recursively permissioned visible rows under 1s"),
    );
    fields.insert(
        "target_met".to_owned(),
        json!(summary.end_to_end_cold_us < target_us),
    );
    fields.insert(
        "policy_model".to_owned(),
        json!(
            "same v0 materialized inherited block visibility fixture as s3_block_tree_permissions"
        ),
    );
    emit_json_line(
        "s3_block_tree_cold_headline",
        &JsonValue::Object(fields).to_string(),
    );
}

fn storage_write_metrics_json(metrics: StorageWriteMetrics) -> JsonValue {
    json!({
        "total": storage_write_bucket_json(metrics.total),
        "history_rows": storage_write_bucket_json(metrics.history_rows),
        "history_indexes": storage_write_bucket_json(metrics.history_indexes),
        "global_current_rows": storage_write_bucket_json(metrics.global_current_rows),
        "global_current_indexes": storage_write_bucket_json(metrics.global_current_indexes),
        "register_global_current_rows": storage_write_bucket_json(metrics.register_global_current_rows),
        "global_changes_rows": storage_write_bucket_json(metrics.global_changes_rows),
        "global_changes_indexes": storage_write_bucket_json(metrics.global_changes_indexes),
        "transactions_rows": storage_write_bucket_json(metrics.transactions_rows),
        "transactions_indexes": storage_write_bucket_json(metrics.transactions_indexes),
        "other": storage_write_bucket_json(metrics.other),
    })
}

fn storage_read_metrics_json(metrics: StorageReadMetrics) -> JsonValue {
    json!({
        "total": storage_read_bucket_json(metrics.total),
        "history_rows": storage_read_bucket_json(metrics.history_rows),
        "history_indexes": storage_read_bucket_json(metrics.history_indexes),
        "global_current_rows": storage_read_bucket_json(metrics.global_current_rows),
        "global_current_indexes": storage_read_bucket_json(metrics.global_current_indexes),
        "register_global_current_rows": storage_read_bucket_json(metrics.register_global_current_rows),
        "global_changes_rows": storage_read_bucket_json(metrics.global_changes_rows),
        "global_changes_indexes": storage_read_bucket_json(metrics.global_changes_indexes),
        "transactions_rows": storage_read_bucket_json(metrics.transactions_rows),
        "transactions_indexes": storage_read_bucket_json(metrics.transactions_indexes),
        "other": storage_read_bucket_json(metrics.other),
    })
}

fn storage_read_bucket_json(bucket: StorageReadBucket) -> JsonValue {
    json!({
        "reads": bucket.reads,
        "ranges": bucket.ranges,
    })
}

fn storage_write_bucket_json(bucket: StorageWriteBucket) -> JsonValue {
    json!({
        "count": bucket.count,
        "bytes": bucket.bytes,
    })
}

fn emit_headline_progress(
    config: &Config,
    stage: &str,
    blocks: usize,
    visible_rows: usize,
    elapsed_us: u64,
) {
    if !config.headline_progress {
        return;
    }
    let mut fields = metadata_fields(
        "s3_block_tree_cold_headline_progress",
        "threaded",
        config.seed,
        &config.profile,
    );
    fields.insert("phase".to_owned(), json!("headline_progress"));
    fields.insert("stage".to_owned(), json!(stage));
    fields.insert("blocks".to_owned(), json!(blocks));
    fields.insert("visible_rows".to_owned(), json!(visible_rows));
    fields.insert("elapsed_us".to_owned(), json!(elapsed_us));
    emit_json_line(
        "s3_block_tree_cold_headline_progress",
        &JsonValue::Object(fields).to_string(),
    );
}

fn base_fields(
    scenario: &str,
    driver: &str,
    phase: &str,
    config: &Config,
) -> serde_json::Map<String, JsonValue> {
    let mut fields = metadata_fields(scenario, driver, config.seed, &config.profile);
    fields.insert("phase".to_owned(), json!(phase));
    fields.insert("orgs".to_owned(), json!(config.orgs));
    fields.insert("teams_per_org".to_owned(), json!(config.teams_per_org));
    fields.insert(
        "resources_per_org".to_owned(),
        json!(config.resources_per_org),
    );
    fields.insert(
        "access_edges_per_resource".to_owned(),
        json!(config.access_edges_per_resource),
    );
    fields
}

fn row(idx: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[8..].copy_from_slice(&(idx as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn resource_row(idx: usize) -> RowUuid {
    row(100_000 + idx)
}

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn node_seed(node_uuid: NodeUuid) -> u64 {
    u64::from_le_bytes(
        node_uuid.as_bytes()[..8]
            .try_into()
            .expect("node seed bytes"),
    )
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

fn env_bool(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

fn env_usize_list(name: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|part| part.trim().parse().ok())
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| default.to_vec())
}

fn percentile(values: &mut [u64], pct: u64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort();
    let idx = (((values.len() - 1) as u64 * pct) / 100) as usize;
    values[idx]
}
