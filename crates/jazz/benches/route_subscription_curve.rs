//! Current-core same-shape, distinct-binding subscription scaling receipt.
//!
//! Run each scale in a fresh process so RSS and allocator retention remain
//! attributable:
//!
//! ```text
//! JAZZ_ROUTE_CURVE_ROUTES=100 cargo bench --profile perf -p jazz \
//!   --features testing --bench route_subscription_curve --quiet
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::time::{Duration, Instant};

mod schema_fixture;

use jazz::db::{
    Db, DbConfig, DbIdentity, LocalUpdates, Propagation, ReadOpts, SeededRowIdSource,
    SubscriptionEvent, SubscriptionStream, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, param};
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use serde::Serialize;

const DOCUMENTS: &str = "route_curve_documents";
const TEAMS: usize = 1_001;
const HOT_TEAM_DOCUMENTS: usize = 1_000;
const PAGE_SIZE: usize = 100;
const MAX_ROUTES: usize = 1_000;
const WRITER: AuthorSubject = AuthorSubject::SYSTEM;

type BenchDb = Db<MemoryStorage>;

#[derive(Serialize)]
struct Receipt {
    scenario: &'static str,
    routes: usize,
    teams: usize,
    documents: usize,
    page_size: usize,
    seed_us: u64,
    hydration_total_us: u64,
    prepare: LatencyStats,
    subscribe: LatencyStats,
    initial_rows: usize,
    runtime: RuntimeReceipt,
    retained: RetainedReceipt,
    rss_kib_after_seed: Option<u64>,
    rss_kib_after_hydration: Option<u64>,
    peak_rss_kib: Option<u64>,
    matching_write_us: u64,
    unrelated_write_us: u64,
    below_boundary_write_us: u64,
    exact_initial: bool,
    exact_matching_delta: bool,
    unrelated_quiet: bool,
    below_boundary_quiet: bool,
    ok: bool,
    tooling_friction: &'static str,
}

#[derive(Serialize)]
struct LatencyStats {
    samples: usize,
    total_us: u64,
    min_us: u64,
    p50_us: u64,
    p95_us: u64,
    max_us: u64,
}

#[derive(Serialize)]
struct RuntimeReceipt {
    graph_nodes: usize,
    active_subscriptions: usize,
    active_prepared_shapes: usize,
    active_shape_params: usize,
    arrangement_count: usize,
    arrangement_rows: usize,
    arrangement_encoded_bytes: usize,
    logical_nodes_requested: u64,
    deduped_graph_nodes: usize,
}

#[derive(Serialize)]
struct RetainedReceipt {
    subscriptions: usize,
    root_rows: usize,
    result_rows: usize,
    version_identities: usize,
    replacement_entries: usize,
    maintained_heap_bytes: usize,
    result_weights_bytes: usize,
    result_payloads_bytes: usize,
    versions_bytes: usize,
    replacements_bytes: usize,
    terminal_schemas_bytes: usize,
    control_state_bytes: usize,
    maintained_and_control_heap_bytes: usize,
    snapshot_bytes: usize,
    reset_frame_bytes: usize,
}

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let routes = env_usize("JAZZ_ROUTE_CURVE_ROUTES", 100);
    assert!(
        (1..=MAX_ROUTES).contains(&routes),
        "JAZZ_ROUTE_CURVE_ROUTES must be between 1 and {MAX_ROUTES}"
    );
    let receipt = run(routes);
    println!(
        "{}",
        serde_json::to_string(&receipt).expect("serialize route curve receipt")
    );
    assert!(receipt.ok, "route curve correctness gate failed");
}

#[allow(dead_code)]
pub(crate) fn correctness_smoke() {
    assert!(run(1).ok, "route curve correctness gate failed");
}

fn run(routes: usize) -> Receipt {
    let db = open_db(routes as u64);
    let seed_started = Instant::now();
    seed_fixture(&db);
    let seed_us = micros(seed_started.elapsed());
    let rss_kib_after_seed = proc_status_kib("VmRSS:");

    let query = Query::from(DOCUMENTS)
        .filter(eq(col("team"), param("team")))
        .order_by("updated_at", OrderDirection::Desc)
        .limit(PAGE_SIZE);
    let mut streams = Vec::with_capacity(routes);
    let mut prepare_samples = Vec::with_capacity(routes);
    let mut subscribe_samples = Vec::with_capacity(routes);
    let mut initial_rows = 0;
    let mut exact_initial = true;
    let mut shape_id = None;
    let hydration_started = Instant::now();

    for team in 0..routes {
        let prepare_started = Instant::now();
        let prepared = db
            .prepare_query_bound(
                &query,
                BTreeMap::from([("team".to_owned(), Value::Uuid(team_row(team).0))]),
            )
            .expect("prepare route binding");
        prepare_samples.push(prepare_started.elapsed());
        if let Some(expected_shape) = shape_id {
            assert_eq!(prepared.shape().shape_id(), expected_shape);
        } else {
            shape_id = Some(prepared.shape().shape_id());
        }

        let subscribe_started = Instant::now();
        let mut stream = block_on(db.subscribe(&prepared, local_opts())).expect("subscribe route");
        subscribe_samples.push(subscribe_started.elapsed());
        let actual = take_initial_reset(&mut stream);
        let expected = expected_initial_rows(team);
        initial_rows += actual.len();
        exact_initial &= actual == expected;
        streams.push(stream);
    }

    let hydration_total_us = micros(hydration_started.elapsed());
    let runtime = runtime_receipt(&db);
    let retained = retained_receipt(&db);
    let rss_kib_after_hydration = proc_status_kib("VmRSS:");

    let matching_row = document_row(0, HOT_TEAM_DOCUMENTS + 1);
    let matching_started = Instant::now();
    insert_document(&db, matching_row, 0, HOT_TEAM_DOCUMENTS as u64 + 1);
    let matching_write_us = micros(matching_started.elapsed());
    let matching_events = drain_events(&mut streams);
    let exact_matching_delta = matching_events.first().is_some_and(|delta| {
        delta.events == 1
            && delta.added == BTreeSet::from([matching_row])
            && delta.removed == BTreeSet::from([document_row(0, HOT_TEAM_DOCUMENTS - PAGE_SIZE)])
            && delta.updated.is_empty()
            && !delta.reset
    }) && matching_events.iter().skip(1).all(Delta::is_quiet);

    let unrelated_started = Instant::now();
    insert_document(&db, document_row(MAX_ROUTES, 1), MAX_ROUTES, 1);
    let unrelated_write_us = micros(unrelated_started.elapsed());
    let unrelated_quiet = drain_events(&mut streams).iter().all(Delta::is_quiet);

    let below_boundary_started = Instant::now();
    insert_document(&db, document_row(0, HOT_TEAM_DOCUMENTS + 2), 0, 0);
    let below_boundary_write_us = micros(below_boundary_started.elapsed());
    let below_boundary_quiet = drain_events(&mut streams).iter().all(Delta::is_quiet);

    let ok = exact_initial
        && exact_matching_delta
        && unrelated_quiet
        && below_boundary_quiet
        && runtime.active_subscriptions == routes
        && retained.subscriptions == routes;

    Receipt {
        scenario: "current_core_route_subscription_curve",
        routes,
        teams: TEAMS,
        documents: HOT_TEAM_DOCUMENTS + TEAMS - 1,
        page_size: PAGE_SIZE,
        seed_us,
        hydration_total_us,
        prepare: latency_stats(prepare_samples),
        subscribe: latency_stats(subscribe_samples),
        initial_rows,
        runtime,
        retained,
        rss_kib_after_seed,
        rss_kib_after_hydration,
        peak_rss_kib: proc_status_kib("VmHWM:"),
        matching_write_us,
        unrelated_write_us,
        below_boundary_write_us,
        exact_initial,
        exact_matching_delta,
        unrelated_quiet,
        below_boundary_quiet,
        ok,
        tooling_friction: "run each route count in a fresh process on a quiet host",
    }
}

#[derive(Default)]
struct Delta {
    events: usize,
    reset: bool,
    added: BTreeSet<RowUuid>,
    updated: BTreeSet<RowUuid>,
    removed: BTreeSet<RowUuid>,
}

impl Delta {
    fn is_quiet(&self) -> bool {
        self.events == 0
    }
}

fn drain_events(streams: &mut [SubscriptionStream]) -> Vec<Delta> {
    streams
        .iter_mut()
        .map(|stream| {
            let mut delta = Delta::default();
            while let Some(event) = stream.try_next_event() {
                delta.events += 1;
                match event {
                    SubscriptionEvent::Delta {
                        reset,
                        added,
                        updated,
                        removed,
                        ..
                    } => {
                        delta.reset |= reset;
                        delta
                            .added
                            .extend(added.into_iter().map(|row| row.row_uuid()));
                        delta
                            .updated
                            .extend(updated.into_iter().map(|row| row.row_uuid()));
                        delta
                            .removed
                            .extend(removed.into_iter().map(|row| row.row_uuid));
                    }
                    SubscriptionEvent::Rejected { reason } => {
                        panic!("route subscription rejected: {reason:?}")
                    }
                    SubscriptionEvent::Closed => panic!("route subscription closed"),
                }
            }
            delta
        })
        .collect()
}

fn take_initial_reset(stream: &mut SubscriptionStream) -> BTreeSet<RowUuid> {
    match stream
        .try_next_event()
        .expect("route subscription did not emit an initial reset")
    {
        SubscriptionEvent::Delta {
            reset: true,
            added,
            updated,
            removed,
            ..
        } => {
            assert!(updated.is_empty());
            assert!(removed.is_empty());
            added.into_iter().map(|row| row.row_uuid()).collect()
        }
        other => panic!("expected initial reset, got {other:?}"),
    }
}

fn expected_initial_rows(team: usize) -> BTreeSet<RowUuid> {
    if team == 0 {
        (HOT_TEAM_DOCUMENTS - PAGE_SIZE..HOT_TEAM_DOCUMENTS)
            .map(|ordinal| document_row(team, ordinal))
            .collect()
    } else {
        BTreeSet::from([document_row(team, 0)])
    }
}

fn open_db(seed: u64) -> BenchDb {
    let schema = schema_fixture::compile(
        SchemaBuilder::new().table(
            TableSchemaBuilder::new(DOCUMENTS)
                .column("team", ColumnType::Uuid)
                .column("updated_at", ColumnType::Timestamp)
                .column("title", ColumnType::Text),
        ),
    );
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&family_refs).expect("valid memory storage families"),
            DbIdentity {
                node: NodeUuid::from_bytes((0x7600_u128 + seed as u128).to_be_bytes()),
                author: WRITER,
            },
        )
        .with_id_source(SeededRowIdSource::new(0x7600 + seed)),
    ))
    .expect("open route curve db")
}

fn seed_fixture(db: &BenchDb) {
    for ordinal in 0..HOT_TEAM_DOCUMENTS {
        insert_document(db, document_row(0, ordinal), 0, ordinal as u64);
    }
    for team in 1..TEAMS {
        insert_document(db, document_row(team, 0), team, team as u64);
    }
}

fn insert_document(db: &BenchDb, row: RowUuid, team: usize, updated_at: u64) {
    block_on(db.insert(
        DOCUMENTS,
        BTreeMap::from([
            ("team".to_owned(), Value::Uuid(team_row(team).0)),
            ("updated_at".to_owned(), Value::U64(updated_at)),
            (
                "title".to_owned(),
                Value::String(format!("route {team} document {updated_at}")),
            ),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .expect("insert route curve document");
}

fn team_row(team: usize) -> RowUuid {
    tagged_row(0x7601, team as u64)
}

fn document_row(team: usize, ordinal: usize) -> RowUuid {
    tagged_row(0x7602 + team as u64, ordinal as u64)
}

fn tagged_row(namespace: u64, value: u64) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&namespace.to_be_bytes());
    bytes[8..].copy_from_slice(&value.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn local_opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Local,
        local_updates: LocalUpdates::Immediate,
        propagation: Propagation::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

fn runtime_receipt(db: &BenchDb) -> RuntimeReceipt {
    let stats = db.runtime_stats_for_test();
    RuntimeReceipt {
        graph_nodes: stats.graph_nodes,
        active_subscriptions: stats.active_subscriptions,
        active_prepared_shapes: stats.active_prepared_shapes,
        active_shape_params: stats.active_shape_params,
        arrangement_count: stats.arrangement_count,
        arrangement_rows: stats.arrangement_rows,
        arrangement_encoded_bytes: stats.arrangement_encoded_bytes,
        logical_nodes_requested: stats.logical_nodes_requested,
        deduped_graph_nodes: stats.deduped_graph_nodes,
    }
}

fn retained_receipt(db: &BenchDb) -> RetainedReceipt {
    let receipts = db.maintained_subscription_size_receipts_for_test();
    RetainedReceipt {
        subscriptions: receipts.len(),
        root_rows: receipts.iter().map(|receipt| receipt.root_rows).sum(),
        result_rows: receipts
            .iter()
            .map(|receipt| receipt.footprint.result_rows)
            .sum(),
        version_identities: receipts
            .iter()
            .map(|receipt| receipt.footprint.version_identities)
            .sum(),
        replacement_entries: receipts
            .iter()
            .map(|receipt| receipt.footprint.replacement_entries)
            .sum(),
        maintained_heap_bytes: receipts
            .iter()
            .map(|receipt| receipt.footprint.maintained_heap_bytes)
            .sum(),
        result_weights_bytes: receipts
            .iter()
            .map(|receipt| receipt.footprint.result_weights_bytes)
            .sum(),
        result_payloads_bytes: receipts
            .iter()
            .map(|receipt| receipt.footprint.result_payloads_bytes)
            .sum(),
        versions_bytes: receipts
            .iter()
            .map(|receipt| receipt.footprint.versions_bytes)
            .sum(),
        replacements_bytes: receipts
            .iter()
            .map(|receipt| receipt.footprint.replacements_bytes)
            .sum(),
        terminal_schemas_bytes: receipts
            .iter()
            .map(|receipt| receipt.footprint.terminal_schemas_bytes)
            .sum(),
        control_state_bytes: receipts
            .iter()
            .map(|receipt| receipt.footprint.control_state_bytes)
            .sum(),
        maintained_and_control_heap_bytes: receipts
            .iter()
            .map(|receipt| receipt.footprint.total_heap_bytes)
            .sum(),
        snapshot_bytes: receipts.iter().map(|receipt| receipt.snapshot_bytes).sum(),
        reset_frame_bytes: receipts
            .iter()
            .map(|receipt| receipt.reset_frame_bytes)
            .sum(),
    }
}

fn latency_stats(samples: Vec<Duration>) -> LatencyStats {
    let mut samples = samples.into_iter().map(micros).collect::<Vec<_>>();
    samples.sort_unstable();
    let percentile = |numerator: usize, denominator: usize| {
        let index = ((samples.len() - 1) * numerator).div_ceil(denominator);
        samples[index]
    };
    LatencyStats {
        samples: samples.len(),
        total_us: samples.iter().sum(),
        min_us: samples[0],
        p50_us: percentile(50, 100),
        p95_us: percentile(95, 100),
        max_us: *samples.last().expect("non-empty latency samples"),
    }
}

fn micros(duration: Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .unwrap_or_else(|_| panic!("{name} must be an unsigned integer"))
        })
        .unwrap_or(default)
}

fn proc_status_kib(label: &str) -> Option<u64> {
    fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find(|line| line.starts_with(label))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}
