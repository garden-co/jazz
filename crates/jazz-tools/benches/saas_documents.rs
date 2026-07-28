//! SaaS document-list benchmark using only the public `jazz::db::Db` facade.
//!
//! The default fixture is intentionally the arithmetic requested by the benchmark:
//! 500,000 documents, 5,000 teams, one 30,000-document hot team, and one
//! 100-document cold team. Those numbers cannot also give every team at least
//! 100 documents: the requested minimum would require 529,900 documents. The
//! emitted JSON reports the resulting skew explicitly.
//! Set `JAZZ_SAAS_DOCUMENTS=529900` to keep all non-hot teams at 100 rows.
//!
//! Suggested invocations after registering this file as a `harness = false` bench:
//!
//! ```text
//! cargo bench --profile perf -p jazz-tools --bench saas_documents
//! JAZZ_SAAS_SEED_MODE=global cargo bench --profile perf -p jazz-tools --bench saas_documents
//! ```
//!
//! Global mode is deliberately opt-in because the only public settled bootstrap
//! API is per-row. Local mode batches rows in mergeable transactions of at most
//! 2,048 versions, but reads the unindexed ahead-current layer. Global mode uses
//! `seed_settled_mergeable_for_bootstrap` per row and can exercise the explicit
//! global-current team indexes.

use std::collections::{BTreeMap, BTreeSet};
use std::hint::black_box;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::{Duration, Instant};

use jazz::db::{
    Db, DbConfig, DbIdentity, LocalUpdates, Propagation, ReadOpts, RowCells, SeededRowIdSource,
    SubscriptionEvent, SubscriptionStream, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, claim, col, eq, lit, param};
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::DurabilityTier;
use serde::Serialize;
use serde_json::{Value as JsonValue, json};

type BenchDb = Db<MemoryStorage>;

const DEFAULT_DOCUMENTS: usize = 500_000;
const DEFAULT_TEAMS: usize = 5_000;
const DEFAULT_MEMBERS_PER_TEAM: usize = 10;
const DEFAULT_QUERY_ITERS: usize = 50;
const DEFAULT_HOT_DOCUMENTS: usize = 30_000;
const DEFAULT_COLD_DOCUMENTS: usize = 100;
const REQUESTED_MIN_DOCUMENTS_PER_TEAM: usize = 100;
const REQUESTED_MAX_DOCUMENTS_PER_TEAM: usize = 30_000;

// Half of MAX_COMMIT_UNIT_VERSIONS, leaving ample encoded-byte headroom for
// the small benchmark rows as well as count headroom.
const MAX_LOCAL_SEED_BATCH: usize = 2_048;

const READER: AuthorId = AuthorId(uuid::uuid!("00000000-0000-0000-0000-0000000000a1"));
const OUTSIDER: AuthorId = AuthorId(uuid::uuid!("00000000-0000-0000-0000-0000000000ee"));
const SYSTEM_NODE: NodeUuid = NodeUuid(uuid::uuid!("51000000-0000-0000-0000-000000000001"));

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum SeedMode {
    Local,
    Global,
}

impl SeedMode {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "local" => Ok(Self::Local),
            "global" => Ok(Self::Global),
            other => Err(format!(
                "JAZZ_SAAS_SEED_MODE must be local or global, got {other:?}"
            )),
        }
    }

    fn read_opts(self) -> ReadOpts {
        ReadOpts {
            tier: match self {
                Self::Local => DurabilityTier::Local,
                Self::Global => DurabilityTier::Global,
            },
            local_updates: match self {
                Self::Local => LocalUpdates::Immediate,
                Self::Global => LocalUpdates::Deferred,
            },
            propagation: Propagation::LocalOnly,
            include_deleted: false,
            ..ReadOpts::default()
        }
    }
}

#[derive(Debug, Serialize)]
struct Config {
    documents: usize,
    teams: usize,
    members_per_team: usize,
    query_iterations: usize,
    hot_documents: usize,
    cold_documents: usize,
    local_seed_batch: usize,
    seed_mode: SeedMode,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let documents = env_usize("JAZZ_SAAS_DOCUMENTS", DEFAULT_DOCUMENTS)?;
        let teams = env_usize("JAZZ_SAAS_TEAMS", DEFAULT_TEAMS)?;
        let members_per_team = env_usize("JAZZ_SAAS_MEMBERS_PER_TEAM", DEFAULT_MEMBERS_PER_TEAM)?;
        let query_iterations = env_usize("JAZZ_SAAS_QUERY_ITERS", DEFAULT_QUERY_ITERS)?;
        let hot_documents = env_usize("JAZZ_SAAS_HOT_DOCUMENTS", DEFAULT_HOT_DOCUMENTS)?;
        let cold_documents = env_usize("JAZZ_SAAS_COLD_DOCUMENTS", DEFAULT_COLD_DOCUMENTS)?;
        let requested_batch = env_usize("JAZZ_SAAS_SEED_BATCH", MAX_LOCAL_SEED_BATCH)?;
        let seed_mode = SeedMode::parse(
            &std::env::var("JAZZ_SAAS_SEED_MODE").unwrap_or_else(|_| "local".to_owned()),
        )?;

        if teams < 3 {
            return Err("JAZZ_SAAS_TEAMS must be at least 3".to_owned());
        }
        if documents < hot_documents.saturating_add(cold_documents) {
            return Err(format!(
                "JAZZ_SAAS_DOCUMENTS ({documents}) must be at least hot + cold ({hot_documents} + {cold_documents})"
            ));
        }
        if members_per_team == 0 {
            return Err("JAZZ_SAAS_MEMBERS_PER_TEAM must be at least 1".to_owned());
        }
        if query_iterations == 0 {
            return Err("JAZZ_SAAS_QUERY_ITERS must be at least 1".to_owned());
        }
        if requested_batch == 0 {
            return Err("JAZZ_SAAS_SEED_BATCH must be at least 1".to_owned());
        }
        if requested_batch > MAX_LOCAL_SEED_BATCH {
            return Err(format!(
                "JAZZ_SAAS_SEED_BATCH must not exceed {MAX_LOCAL_SEED_BATCH}"
            ));
        }
        teams
            .checked_mul(members_per_team)
            .ok_or_else(|| "teams * members_per_team overflows usize".to_owned())?;
        u64::try_from(documents)
            .map_err(|_| "document count does not fit in u64 updated_at values".to_owned())?;

        Ok(Self {
            documents,
            teams,
            members_per_team,
            query_iterations,
            hot_documents,
            cold_documents,
            local_seed_batch: requested_batch,
            seed_mode,
        })
    }
}

#[derive(Debug)]
struct Fixture {
    team_document_counts: Vec<usize>,
    team_document_starts: Vec<usize>,
}

impl Fixture {
    fn team(&self, team_index: usize) -> RowUuid {
        row_uuid(0x51, team_index)
    }

    fn count(&self, team_index: usize) -> usize {
        self.team_document_counts[team_index]
    }

    fn start(&self, team_index: usize) -> usize {
        self.team_document_starts[team_index]
    }
}

#[derive(Debug, Serialize)]
struct DistributionReport {
    total_documents: usize,
    total_teams: usize,
    hot_team_index: usize,
    hot_team_documents: usize,
    cold_team_index: usize,
    cold_team_documents: usize,
    requested_min_documents_per_team: usize,
    requested_max_documents_per_team: usize,
    actual_min_documents_per_team: usize,
    actual_max_documents_per_team: usize,
    teams_below_requested_min: usize,
    teams_above_requested_max: usize,
    minimum_total_for_requested_hot_and_bounds: usize,
    arithmetic_shortfall: usize,
    other_team_mean_documents: f64,
    exact_total_preserved: bool,
}

#[derive(Debug, Serialize)]
struct PhaseReport {
    rows: usize,
    duration_ms: f64,
    rows_per_second: f64,
    write_api: &'static str,
}

impl PhaseReport {
    fn new(rows: usize, elapsed: Duration, write_api: &'static str) -> Self {
        let seconds = elapsed.as_secs_f64();
        Self {
            rows,
            duration_ms: millis(elapsed),
            rows_per_second: if seconds == 0.0 {
                f64::INFINITY
            } else {
                rows as f64 / seconds
            },
            write_api,
        }
    }
}

#[derive(Debug, Serialize)]
struct SeedReport {
    open_ms: f64,
    teams: PhaseReport,
    memberships: PhaseReport,
    documents: PhaseReport,
    total_ms: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueryStyle {
    Literal,
    Parameterized,
}

impl QueryStyle {
    fn label(self) -> &'static str {
        match self {
            Self::Literal => "literal",
            Self::Parameterized => "parameterized",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum FilterVariant {
    TeamOnly,
    ActiveUnarchived,
}

impl FilterVariant {
    fn label(self) -> &'static str {
        match self {
            Self::TeamOnly => "team_only",
            Self::ActiveUnarchived => "status_active_and_not_archived",
        }
    }

    fn matches(self, local_document_index: usize) -> bool {
        match self {
            Self::TeamOnly => true,
            Self::ActiveUnarchived => {
                status(local_document_index) == "active" && !archived(local_document_index)
            }
        }
    }
}

#[derive(Debug, Serialize)]
struct LatencyStats {
    iterations: usize,
    total_ms: f64,
    mean_us: f64,
    min_us: f64,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    max_us: f64,
    reads_per_second: f64,
}

#[derive(Debug, Serialize)]
struct ScenarioReport {
    name: String,
    team_profile: &'static str,
    team_index: usize,
    documents_in_team: usize,
    query_style: &'static str,
    filter_variant: &'static str,
    authorization_api: &'static str,
    order_by: &'static str,
    limit: usize,
    expected_rows: usize,
    prepare_us: f64,
    first_binding_read_us: f64,
    repeated_reads: LatencyStats,
    first_to_repeated_mean_ratio: f64,
    correctness: &'static str,
}

#[derive(Debug, Serialize)]
struct BenchmarkOutput {
    benchmark: &'static str,
    config: Config,
    storage: &'static str,
    index_scope: &'static str,
    distribution: DistributionReport,
    seeding: SeedReport,
    scenarios: Vec<ScenarioReport>,
    authorization_canary: JsonValue,
    parameterized_cell_access_canary: JsonValue,
    simultaneous_parameter_bindings_canary: JsonValue,
    observed_ratios: JsonValue,
    notes: Vec<&'static str>,
    tooling_friction: &'static str,
}

fn main() {
    match run() {
        Ok(output) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output).expect("serialize SaaS benchmark output")
            );
        }
        Err(error) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "benchmark": "saas_documents",
                    "ok": false,
                    "error": error,
                }))
                .expect("serialize SaaS benchmark error")
            );
            std::process::exit(1);
        }
    }
}

fn run() -> Result<BenchmarkOutput, String> {
    let config = Config::from_env()?;
    let (fixture, distribution) = build_distribution(&config)?;
    let schema = saas_schema();
    let document_table = schema
        .tables
        .iter()
        .find(|table| table.name == "documents")
        .expect("SaaS schema has documents")
        .clone();

    let total_started = Instant::now();
    let open_started = Instant::now();
    let db = open_db(schema, config.seed_mode)?;
    let open_elapsed = open_started.elapsed();

    eprintln!(
        "seeding {} teams, {} memberships, and {} documents in {:?} mode",
        config.teams,
        config.teams * config.members_per_team,
        config.documents,
        config.seed_mode
    );

    let teams = seed_rows(
        &db,
        config.seed_mode,
        config.local_seed_batch,
        "teams",
        config.teams,
        |index| {
            (
                "teams",
                row_uuid(0x51, index),
                BTreeMap::from([("name".to_owned(), Value::String(format!("Team {index}")))]),
            )
        },
    )?;

    let membership_count = config
        .teams
        .checked_mul(config.members_per_team)
        .ok_or_else(|| "membership count overflowed".to_owned())?;
    let members_per_team = config.members_per_team;
    let memberships = seed_rows(
        &db,
        config.seed_mode,
        config.local_seed_batch,
        "team_memberships",
        membership_count,
        |index| {
            let team_index = index / members_per_team;
            let member_index = index % members_per_team;
            (
                "team_memberships",
                row_uuid(0x52, index),
                membership_cells(
                    row_uuid(0x51, team_index),
                    member_identity(team_index, member_index, index),
                    member_index,
                ),
            )
        },
    )?;

    let counts = fixture.team_document_counts.clone();
    let mut team_index = 0usize;
    let mut local_index = 0usize;
    let documents = seed_rows(
        &db,
        config.seed_mode,
        config.local_seed_batch,
        "documents",
        config.documents,
        |global_index| {
            while team_index < counts.len() && local_index == counts[team_index] {
                team_index += 1;
                local_index = 0;
            }
            let cells = document_cells(row_uuid(0x51, team_index), global_index, local_index);
            let row = row_uuid(0x53, global_index);
            local_index += 1;
            ("documents", row, cells)
        },
    )?;

    let seeding = SeedReport {
        open_ms: millis(open_elapsed),
        teams,
        memberships,
        documents,
        total_ms: millis(total_started.elapsed()),
    };

    let read_opts = config.seed_mode.read_opts();
    let mut scenarios = Vec::with_capacity(8);
    for (team_profile, selected_team) in [("hot", 0usize), ("cold", 1usize)] {
        for variant in [FilterVariant::TeamOnly, FilterVariant::ActiveUnarchived] {
            for style in [QueryStyle::Literal, QueryStyle::Parameterized] {
                eprintln!(
                    "measuring {team_profile} / {} / {}",
                    variant.label(),
                    style.label()
                );
                scenarios.push(run_scenario(
                    &db,
                    &fixture,
                    &read_opts,
                    config.query_iterations,
                    team_profile,
                    selected_team,
                    variant,
                    style,
                )?);
            }
        }
    }

    let authorization_canary = safe_canary(|| authorization_canary(&db, &fixture, &read_opts));
    let parameterized_cell_access_canary = safe_canary(|| {
        parameterized_cell_access_canary(&db, &fixture, &document_table, &read_opts)
    });
    let simultaneous_parameter_bindings_canary =
        safe_canary(|| simultaneous_bindings_canary(&db, &fixture, config.seed_mode, &read_opts));
    let observed_ratios = observed_ratios(&scenarios);

    let (index_scope, notes) = match config.seed_mode {
        SeedMode::Local => (
            "declared team indexes are global-current only; local rows are read from ahead-current",
            vec![
                "Local mode is the fast-to-seed full-scan baseline, not an index benchmark.",
                "first_binding_read measures first use of that shape/binding after the shared fixture was seeded; it is not a process/storage cold start.",
                "The document policy is evaluated for READER through team_memberships by all_for_identity/subscribe_for_identity.",
                "The schema has single-column team indexes; there is no composite (team, updated_at DESC) index builder in this public schema surface.",
            ],
        ),
        SeedMode::Global => (
            "explicit global-current team indexes on documents and team_memberships",
            vec![
                "Global bootstrap is per-row because that is the public settled import API; seed time should not be compared with local batch seed time.",
                "first_binding_read measures first use of that shape/binding after the shared fixture was seeded; it is not a process/storage cold start.",
                "The document policy is evaluated for READER through team_memberships by all_for_identity/subscribe_for_identity.",
                "The schema has single-column team indexes; there is no composite (team, updated_at DESC) index builder in this public schema surface.",
            ],
        ),
    };

    Ok(BenchmarkOutput {
        benchmark: "saas_documents",
        config,
        storage: "MemoryStorage",
        index_scope,
        distribution,
        seeding,
        scenarios,
        authorization_canary,
        parameterized_cell_access_canary,
        simultaneous_parameter_bindings_canary,
        observed_ratios,
        notes,
        tooling_friction: "Global settled bootstrap is row-at-a-time; a batch import API would shorten setup.",
    })
}

fn saas_schema() -> JazzSchema {
    let document_read_policy = Policy::shape(Query::from("documents").join_via_column(
        "team_memberships",
        "team",
        "team",
        [eq(col("user"), claim("sub"))],
    ));

    JazzSchema::new([
        TableSchema::new("teams", [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "team_memberships",
            [
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("user", ColumnType::Uuid),
                ColumnSchema::new("role", ColumnType::String),
            ],
        )
        .with_reference("team", "teams")
        .with_indexed_columns(["team", "user"])
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "documents",
            [
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("updated_at", ColumnType::U64),
                ColumnSchema::new("status", ColumnType::String),
                ColumnSchema::new("archived", ColumnType::Bool),
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("body", ColumnType::String),
            ],
        )
        .with_reference("team", "teams")
        .with_indexed_column("team")
        .with_read_policy(document_read_policy)
        .with_write_policy(Policy::public()),
    ])
}

fn open_db(schema: JazzSchema, mode: SeedMode) -> Result<BenchDb, String> {
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let config = DbConfig::new(
        schema,
        MemoryStorage::new(&refs),
        DbIdentity {
            node: SYSTEM_NODE,
            author: AuthorId::SYSTEM,
        },
    )
    .with_id_source(SeededRowIdSource::new(0x5aa5));

    match mode {
        SeedMode::Local => block_on(Db::open(config)),
        SeedMode::Global => block_on(Db::open_history_complete(config)),
    }
    .map_err(|error| format!("open SaaS benchmark db: {error}"))
}

fn build_distribution(config: &Config) -> Result<(Fixture, DistributionReport), String> {
    let mut counts = vec![0usize; config.teams];
    counts[0] = config.hot_documents;
    counts[1] = config.cold_documents;

    let other_teams = config.teams - 2;
    let remaining = config.documents - config.hot_documents - config.cold_documents;
    let quotient = remaining / other_teams;
    let remainder = remaining % other_teams;
    for (offset, count) in counts[2..].iter_mut().enumerate() {
        *count = quotient + usize::from(offset < remainder);
    }

    let exact_total = counts.iter().try_fold(0usize, |sum, count| {
        sum.checked_add(*count)
            .ok_or_else(|| "document distribution overflowed".to_owned())
    })?;
    if exact_total != config.documents {
        return Err(format!(
            "distribution generated {exact_total} documents, expected {}",
            config.documents
        ));
    }

    let mut starts = Vec::with_capacity(counts.len());
    let mut next = 0usize;
    for count in &counts {
        starts.push(next);
        next = next
            .checked_add(*count)
            .ok_or_else(|| "document start offset overflowed".to_owned())?;
    }

    let minimum_total = config
        .hot_documents
        .checked_add(config.cold_documents)
        .and_then(|fixed| {
            fixed.checked_add((config.teams - 2).saturating_mul(REQUESTED_MIN_DOCUMENTS_PER_TEAM))
        })
        .ok_or_else(|| "minimum requested distribution overflowed".to_owned())?;
    let actual_min = *counts.iter().min().expect("at least three teams");
    let actual_max = *counts.iter().max().expect("at least three teams");
    let report = DistributionReport {
        total_documents: exact_total,
        total_teams: counts.len(),
        hot_team_index: 0,
        hot_team_documents: counts[0],
        cold_team_index: 1,
        cold_team_documents: counts[1],
        requested_min_documents_per_team: REQUESTED_MIN_DOCUMENTS_PER_TEAM,
        requested_max_documents_per_team: REQUESTED_MAX_DOCUMENTS_PER_TEAM,
        actual_min_documents_per_team: actual_min,
        actual_max_documents_per_team: actual_max,
        teams_below_requested_min: counts
            .iter()
            .filter(|count| **count < REQUESTED_MIN_DOCUMENTS_PER_TEAM)
            .count(),
        teams_above_requested_max: counts
            .iter()
            .filter(|count| **count > REQUESTED_MAX_DOCUMENTS_PER_TEAM)
            .count(),
        minimum_total_for_requested_hot_and_bounds: minimum_total,
        arithmetic_shortfall: minimum_total.saturating_sub(config.documents),
        other_team_mean_documents: remaining as f64 / other_teams as f64,
        exact_total_preserved: true,
    };

    Ok((
        Fixture {
            team_document_counts: counts,
            team_document_starts: starts,
        },
        report,
    ))
}

fn seed_rows(
    db: &BenchDb,
    mode: SeedMode,
    batch_size: usize,
    phase: &'static str,
    count: usize,
    mut make_row: impl FnMut(usize) -> (&'static str, RowUuid, RowCells),
) -> Result<PhaseReport, String> {
    let started = Instant::now();
    let progress_step = (count / 10).max(1);
    let mut next_progress = progress_step;

    match mode {
        SeedMode::Local => {
            let mut start = 0usize;
            while start < count {
                let end = start.saturating_add(batch_size).min(count);
                let mut tx = db.mergeable_tx();
                for index in start..end {
                    let (table, row, cells) = make_row(index);
                    tx.insert_with_id(table, row, cells)
                        .map_err(|error| format!("stage {phase} row {index}: {error}"))?;
                }
                tx.commit()
                    .map_err(|error| format!("commit {phase} rows {start}..{end}: {error}"))?;
                start = end;
                if start >= next_progress || start == count {
                    eprintln!("seeded {phase}: {start}/{count}");
                    next_progress = next_progress.saturating_add(progress_step);
                }
            }
            Ok(PhaseReport::new(
                count,
                started.elapsed(),
                "Db::mergeable_tx (<=2048 rows/commit)",
            ))
        }
        SeedMode::Global => {
            for index in 0..count {
                let (table, row, cells) = make_row(index);
                db.seed_settled_mergeable_for_bootstrap(table, row, AuthorId::SYSTEM, cells)
                    .map_err(|error| format!("settled-bootstrap {phase} row {index}: {error}"))?;
                let done = index + 1;
                if done >= next_progress || done == count {
                    eprintln!("seeded {phase}: {done}/{count}");
                    next_progress = next_progress.saturating_add(progress_step);
                }
            }
            Ok(PhaseReport::new(
                count,
                started.elapsed(),
                "Db::seed_settled_mergeable_for_bootstrap (one row/call)",
            ))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn run_scenario(
    db: &BenchDb,
    fixture: &Fixture,
    read_opts: &ReadOpts,
    query_iterations: usize,
    team_profile: &'static str,
    team_index: usize,
    variant: FilterVariant,
    style: QueryStyle,
) -> Result<ScenarioReport, String> {
    let team = fixture.team(team_index);
    let query = build_query(style, variant, team);
    let prepare_started = Instant::now();
    let prepared = match style {
        QueryStyle::Literal => db.prepare_query(&query),
        QueryStyle::Parameterized => db.prepare_query_bound(
            &query,
            BTreeMap::from([("team".to_owned(), Value::Uuid(team.0))]),
        ),
    }
    .map_err(|error| {
        format!(
            "prepare {} {} {team_profile} query: {error}",
            style.label(),
            variant.label()
        )
    })?;
    let prepare_elapsed = prepare_started.elapsed();

    let expected_updated_at = expected_updated_at(fixture, team_index, variant, 100);
    let expected_rows = expected_updated_at.len();
    let first_started = Instant::now();
    let first_rows =
        block_on(db.all_for_identity(&prepared, read_opts.clone(), READER)).map_err(|error| {
            format!(
                "first authorized {} {} {team_profile} read: {error}",
                style.label(),
                variant.label()
            )
        })?;
    let first_elapsed = first_started.elapsed();
    validate_rows(&first_rows, &expected_updated_at)?;
    black_box(first_rows.len());

    let mut repeated = Vec::with_capacity(query_iterations);
    for iteration in 0..query_iterations {
        let started = Instant::now();
        let rows = block_on(db.all_for_identity(&prepared, read_opts.clone(), READER)).map_err(
            |error| {
                format!(
                    "repeated authorized {} {} {team_profile} read {iteration}: {error}",
                    style.label(),
                    variant.label()
                )
            },
        )?;
        let elapsed = started.elapsed();
        validate_rows(&rows, &expected_updated_at)?;
        black_box(rows.len());
        repeated.push(elapsed);
    }
    let repeated_reads = latency_stats(&repeated);
    let first_us = micros(first_elapsed);
    let first_to_repeated_mean_ratio = ratio(first_us, repeated_reads.mean_us);

    Ok(ScenarioReport {
        name: format!(
            "{team_profile}/{}/{style}",
            variant.label(),
            style = style.label()
        ),
        team_profile,
        team_index,
        documents_in_team: fixture.count(team_index),
        query_style: style.label(),
        filter_variant: variant.label(),
        authorization_api: "Db::all_for_identity(READER)",
        order_by: "updated_at DESC",
        limit: 100,
        expected_rows,
        prepare_us: micros(prepare_elapsed),
        first_binding_read_us: first_us,
        repeated_reads,
        first_to_repeated_mean_ratio,
        correctness: "validated exact deterministic row identities in descending updated_at order, filters, limit, and policy visibility",
    })
}

fn build_query(style: QueryStyle, variant: FilterVariant, team: RowUuid) -> Query {
    let team_predicate = match style {
        QueryStyle::Literal => eq(col("team"), lit(team.0)),
        QueryStyle::Parameterized => eq(col("team"), param("team")),
    };
    let mut query = Query::from("documents").filter(team_predicate);
    if matches!(variant, FilterVariant::ActiveUnarchived) {
        query = query
            .filter(eq(col("status"), lit("active")))
            .filter(eq(col("archived"), lit(false)));
    }
    query
        .order_by("updated_at", OrderDirection::Desc)
        .limit(100)
}

fn validate_rows(
    rows: &[jazz::node::CurrentRow],
    expected_updated_at: &[u64],
) -> Result<(), String> {
    if rows.len() != expected_updated_at.len() {
        return Err(format!(
            "query returned {} rows, expected {}",
            rows.len(),
            expected_updated_at.len()
        ));
    }

    for (position, (row, expected_timestamp)) in rows.iter().zip(expected_updated_at).enumerate() {
        let expected_row = row_uuid(
            0x53,
            usize::try_from(*expected_timestamp).expect("benchmark timestamp fits in usize"),
        );
        if row.row_uuid() != expected_row {
            return Err(format!(
                "row {position} is {}, expected {} for updated_at {expected_timestamp}",
                row.row_uuid().0,
                expected_row.0
            ));
        }
    }
    Ok(())
}

fn expected_updated_at(
    fixture: &Fixture,
    team_index: usize,
    variant: FilterVariant,
    limit: usize,
) -> Vec<u64> {
    let start = fixture.start(team_index);
    (0..fixture.count(team_index))
        .rev()
        .filter(|local_index| variant.matches(*local_index))
        .take(limit)
        .map(|local_index| {
            u64::try_from(start + local_index).expect("validated document count fits in u64")
        })
        .collect()
}

fn authorization_canary(
    db: &BenchDb,
    fixture: &Fixture,
    read_opts: &ReadOpts,
) -> Result<JsonValue, String> {
    let team = fixture.team(0);
    let query = build_query(QueryStyle::Parameterized, FilterVariant::TeamOnly, team);
    let prepared = db
        .prepare_query_bound(
            &query,
            BTreeMap::from([("team".to_owned(), Value::Uuid(team.0))]),
        )
        .map_err(|error| format!("prepare outsider authorization canary: {error}"))?;
    let rows = block_on(db.all_for_identity(&prepared, read_opts.clone(), OUTSIDER))
        .map_err(|error| format!("run outsider authorization canary: {error}"))?;
    if !rows.is_empty() {
        return Err(format!(
            "outsider saw {} hot-team documents without membership",
            rows.len()
        ));
    }
    Ok(json!({
        "ok": true,
        "authorized_identity": READER.0.to_string(),
        "unauthorized_identity": OUTSIDER.0.to_string(),
        "unauthorized_rows": 0,
        "api": "Db::all_for_identity",
    }))
}

fn parameterized_cell_access_canary(
    db: &BenchDb,
    fixture: &Fixture,
    document_table: &TableSchema,
    read_opts: &ReadOpts,
) -> Result<JsonValue, String> {
    let team = fixture.team(0);
    let query = build_query(QueryStyle::Parameterized, FilterVariant::TeamOnly, team);
    let prepared = db
        .prepare_query_bound(
            &query,
            BTreeMap::from([("team".to_owned(), Value::Uuid(team.0))]),
        )
        .map_err(|error| format!("prepare parameterized cell canary: {error}"))?;
    let rows = block_on(db.all_for_identity(&prepared, read_opts.clone(), READER))
        .map_err(|error| format!("read parameterized cell canary: {error}"))?;
    let actual = rows
        .first()
        .and_then(|row| row.cell(document_table, "team"));
    Ok(json!({
        "ok": actual == Some(Value::Uuid(team.0)),
        "api": "CurrentRow::cell",
        "expected_team": team.0.to_string(),
        "actual_team": format!("{actual:?}"),
        "observation": "parameter-constrained projections currently expose user_team as non-nullable; CurrentRow::cell expects nullable user cells",
    }))
}

fn simultaneous_bindings_canary(
    db: &BenchDb,
    fixture: &Fixture,
    seed_mode: SeedMode,
    read_opts: &ReadOpts,
) -> Result<JsonValue, String> {
    let parameterized = build_query(
        QueryStyle::Parameterized,
        FilterVariant::TeamOnly,
        fixture.team(0),
    );
    let hot_team = fixture.team(0);
    let cold_team = fixture.team(1);
    let hot = db
        .prepare_query_bound(
            &parameterized,
            BTreeMap::from([("team".to_owned(), Value::Uuid(hot_team.0))]),
        )
        .map_err(|error| format!("prepare hot simultaneous binding: {error}"))?;
    let cold = db
        .prepare_query_bound(
            &parameterized,
            BTreeMap::from([("team".to_owned(), Value::Uuid(cold_team.0))]),
        )
        .map_err(|error| format!("prepare cold simultaneous binding: {error}"))?;
    if hot.binding().binding_id() == cold.binding().binding_id() {
        return Err("hot and cold prepared bindings have the same binding id".to_owned());
    }

    let mut hot_subscription = block_on(db.subscribe_for_identity(&hot, read_opts.clone(), READER))
        .map_err(|error| format!("subscribe hot simultaneous binding: {error}"))?;
    let mut cold_subscription =
        block_on(db.subscribe_for_identity(&cold, read_opts.clone(), READER))
            .map_err(|error| format!("subscribe cold simultaneous binding: {error}"))?;

    let expected_hot = expected_row_ids(fixture, 0, FilterVariant::TeamOnly, 100);
    let expected_cold = expected_row_ids(fixture, 1, FilterVariant::TeamOnly, 100);
    let mut hot_rows = validate_initial_subscription(
        "hot",
        hot_subscription
            .try_next_event()
            .ok_or_else(|| "hot subscription did not queue its initial reset".to_owned())?,
        &expected_hot,
    )?;
    let mut cold_rows = validate_initial_subscription(
        "cold",
        cold_subscription
            .try_next_event()
            .ok_or_else(|| "cold subscription did not queue its initial reset".to_owned())?,
        &expected_cold,
    )?;

    let hot_post_bind_events =
        drain_subscription_events("hot after cold bind", &mut hot_subscription, &mut hot_rows)?;
    let cold_post_bind_events = drain_subscription_events(
        "cold after hot bind",
        &mut cold_subscription,
        &mut cold_rows,
    )?;
    validate_subscription_snapshot("hot after both binds", &hot_rows, &expected_hot)?;
    validate_subscription_snapshot("cold after both binds", &cold_rows, &expected_cold)?;

    let total_documents = fixture.team_document_counts.iter().copied().sum::<usize>();
    let hot_insert = row_uuid(0x5f, 0);
    insert_canary_document(
        db,
        seed_mode,
        hot_insert,
        hot_team,
        total_documents,
        fixture.count(0),
    )?;
    let hot_mutation_events =
        drain_subscription_events("hot mutation", &mut hot_subscription, &mut hot_rows)?;
    if hot_mutation_events == 0 {
        return Err("hot subscription emitted no event for a newest hot-team row".to_owned());
    }
    let cold_events_after_hot = drain_subscription_events(
        "cold after hot mutation",
        &mut cold_subscription,
        &mut cold_rows,
    )?;
    let mut expected_hot_after = expected_hot.clone();
    expected_hot_after.insert(hot_insert);
    if expected_hot_after.len() > 100 {
        expected_hot_after.pop_first();
    }
    validate_subscription_snapshot("hot after hot mutation", &hot_rows, &expected_hot_after)?;
    validate_subscription_snapshot("cold after hot mutation", &cold_rows, &expected_cold)?;

    let cold_insert = row_uuid(0x5f, 1);
    insert_canary_document(
        db,
        seed_mode,
        cold_insert,
        cold_team,
        total_documents + 1,
        fixture.count(1),
    )?;
    let cold_mutation_events =
        drain_subscription_events("cold mutation", &mut cold_subscription, &mut cold_rows)?;
    if cold_mutation_events == 0 {
        return Err("cold subscription emitted no event for a newest cold-team row".to_owned());
    }
    let hot_events_after_cold = drain_subscription_events(
        "hot after cold mutation",
        &mut hot_subscription,
        &mut hot_rows,
    )?;
    let mut expected_cold_after = expected_cold.clone();
    expected_cold_after.insert(cold_insert);
    if expected_cold_after.len() > 100 {
        expected_cold_after.pop_first();
    }
    validate_subscription_snapshot("hot after cold mutation", &hot_rows, &expected_hot_after)?;
    validate_subscription_snapshot("cold after cold mutation", &cold_rows, &expected_cold_after)?;

    Ok(json!({
        "ok": true,
        "api": "Db::subscribe_for_identity",
        "same_shape": hot.shape().shape_id() == cold.shape().shape_id(),
        "distinct_binding_ids": true,
        "hot_rows": hot_rows.len(),
        "cold_rows": cold_rows.len(),
        "hot_post_bind_events": hot_post_bind_events,
        "cold_post_bind_events": cold_post_bind_events,
        "hot_mutation_events": hot_mutation_events,
        "cold_events_after_hot": cold_events_after_hot,
        "cold_mutation_events": cold_mutation_events,
        "hot_events_after_cold": hot_events_after_cold,
    }))
}

fn validate_initial_subscription(
    label: &str,
    event: SubscriptionEvent,
    expected_rows: &BTreeSet<RowUuid>,
) -> Result<BTreeSet<RowUuid>, String> {
    match event {
        SubscriptionEvent::Delta {
            reset: true,
            added,
            updated,
            removed,
            ..
        } => {
            if !removed.is_empty() {
                return Err(format!("{label} initial reset unexpectedly removed rows"));
            }
            let actual_rows = added
                .iter()
                .chain(updated.iter())
                .map(|row| row.row_uuid())
                .collect::<BTreeSet<_>>();
            validate_subscription_snapshot(label, &actual_rows, expected_rows)?;
            Ok(actual_rows)
        }
        other => Err(format!(
            "expected {label} initial reset delta for simultaneous binding, got {other:?}"
        )),
    }
}

fn expected_row_ids(
    fixture: &Fixture,
    team_index: usize,
    variant: FilterVariant,
    limit: usize,
) -> BTreeSet<RowUuid> {
    expected_updated_at(fixture, team_index, variant, limit)
        .into_iter()
        .map(|timestamp| {
            row_uuid(
                0x53,
                usize::try_from(timestamp).expect("benchmark timestamp fits in usize"),
            )
        })
        .collect()
}

fn drain_subscription_events(
    label: &str,
    subscription: &mut SubscriptionStream,
    snapshot: &mut BTreeSet<RowUuid>,
) -> Result<usize, String> {
    let mut events = 0;
    while let Some(event) = subscription.try_next_event() {
        events += 1;
        apply_subscription_event(label, snapshot, event)?;
    }
    Ok(events)
}

fn apply_subscription_event(
    label: &str,
    snapshot: &mut BTreeSet<RowUuid>,
    event: SubscriptionEvent,
) -> Result<(), String> {
    match event {
        SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            ..
        } => {
            if reset {
                snapshot.clear();
            }
            for row in removed {
                snapshot.remove(&row.row_uuid);
            }
            for row in added.into_iter().chain(updated) {
                snapshot.insert(row.row_uuid());
            }
            Ok(())
        }
        other => Err(format!("{label} emitted non-delta event {other:?}")),
    }
}

fn validate_subscription_snapshot(
    label: &str,
    actual: &BTreeSet<RowUuid>,
    expected: &BTreeSet<RowUuid>,
) -> Result<(), String> {
    if actual != expected {
        return Err(format!(
            "{label} has {} rows, expected {}; simultaneous binding leaked or misrouted rows",
            actual.len(),
            expected.len()
        ));
    }
    Ok(())
}

fn insert_canary_document(
    db: &BenchDb,
    seed_mode: SeedMode,
    row: RowUuid,
    team: RowUuid,
    global_index: usize,
    local_index: usize,
) -> Result<(), String> {
    let cells = document_cells(team, global_index, local_index);
    match seed_mode {
        SeedMode::Local => db
            .insert_with_id("documents", row, cells)
            .map(|_| ())
            .map_err(|error| format!("insert local simultaneous-binding canary row: {error}")),
        SeedMode::Global => db
            .seed_settled_mergeable_for_bootstrap("documents", row, AuthorId::SYSTEM, cells)
            .map(|_| ())
            .map_err(|error| format!("insert settled simultaneous-binding canary row: {error}")),
    }
}

fn safe_canary(canary: impl FnOnce() -> Result<JsonValue, String>) -> JsonValue {
    match catch_unwind(AssertUnwindSafe(canary)) {
        Ok(Ok(report)) => report,
        Ok(Err(error)) => json!({
            "ok": false,
            "error": error,
        }),
        Err(payload) => json!({
            "ok": false,
            "panic": panic_message(payload),
        }),
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

fn observed_ratios(scenarios: &[ScenarioReport]) -> JsonValue {
    let mean = |profile: &str, variant: &str, style: &str| {
        scenarios
            .iter()
            .find(|scenario| {
                scenario.team_profile == profile
                    && scenario.filter_variant == variant
                    && scenario.query_style == style
            })
            .map(|scenario| scenario.repeated_reads.mean_us)
    };

    let hot_literal = mean("hot", "team_only", "literal");
    let cold_literal = mean("cold", "team_only", "literal");
    let hot_parameterized = mean("hot", "team_only", "parameterized");
    let hot_filtered = mean("hot", "status_active_and_not_archived", "parameterized");

    json!({
        "hot_to_cold_team_only_literal_repeated_ratio":
            optional_ratio(hot_literal, cold_literal),
        "hot_parameterized_to_literal_repeated_ratio":
            optional_ratio(hot_parameterized, hot_literal),
        "hot_filtered_to_team_only_parameterized_repeated_ratio":
            optional_ratio(hot_filtered, hot_parameterized),
        "interpretation": {
            "hot_to_cold": "sensitivity to team cardinality, candidate scanning, authorization joins, and sorting",
            "parameterized_to_literal": "binding/cache route overhead after preparation",
            "filtered_to_team_only": "cost of non-indexed status/archived predicates over the team candidate set",
        }
    })
}

fn optional_ratio(numerator: Option<f64>, denominator: Option<f64>) -> JsonValue {
    match (numerator, denominator) {
        (Some(numerator), Some(denominator)) if denominator != 0.0 => {
            json!(numerator / denominator)
        }
        _ => JsonValue::Null,
    }
}

fn latency_stats(samples: &[Duration]) -> LatencyStats {
    let mut nanos = samples.iter().map(Duration::as_nanos).collect::<Vec<_>>();
    nanos.sort_unstable();
    let total_nanos = nanos.iter().copied().sum::<u128>();
    let total_seconds = total_nanos as f64 / 1_000_000_000.0;
    let mean_us = total_nanos as f64 / nanos.len() as f64 / 1_000.0;

    LatencyStats {
        iterations: nanos.len(),
        total_ms: total_nanos as f64 / 1_000_000.0,
        mean_us,
        min_us: nanos[0] as f64 / 1_000.0,
        p50_us: percentile(&nanos, 0.50) as f64 / 1_000.0,
        p95_us: percentile(&nanos, 0.95) as f64 / 1_000.0,
        p99_us: percentile(&nanos, 0.99) as f64 / 1_000.0,
        max_us: nanos[nanos.len() - 1] as f64 / 1_000.0,
        reads_per_second: if total_seconds == 0.0 {
            f64::INFINITY
        } else {
            nanos.len() as f64 / total_seconds
        },
    }
}

fn percentile(sorted_nanos: &[u128], percentile: f64) -> u128 {
    let index = ((sorted_nanos.len() - 1) as f64 * percentile).round() as usize;
    sorted_nanos[index]
}

fn membership_cells(team: RowUuid, user: AuthorId, member_index: usize) -> RowCells {
    BTreeMap::from([
        ("team".to_owned(), Value::Uuid(team.0)),
        ("user".to_owned(), Value::Uuid(user.0)),
        (
            "role".to_owned(),
            Value::String(if member_index == 0 { "owner" } else { "member" }.to_owned()),
        ),
    ])
}

fn document_cells(team: RowUuid, global_index: usize, local_index: usize) -> RowCells {
    BTreeMap::from([
        ("team".to_owned(), Value::Uuid(team.0)),
        (
            "updated_at".to_owned(),
            Value::U64(u64::try_from(global_index).expect("validated document count fits in u64")),
        ),
        (
            "status".to_owned(),
            Value::String(status(local_index).to_owned()),
        ),
        ("archived".to_owned(), Value::Bool(archived(local_index))),
        (
            "title".to_owned(),
            Value::String(format!("Document {global_index}")),
        ),
        (
            "body".to_owned(),
            Value::String("Synthetic SaaS benchmark document body".to_owned()),
        ),
    ])
}

fn status(local_index: usize) -> &'static str {
    match local_index % 3 {
        0 => "active",
        1 => "draft",
        _ => "closed",
    }
}

fn archived(local_index: usize) -> bool {
    local_index.is_multiple_of(10)
}

fn member_identity(team_index: usize, member_index: usize, membership_index: usize) -> AuthorId {
    if member_index == 0 && team_index < 2 {
        READER
    } else {
        AuthorId(row_uuid(0x54, membership_index).0)
    }
}

fn row_uuid(kind: u8, index: usize) -> RowUuid {
    let mut bytes = [0u8; 16];
    bytes[0] = kind;
    bytes[8..].copy_from_slice(
        &u64::try_from(index)
            .expect("benchmark row index fits in u64")
            .wrapping_add(1)
            .to_be_bytes(),
    );
    RowUuid::from_bytes(bytes)
}

fn env_usize(name: &str, default: usize) -> Result<usize, String> {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .map_err(|error| format!("{name}={value:?} is not a usize: {error}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(format!("read {name}: {error}")),
    }
}

fn ratio(numerator: f64, denominator: f64) -> f64 {
    if denominator == 0.0 {
        f64::INFINITY
    } else {
        numerator / denominator
    }
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}
