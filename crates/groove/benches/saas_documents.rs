//! SaaS-style team document query benchmark.
//!
//! The default fixture contains exactly 500k documents and 5k teams, including
//! one 30k-document hot team and one 100-document small team. Those exact
//! constraints are mathematically incompatible with every team having at least
//! 100 documents (500k / 5k == 100), so the remaining teams absorb the skew and
//! contain 94-95 documents. Set `GROOVE_SAAS_DOCUMENTS=529900` to instead keep
//! every non-hot team at 100 documents.
//!
//! The benchmark compares:
//!
//! - full table hydration + membership semi-join + filtered TopBy(100);
//! - explicit durable-index candidate enumeration before the same graph;
//! - a raw composite-index prefix scan and a last-with-prefix control;
//! - maintained TopBy writes inside and outside the selected team, plus a
//!   100-row selected-team batch.
//!
//! Run the full default fixture:
//!
//! ```text
//! cargo bench --profile perf -p groove --bench saas_documents --quiet
//! ```
//!
//! Useful knobs:
//! `GROOVE_SAAS_DOCUMENTS`, `GROOVE_SAAS_TEAMS`,
//! `GROOVE_SAAS_HOT_TEAM_DOCUMENTS`, `GROOVE_SAAS_MEMBERS_PER_TEAM`,
//! `GROOVE_SAAS_QUERY_ITERS`, `GROOVE_SAAS_WRITE_ITERS`, and
//! `GROOVE_SAAS_BATCH_SIZE`.

use std::collections::BTreeSet;
use std::env;
use std::time::{Duration, Instant};

use groove::db::{Database, GraphBuilder, PredicateExpr, StorageReadMetrics};
use groove::ivm::{RecordDeltas, TopByLimit, TopByOrder};
use groove::records::{RecordDescriptor, Value};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::{Durability, MemoryStorage, OrderedKvStorage, RocksDbStorage};
use hdrhistogram::Histogram;

const HOT_TEAM_ID: u64 = 1;
const SMALL_TEAM_ID: u64 = 2;
const CURRENT_USER_ID: u64 = 1;
const UNAUTHORIZED_USER_ID: u64 = u64::MAX - 1;
const ACTIVE_STATUS: u8 = 1;
const PAGE_SIZE: usize = 100;
const SUBSCRIBED_BATCH_ROWS: usize = 100;

const DOCUMENTS_BY_TEAM_UPDATED: &str = "documents_by_team_updated";
const DOCUMENTS_BY_TEAM_STATUS_ARCHIVED_UPDATED: &str = "documents_by_team_status_archived_updated";
const MEMBERSHIPS_BY_USER_TEAM: &str = "memberships_by_user_team";

fn main() {
    let config = Config::from_env();
    config.validate();
    let schema = saas_schema();
    let column_families = schema
        .column_families()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let column_family_refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();

    match env::var("GROOVE_SAAS_STORAGE")
        .unwrap_or_else(|_| "rocksdb".to_owned())
        .as_str()
    {
        "memory" => run(
            config,
            schema,
            MemoryStorage::new(&column_family_refs),
            "memory",
        ),
        "rocksdb" => {
            let temp_dir = tempfile::tempdir().expect("create benchmark RocksDB directory");
            let storage = RocksDbStorage::open_with_durability(
                temp_dir.path(),
                &column_family_refs,
                Durability::WalNoSync,
            )
            .expect("open benchmark RocksDB");
            run(config, schema, storage, "rocksdb_wal_no_sync");
        }
        other => panic!("unsupported GROOVE_SAAS_STORAGE={other}; use memory or rocksdb"),
    }
}

fn run<S>(config: Config, schema: DatabaseSchema, storage: S, storage_name: &str)
where
    S: OrderedKvStorage,
{
    let document_descriptor = documents_table().record_schema();
    let membership_descriptor = memberships_table().record_schema();
    let mut database = Database::new(schema, storage).expect("open SaaS benchmark database");

    eprintln!(
        "seeding {} documents, {} teams, {} members/team into {storage_name}",
        config.documents, config.teams, config.members_per_team
    );
    let seed_started = Instant::now();
    let fixture = seed_fixture(&mut database, config);
    let seed_elapsed = seed_started.elapsed();
    println!(
        concat!(
            "{{",
            "\"scenario\":\"saas_documents\",",
            "\"case\":\"fixture\",",
            "\"storage\":\"{}\",",
            "\"documents\":{},",
            "\"teams\":{},",
            "\"memberships\":{},",
            "\"hot_team_documents\":{},",
            "\"small_team_documents\":{},",
            "\"other_team_min_documents\":{},",
            "\"other_team_max_documents\":{},",
            "\"seed_us\":{}",
            "}}"
        ),
        storage_name,
        config.documents,
        config.teams,
        config.teams * config.members_per_team,
        fixture.hot_documents.len(),
        fixture.small_documents.len(),
        fixture.other_team_min_documents,
        fixture.other_team_max_documents,
        duration_micros(seed_elapsed),
    );

    for target in [
        TargetTeam::new("hot", HOT_TEAM_ID, &fixture.hot_documents),
        TargetTeam::new("small", SMALL_TEAM_ID, &fixture.small_documents),
    ] {
        for filter in [DocumentFilter::Latest, DocumentFilter::ActiveUnarchived] {
            run_query_case(
                &mut database,
                config,
                storage_name,
                AccessPath::FullScan,
                target,
                filter,
                CURRENT_USER_ID,
                &document_descriptor,
                &membership_descriptor,
            );
            run_query_case(
                &mut database,
                config,
                storage_name,
                AccessPath::Indexed,
                target,
                filter,
                CURRENT_USER_ID,
                &document_descriptor,
                &membership_descriptor,
            );
        }
    }

    run_query_case(
        &mut database,
        config,
        storage_name,
        AccessPath::FullScan,
        TargetTeam::new("hot", HOT_TEAM_ID, &fixture.hot_documents),
        DocumentFilter::Latest,
        UNAUTHORIZED_USER_ID,
        &document_descriptor,
        &membership_descriptor,
    );

    run_index_controls(
        &mut database,
        config,
        storage_name,
        fixture.hot_documents.len(),
    );
    run_subscribed_writes(&mut database, config, storage_name, &fixture.hot_documents);

    let stats = database.runtime_stats();
    println!(
        concat!(
            "{{",
            "\"scenario\":\"saas_documents\",",
            "\"case\":\"runtime_final\",",
            "\"storage\":\"{}\",",
            "\"graph_nodes\":{},",
            "\"arrangements\":{},",
            "\"arrangement_rows\":{},",
            "\"arrangement_bytes\":{},",
            "\"logical_nodes_requested\":{},",
            "\"deduped_graph_nodes\":{},",
            "\"dedupe_ratio\":{},",
            "\"tooling_friction\":\"bounded reverse-prefix iteration with early termination is not exposed\"",
            "}}"
        ),
        storage_name,
        stats.graph_nodes,
        stats.arrangement_count,
        stats.arrangement_rows,
        stats.arrangement_encoded_bytes,
        stats.logical_nodes_requested,
        stats.deduped_graph_nodes,
        stats.dedupe_ratio(),
    );
}

#[derive(Clone, Copy)]
struct Config {
    documents: usize,
    teams: usize,
    hot_team_documents: usize,
    small_team_documents: usize,
    members_per_team: usize,
    query_iterations: usize,
    write_iterations: usize,
    batch_size: usize,
}

impl Config {
    fn from_env() -> Self {
        Self {
            documents: env_usize("GROOVE_SAAS_DOCUMENTS", 500_000),
            teams: env_usize("GROOVE_SAAS_TEAMS", 5_000),
            hot_team_documents: env_usize("GROOVE_SAAS_HOT_TEAM_DOCUMENTS", 30_000),
            small_team_documents: env_usize("GROOVE_SAAS_SMALL_TEAM_DOCUMENTS", 100),
            members_per_team: env_usize("GROOVE_SAAS_MEMBERS_PER_TEAM", 10),
            query_iterations: env_usize("GROOVE_SAAS_QUERY_ITERS", 10),
            write_iterations: env_usize("GROOVE_SAAS_WRITE_ITERS", 20),
            batch_size: env_usize("GROOVE_SAAS_BATCH_SIZE", 10_000),
        }
    }

    fn validate(self) {
        assert!(self.teams >= 3, "benchmark requires at least three teams");
        assert!(
            self.documents >= self.hot_team_documents + self.small_team_documents,
            "document count is smaller than the two target teams"
        );
        assert!(
            self.hot_team_documents >= PAGE_SIZE,
            "hot team must fill one page"
        );
        assert!(
            self.small_team_documents >= PAGE_SIZE,
            "small team must fill one page"
        );
        assert!(
            self.members_per_team > 0,
            "each team needs at least one member"
        );
        assert!(self.query_iterations > 0, "query iterations must be > 0");
        assert!(self.write_iterations > 0, "write iterations must be > 0");
        assert!(self.batch_size > 0, "batch size must be > 0");
    }

    fn team_document_counts(self) -> Vec<usize> {
        let mut counts = vec![0; self.teams];
        counts[0] = self.hot_team_documents;
        counts[1] = self.small_team_documents;
        let remaining = self
            .documents
            .saturating_sub(self.hot_team_documents + self.small_team_documents);
        let other_teams = self.teams - 2;
        let per_team = remaining / other_teams;
        let remainder = remaining % other_teams;
        for (offset, count) in counts[2..].iter_mut().enumerate() {
            *count = per_team + usize::from(offset < remainder);
        }
        assert_eq!(counts.iter().sum::<usize>(), self.documents);
        counts
    }
}

#[derive(Clone, Copy, Debug)]
struct SeedDocument {
    id: u64,
    updated_at: u64,
    status: u8,
    archived: bool,
}

struct Fixture {
    hot_documents: Vec<SeedDocument>,
    small_documents: Vec<SeedDocument>,
    other_team_min_documents: usize,
    other_team_max_documents: usize,
}

fn seed_fixture<S>(database: &mut Database<S>, config: Config) -> Fixture
where
    S: OrderedKvStorage,
{
    let counts = config.team_document_counts();

    let mut teams = database.open_batch();
    for team_id in 1..=config.teams as u64 {
        teams.insert(
            "teams",
            vec![
                Value::U64(team_id),
                Value::String(format!("Team {team_id}")),
            ],
        );
    }
    database.commit_batch(teams).expect("seed teams");

    let mut membership_id = 1_u64;
    let mut staged = 0_usize;
    let mut memberships = database.open_batch();
    for team_id in 1..=config.teams as u64 {
        for member_offset in 0..config.members_per_team {
            let user_id =
                if member_offset == 0 && (team_id == HOT_TEAM_ID || team_id == SMALL_TEAM_ID) {
                    CURRENT_USER_ID
                } else {
                    2 + (team_id - 1) * config.members_per_team as u64 + member_offset as u64
                };
            memberships.insert(
                "team_memberships",
                vec![
                    Value::U64(membership_id),
                    Value::U64(team_id),
                    Value::U64(user_id),
                    Value::U8((member_offset % 3) as u8),
                ],
            );
            membership_id += 1;
            staged += 1;
            if staged == config.batch_size {
                database
                    .commit_batch(std::mem::take(&mut memberships))
                    .expect("seed membership batch");
                staged = 0;
            }
        }
    }
    if staged != 0 {
        database
            .commit_batch(memberships)
            .expect("seed final membership batch");
    }

    let mut hot_documents = Vec::with_capacity(counts[0]);
    let mut small_documents = Vec::with_capacity(counts[1]);
    let mut document_id = 1_u64;
    let mut seeded_documents = 0_usize;
    let mut staged = 0_usize;
    let mut documents = database.open_batch();
    for (team_offset, &document_count) in counts.iter().enumerate() {
        let team_id = team_offset as u64 + 1;
        for ordinal in 0..document_count {
            let ordinal = ordinal as u64 + 1;
            let status = (ordinal % 4) as u8;
            let archived = ordinal.is_multiple_of(19);
            let seed = SeedDocument {
                id: document_id,
                updated_at: ordinal,
                status,
                archived,
            };
            if team_id == HOT_TEAM_ID {
                hot_documents.push(seed);
            } else if team_id == SMALL_TEAM_ID {
                small_documents.push(seed);
            }
            documents.insert(
                "documents",
                vec![
                    Value::U64(document_id),
                    Value::U64(team_id),
                    Value::U64(ordinal),
                    Value::U8(status),
                    Value::Bool(archived),
                    Value::String(format!("Document {document_id}")),
                    Value::String(format!(
                        "team-{team_id}-document-{ordinal}-benchmark-payload"
                    )),
                ],
            );
            document_id += 1;
            staged += 1;
            seeded_documents += 1;
            if staged == config.batch_size {
                database
                    .commit_batch(std::mem::take(&mut documents))
                    .expect("seed document batch");
                staged = 0;
                if seeded_documents.is_multiple_of(100_000) {
                    eprintln!("seeded {seeded_documents}/{} documents", config.documents);
                }
            }
        }
    }
    if staged != 0 {
        database
            .commit_batch(documents)
            .expect("seed final document batch");
    }

    Fixture {
        hot_documents,
        small_documents,
        other_team_min_documents: counts[2..].iter().copied().min().unwrap_or_default(),
        other_team_max_documents: counts[2..].iter().copied().max().unwrap_or_default(),
    }
}

#[derive(Clone, Copy)]
struct TargetTeam<'a> {
    name: &'static str,
    id: u64,
    documents: &'a [SeedDocument],
}

impl<'a> TargetTeam<'a> {
    fn new(name: &'static str, id: u64, documents: &'a [SeedDocument]) -> Self {
        Self {
            name,
            id,
            documents,
        }
    }
}

#[derive(Clone, Copy)]
enum DocumentFilter {
    Latest,
    ActiveUnarchived,
}

impl DocumentFilter {
    fn name(self) -> &'static str {
        match self {
            Self::Latest => "latest",
            Self::ActiveUnarchived => "active_unarchived",
        }
    }

    fn matches(self, document: SeedDocument) -> bool {
        match self {
            Self::Latest => true,
            Self::ActiveUnarchived => document.status == ACTIVE_STATUS && !document.archived,
        }
    }
}

#[derive(Clone, Copy)]
enum AccessPath {
    FullScan,
    Indexed,
}

impl AccessPath {
    fn name(self) -> &'static str {
        match self {
            Self::FullScan => "full_scan",
            Self::Indexed => "indexed_candidates",
        }
    }
}

struct BuiltGraph {
    graph: GraphBuilder,
    document_candidates: usize,
    membership_candidates: usize,
}

#[allow(clippy::too_many_arguments)]
fn run_query_case<S>(
    database: &mut Database<S>,
    config: Config,
    storage_name: &str,
    access_path: AccessPath,
    target: TargetTeam<'_>,
    filter: DocumentFilter,
    user_id: u64,
    document_descriptor: &RecordDescriptor,
    membership_descriptor: &RecordDescriptor,
) where
    S: OrderedKvStorage,
{
    let expected = expected_ids(target.documents, filter, user_id == CURRENT_USER_ID);
    let mut repeat_histogram = Histogram::new(3).expect("repeat latency histogram");
    let mut cold_us = 0_u64;
    let mut cold_reads = StorageReadMetrics::default();
    let mut document_candidates = 0_usize;
    let mut membership_candidates = 0_usize;

    for iteration in 0..=config.query_iterations {
        database.reset_storage_read_metrics();
        let started = Instant::now();
        let built = build_query_graph(
            database,
            access_path,
            target.id,
            filter,
            user_id,
            document_descriptor,
            membership_descriptor,
        );
        document_candidates = match access_path {
            AccessPath::FullScan => config.documents,
            AccessPath::Indexed => built.document_candidates,
        };
        membership_candidates = match access_path {
            AccessPath::FullScan => config.teams * config.members_per_team,
            AccessPath::Indexed => built.membership_candidates,
        };
        let result = database
            .query_graph(built.graph)
            .expect("execute SaaS document graph");
        let elapsed = started.elapsed();
        assert_result(&result, &expected);
        let reads = database.take_storage_read_metrics();
        if iteration == 0 {
            cold_us = duration_micros(elapsed);
            cold_reads = reads;
        } else {
            repeat_histogram
                .record(duration_micros(elapsed))
                .expect("record repeat query");
        }
    }

    println!(
        concat!(
            "{{",
            "\"scenario\":\"saas_documents\",",
            "\"case\":\"query\",",
            "\"storage\":\"{}\",",
            "\"access_path\":\"{}\",",
            "\"team\":\"{}\",",
            "\"team_documents\":{},",
            "\"filter\":\"{}\",",
            "\"authorized\":{},",
            "\"returned\":{},",
            "\"document_candidates\":{},",
            "\"membership_candidates\":{},",
            "\"cold_us\":{},",
            "\"repeat_us\":{},",
            "\"cold_storage_records\":{},",
            "\"cold_storage_ranges\":{}",
            "}}"
        ),
        storage_name,
        access_path.name(),
        target.name,
        target.documents.len(),
        filter.name(),
        user_id == CURRENT_USER_ID,
        expected.len(),
        document_candidates,
        membership_candidates,
        cold_us,
        histogram_json(&repeat_histogram),
        cold_reads.total.reads,
        cold_reads.total.ranges,
    );
}

fn build_query_graph<S>(
    database: &Database<S>,
    access_path: AccessPath,
    team_id: u64,
    filter: DocumentFilter,
    user_id: u64,
    document_descriptor: &RecordDescriptor,
    membership_descriptor: &RecordDescriptor,
) -> BuiltGraph
where
    S: OrderedKvStorage,
{
    let (documents, document_candidates) = match access_path {
        AccessPath::FullScan => (GraphBuilder::table("documents"), 0),
        AccessPath::Indexed => {
            let (index, prefix) = match filter {
                DocumentFilter::Latest => (DOCUMENTS_BY_TEAM_UPDATED, vec![Value::U64(team_id)]),
                DocumentFilter::ActiveUnarchived => (
                    DOCUMENTS_BY_TEAM_STATUS_ARCHIVED_UPDATED,
                    vec![
                        Value::U64(team_id),
                        Value::U8(ACTIVE_STATUS),
                        Value::Bool(false),
                    ],
                ),
            };
            let records = database
                .index_scan_raw("documents", index, &prefix)
                .expect("scan document index")
                .into_iter()
                .map(|row| row.record().raw().to_vec())
                .collect::<Vec<_>>();
            let count = records.len();
            (
                GraphBuilder::inline_records(*document_descriptor, records),
                count,
            )
        }
    };
    let documents = documents.filter(document_predicate(team_id, filter));

    let (memberships, membership_candidates) = match access_path {
        AccessPath::FullScan => (GraphBuilder::table("team_memberships"), 0),
        AccessPath::Indexed => {
            let records = database
                .index_scan_raw(
                    "team_memberships",
                    MEMBERSHIPS_BY_USER_TEAM,
                    &[Value::U64(user_id), Value::U64(team_id)],
                )
                .expect("scan membership index")
                .into_iter()
                .map(|row| row.record().raw().to_vec())
                .collect::<Vec<_>>();
            let count = records.len();
            (
                GraphBuilder::inline_records(*membership_descriptor, records),
                count,
            )
        }
    };
    let memberships = memberships.filter(PredicateExpr::eq("user_id", Value::U64(user_id)));
    let authorized = GraphBuilder::semi_join(documents, memberships, ["team_id"], ["team_id"]);
    let graph = GraphBuilder::top_by(
        authorized,
        Vec::<String>::new(),
        [TopByOrder::desc("updated_at")],
        ["id"],
        0,
        TopByLimit::Finite(PAGE_SIZE as u64),
    );
    BuiltGraph {
        graph,
        document_candidates,
        membership_candidates,
    }
}

fn document_predicate(team_id: u64, filter: DocumentFilter) -> PredicateExpr {
    let mut predicates = vec![PredicateExpr::eq("team_id", Value::U64(team_id))];
    if matches!(filter, DocumentFilter::ActiveUnarchived) {
        predicates.extend([
            PredicateExpr::eq("status", Value::U8(ACTIVE_STATUS)),
            PredicateExpr::eq("archived", Value::Bool(false)),
        ]);
    }
    PredicateExpr::And(predicates).canonicalize()
}

fn expected_ids(
    documents: &[SeedDocument],
    filter: DocumentFilter,
    authorized: bool,
) -> BTreeSet<u64> {
    if !authorized {
        return BTreeSet::new();
    }
    let mut matches = documents
        .iter()
        .copied()
        .filter(|document| filter.matches(*document))
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| {
        right
            .updated_at
            .cmp(&left.updated_at)
            .then_with(|| left.id.cmp(&right.id))
    });
    matches
        .into_iter()
        .take(PAGE_SIZE)
        .map(|document| document.id)
        .collect()
}

fn assert_result(result: &RecordDeltas, expected: &BTreeSet<u64>) {
    let mut actual = BTreeSet::new();
    for (record, weight) in result.iter() {
        assert_eq!(weight, 1, "one-shot TopBy rows must have unit weight");
        let Value::U64(id) = record.get("id").expect("read result id") else {
            panic!("document id is not u64");
        };
        actual.insert(id);
    }
    assert_eq!(&actual, expected, "SaaS query disagrees with oracle");
}

fn run_index_controls<S>(
    database: &mut Database<S>,
    config: Config,
    storage_name: &str,
    expected_hot_rows: usize,
) where
    S: OrderedKvStorage,
{
    let mut scan_histogram = Histogram::new(3).expect("index scan histogram");
    let mut scan_reads = StorageReadMetrics::default();
    for _ in 0..config.query_iterations {
        database.reset_storage_read_metrics();
        let started = Instant::now();
        let rows = database
            .index_scan_raw(
                "documents",
                DOCUMENTS_BY_TEAM_UPDATED,
                &[Value::U64(HOT_TEAM_ID)],
            )
            .expect("scan hot-team ordered index");
        assert_eq!(rows.len(), expected_hot_rows);
        scan_histogram
            .record(duration_micros(started.elapsed()))
            .expect("record index scan");
        scan_reads = database.take_storage_read_metrics();
    }

    let mut last_histogram = Histogram::new(3).expect("index last histogram");
    let mut last_reads = StorageReadMetrics::default();
    for _ in 0..config.query_iterations {
        database.reset_storage_read_metrics();
        let started = Instant::now();
        let last = database
            .index_last_raw(
                "documents",
                DOCUMENTS_BY_TEAM_UPDATED,
                &[Value::U64(HOT_TEAM_ID)],
            )
            .expect("seek last hot-team document");
        assert!(last.is_some());
        last_histogram
            .record(duration_micros(started.elapsed()))
            .expect("record index last");
        last_reads = database.take_storage_read_metrics();
    }

    println!(
        concat!(
            "{{",
            "\"scenario\":\"saas_documents\",",
            "\"case\":\"ordered_index_controls\",",
            "\"storage\":\"{}\",",
            "\"team_documents\":{},",
            "\"prefix_scan_us\":{},",
            "\"prefix_scan_records\":{},",
            "\"prefix_scan_ranges\":{},",
            "\"last_seek_us\":{},",
            "\"last_seek_records\":{},",
            "\"last_seek_ranges\":{}",
            "}}"
        ),
        storage_name,
        expected_hot_rows,
        histogram_json(&scan_histogram),
        scan_reads.total.reads,
        scan_reads.total.ranges,
        histogram_json(&last_histogram),
        last_reads.total.reads,
        last_reads.total.ranges,
    );
}

fn run_subscribed_writes<S>(
    database: &mut Database<S>,
    config: Config,
    storage_name: &str,
    hot_documents: &[SeedDocument],
) where
    S: OrderedKvStorage,
{
    let expected = expected_ids(hot_documents, DocumentFilter::ActiveUnarchived, true);
    let mut expected_page = expected.clone();
    let initial_started = Instant::now();
    let subscription = database
        .subscribe_one_sink(
            build_query_graph(
                database,
                AccessPath::FullScan,
                HOT_TEAM_ID,
                DocumentFilter::ActiveUnarchived,
                CURRENT_USER_ID,
                &documents_table().record_schema(),
                &memberships_table().record_schema(),
            )
            .graph,
        )
        .expect("subscribe to hot team");
    let initial = subscription.recv().expect("receive initial hot-team page");
    let initial_elapsed = initial_started.elapsed();
    assert_result(&initial, &expected);

    let mut inside_wall = Histogram::new(3).expect("inside-team wall histogram");
    let mut inside_tick = Histogram::new(3).expect("inside-team tick histogram");
    let mut inside_storage = Histogram::new(3).expect("inside-team storage histogram");
    let mut outside_wall = Histogram::new(3).expect("outside-team wall histogram");
    let mut outside_tick = Histogram::new(3).expect("outside-team tick histogram");
    let mut outside_storage = Histogram::new(3).expect("outside-team storage histogram");
    let mut next_id = config.documents as u64 + 1;
    let mut next_updated_at = hot_documents
        .iter()
        .map(|document| document.updated_at)
        .max()
        .unwrap_or_default()
        + 1;

    for iteration in 0..config.write_iterations {
        let inserted_id = next_id;
        let mut batch = database.open_batch();
        batch.insert(
            "documents",
            document_values(
                inserted_id,
                HOT_TEAM_ID,
                next_updated_at,
                ACTIVE_STATUS,
                false,
            ),
        );
        let started = Instant::now();
        database
            .commit_batch(batch)
            .expect("insert newest selected-team document");
        inside_wall
            .record(duration_micros(started.elapsed()))
            .expect("record selected-team write");
        let metrics = database
            .last_commit_metrics()
            .expect("selected-team commit metrics");
        inside_tick
            .record(duration_micros(metrics.ivm_tick_time))
            .expect("record selected-team tick");
        inside_storage
            .record(duration_micros(metrics.storage_write_time))
            .expect("record selected-team storage");
        let delta = subscription
            .recv()
            .expect("receive selected-team TopBy delta");
        expected_page.insert(inserted_id);
        let removed_id = (expected_page.len() > PAGE_SIZE)
            .then(|| expected_page.pop_first().expect("page has an oldest row"));
        assert_top_by_write_delta(&delta, inserted_id, removed_id, iteration);
        next_id += 1;
        next_updated_at += 1;

        let outside_team = 3 + iteration as u64 % (config.teams as u64 - 2);
        let mut batch = database.open_batch();
        batch.insert(
            "documents",
            document_values(next_id, outside_team, next_updated_at, ACTIVE_STATUS, false),
        );
        let started = Instant::now();
        database
            .commit_batch(batch)
            .expect("insert outside selected team");
        outside_wall
            .record(duration_micros(started.elapsed()))
            .expect("record outside-team write");
        let metrics = database
            .last_commit_metrics()
            .expect("outside-team commit metrics");
        outside_tick
            .record(duration_micros(metrics.ivm_tick_time))
            .expect("record outside-team tick");
        outside_storage
            .record(duration_micros(metrics.storage_write_time))
            .expect("record outside-team storage");
        assert!(
            matches!(
                subscription.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ),
            "outside-team write unexpectedly changed the selected-team page at iteration {iteration}"
        );
        next_id += 1;
        next_updated_at += 1;
    }

    let previous_page = expected_page.clone();
    let mut batch_page = BTreeSet::new();
    let mut batch = database.open_batch();
    for _ in 0..SUBSCRIBED_BATCH_ROWS {
        batch.insert(
            "documents",
            document_values(next_id, HOT_TEAM_ID, next_updated_at, ACTIVE_STATUS, false),
        );
        batch_page.insert(next_id);
        next_id += 1;
        next_updated_at += 1;
    }
    let batch_started = Instant::now();
    database
        .commit_batch(batch)
        .expect("insert selected-team document batch");
    let batch_wall_us = duration_micros(batch_started.elapsed());
    let batch_metrics = database
        .last_commit_metrics()
        .expect("selected-team batch commit metrics");
    let batch_tick_us = duration_micros(batch_metrics.ivm_tick_time);
    let batch_storage_us = duration_micros(batch_metrics.storage_write_time);
    let batch_delta = subscription
        .recv()
        .expect("receive selected-team batch TopBy delta");
    let (batch_added, batch_removed) = top_by_delta_ids(&batch_delta, "batch");
    assert_eq!(
        batch_added, batch_page,
        "the 100 newest batch rows must replace the selected-team page"
    );
    assert_eq!(
        batch_removed, previous_page,
        "the selected-team batch evicted the wrong previous page"
    );

    database.unsubscribe(subscription.id());

    println!(
        concat!(
            "{{",
            "\"scenario\":\"saas_documents\",",
            "\"case\":\"subscribed_writes\",",
            "\"storage\":\"{}\",",
            "\"initial_team_documents\":{},",
            "\"pre_batch_team_documents\":{},",
            "\"initial_us\":{},",
            "\"inside_team_wall_us\":{},",
            "\"inside_team_tick_us\":{},",
            "\"inside_team_storage_us\":{},",
            "\"outside_team_wall_us\":{},",
            "\"outside_team_tick_us\":{},",
            "\"outside_team_storage_us\":{},",
            "\"batch_rows\":{},",
            "\"batch_commit_wall_us\":{},",
            "\"batch_tick_us\":{},",
            "\"batch_storage_us\":{}",
            "}}"
        ),
        storage_name,
        hot_documents.len(),
        hot_documents.len() + config.write_iterations,
        duration_micros(initial_elapsed),
        histogram_json(&inside_wall),
        histogram_json(&inside_tick),
        histogram_json(&inside_storage),
        histogram_json(&outside_wall),
        histogram_json(&outside_tick),
        histogram_json(&outside_storage),
        SUBSCRIBED_BATCH_ROWS,
        batch_wall_us,
        batch_tick_us,
        batch_storage_us,
    );
}

fn assert_top_by_write_delta(
    delta: &RecordDeltas,
    inserted_id: u64,
    removed_id: Option<u64>,
    iteration: usize,
) {
    let label = format!("iteration {iteration}");
    let (added, removed) = top_by_delta_ids(delta, &label);
    assert_eq!(
        added,
        BTreeSet::from([inserted_id]),
        "newest selected-team row must enter the page at iteration {iteration}"
    );
    assert_eq!(
        removed,
        removed_id.into_iter().collect(),
        "selected-team page evicted the wrong row at iteration {iteration}"
    );
}

fn top_by_delta_ids(delta: &RecordDeltas, label: &str) -> (BTreeSet<u64>, BTreeSet<u64>) {
    let mut added = BTreeSet::new();
    let mut removed = BTreeSet::new();
    for (record, weight) in delta.iter() {
        let Value::U64(id) = record.get("id").expect("read subscription result id") else {
            panic!("subscription document id is not u64");
        };
        match weight {
            1 => {
                added.insert(id);
            }
            -1 => {
                removed.insert(id);
            }
            other => panic!("unexpected TopBy delta weight {other} for {label}"),
        }
    }
    (added, removed)
}

fn document_values(
    id: u64,
    team_id: u64,
    updated_at: u64,
    status: u8,
    archived: bool,
) -> Vec<Value> {
    vec![
        Value::U64(id),
        Value::U64(team_id),
        Value::U64(updated_at),
        Value::U8(status),
        Value::Bool(archived),
        Value::String(format!("Document {id}")),
        Value::String(format!(
            "team-{team_id}-document-{updated_at}-benchmark-payload"
        )),
    ]
}

fn saas_schema() -> DatabaseSchema {
    DatabaseSchema::new([teams_table(), memberships_table(), documents_table()])
}

fn teams_table() -> TableSchema {
    TableSchema::new(
        "teams",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("name", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
}

fn memberships_table() -> TableSchema {
    TableSchema::new(
        "team_memberships",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("team_id", ColumnType::U64),
            ColumnSchema::new("user_id", ColumnType::U64),
            ColumnSchema::new("role", ColumnType::U8),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new(
        MEMBERSHIPS_BY_USER_TEAM,
        ["user_id", "team_id", "id"],
    ))
}

fn documents_table() -> TableSchema {
    TableSchema::new(
        "documents",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("team_id", ColumnType::U64),
            ColumnSchema::new("updated_at", ColumnType::U64),
            ColumnSchema::new("status", ColumnType::U8),
            ColumnSchema::new("archived", ColumnType::Bool),
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new(
        DOCUMENTS_BY_TEAM_UPDATED,
        ["team_id", "updated_at", "id"],
    ))
    .with_index(IndexSchema::new(
        DOCUMENTS_BY_TEAM_STATUS_ARCHIVED_UPDATED,
        ["team_id", "status", "archived", "updated_at", "id"],
    ))
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn duration_micros(duration: Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}

fn histogram_json(histogram: &Histogram<u64>) -> String {
    format!(
        "{{\"n\":{},\"p50\":{},\"p95\":{},\"p99\":{},\"max\":{}}}",
        histogram.len(),
        histogram.value_at_quantile(0.50),
        histogram.value_at_quantile(0.95),
        histogram.value_at_quantile(0.99),
        histogram.max(),
    )
}
