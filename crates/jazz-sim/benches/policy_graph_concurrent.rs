use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Instant;

use jazz::db::{
    Db, DbConfig, DbIdentity, Node, ReadOpts, SeededRowIdSource, SubscriptionEvent,
    SubscriptionStream, Transport,
};
use jazz::groove::records::Value;
use jazz::groove::schema::ColumnType;
use jazz::groove::storage::{Durability, RocksDbStorage};
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::{CurrentRow, MergeableCommit, NodeState};
use jazz::protocol::SyncMessage;
use jazz::query::Query;
use jazz::schema::{JazzSchema, TableSchema};
use jazz::tx::DurabilityTier;
use jazz::wire::TransportError;
use jazz_sim::{emit_json_line, metadata_fields};
use serde_json::{Value as JsonValue, json};

const PUBLIC_FIXTURE_DIR: &str = "../../packages/jazz-tools/src/testing/fixtures/policy-graph-perf";
const FIXTURE_DIR_ENV: &str = "JAZZ_POLICY_GRAPH_FIXTURE_DIR";
const MEMBER_SEED_ROWS: &str = "member-seed-rows.json";
const SEED_CACHE_VERSION: &str = "policy-graph-concurrent-seed-v14-env-fixture";
const SEED_CACHE_READY: &str = ".jazz_policy_graph_seed_ready";

fn main() {
    let session_id = std::env::var("CODEX_SESSION_ID")
        .or_else(|_| std::env::var("JAZZ_CODEX_SESSION_ID"))
        .unwrap_or_else(|_| format!("pid-{}", std::process::id()));
    eprintln!("POLICY_GRAPH_CONCURRENT_SESSION_ID {session_id}");

    let config = Config::from_env();
    eprintln!(
        "POLICY_GRAPH_CONCURRENT_FIXTURE label={} dir={}",
        config.fixture.label,
        config.fixture.dir.display()
    );
    let schema = policy_graph_schema_fixture(&config.fixture);
    let seeded = seed_core(&schema, &config);
    let manifest = member_seed_manifest(&config.fixture);
    let expected = manifest.expected_counts();
    assert_core_visibility(&seeded, &expected, config.identity);

    if config.runs_phase("cold") {
        let summary = run_cold(&schema, &seeded, &expected, &config);
        emit_summary(&config, &session_id, "cold", &summary);
    }
    if config.runs_phase("warm") {
        let summary = run_warm(&schema, &seeded, &expected, &config);
        emit_summary(&config, &session_id, "warm", &summary);
    }
}

fn assert_core_visibility(
    seeded: &Seeded,
    expected: &BTreeMap<String, usize>,
    identity: BenchIdentity,
) {
    let read_opts = ReadOpts {
        tier: DurabilityTier::Global,
        ..ReadOpts::default()
    };
    for table in seeded.subscription_tables() {
        let prepared = seeded
            .core
            .prepare_query(&Query::from(table.as_str()))
            .unwrap_or_else(|error| panic!("prepare core visibility {table}: {error}"));
        let rows = jazz::db::block_on(seeded.core.all_for_identity(
            &prepared,
            read_opts.clone(),
            identity.author(seeded),
        ))
        .unwrap_or_else(|error| panic!("core visibility {table}: {error}"));
        let expected_count = expected[&table];
        if rows.len() != expected_count {
            let local_rows = jazz::db::block_on(seeded.core.all_for_identity(
                &prepared,
                ReadOpts::default(),
                identity.author(seeded),
            ))
            .unwrap_or_default();
            let system_rows = jazz::db::block_on(seeded.core.all_for_identity(
                &prepared,
                read_opts.clone(),
                AuthorId::SYSTEM,
            ))
            .unwrap_or_default();
            let policy_debug = if table == "t105" {
                Some(t105_access_debug(seeded, identity.author(seeded)))
            } else {
                None
            };
            panic!(
                "core visibility mismatch for {table}: got={} expected={} local_rows={} system_rows={} policy_debug={:?}",
                rows.len(),
                expected_count,
                local_rows.len(),
                system_rows.len(),
                policy_debug,
            );
        }
    }
}

fn t105_access_debug(seeded: &Seeded, member: AuthorId) -> String {
    let t1_member = visible_count(seeded, "t1", member);
    let t188_member = visible_count(seeded, "t188", member);
    let t190_member = visible_count(seeded, "t190", member);
    let t190_system = visible_count(seeded, "t190", AuthorId::SYSTEM);
    let prepared = seeded
        .core
        .prepare_query(&Query::from("t190"))
        .expect("prepare t190 debug");
    let rows = jazz::db::block_on(seeded.core.all_for_identity(
        &prepared,
        ReadOpts {
            tier: DurabilityTier::Global,
            ..ReadOpts::default()
        },
        AuthorId::SYSTEM,
    ))
    .unwrap_or_default();
    let table = find_table(&seeded.schema, "t190");
    let samples = rows
        .iter()
        .take(5)
        .map(|row| {
            format!(
                "resource={:?} team={:?} role={:?} admin={:?}",
                row.cell(table, "c456"),
                row.cell(table, "c457"),
                row.cell(table, "c458"),
                row.cell(table, "c459")
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    let t190_teams = rows
        .iter()
        .filter_map(|row| match row.cell(table, "c457") {
            Some(Value::Uuid(uuid)) => Some(RowUuid(uuid)),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let member_t1_rows = visible_rows(seeded, "t1", member);
    let member_t1_ids = member_t1_rows
        .iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    let t190_team_hits = t190_teams.intersection(&member_t1_ids).count();
    format!(
        "t1_member={t1_member} t188_member={t188_member} t190_member={t190_member} t190_system={t190_system} t190_team_hits_in_member_t1={t190_team_hits}/{} t190_samples=[{samples}]",
        t190_teams.len()
    )
}

fn visible_count(seeded: &Seeded, table: &str, author: AuthorId) -> usize {
    visible_rows(seeded, table, author).len()
}

fn visible_rows(seeded: &Seeded, table: &str, author: AuthorId) -> Vec<CurrentRow> {
    let prepared = seeded
        .core
        .prepare_query(&Query::from(table))
        .unwrap_or_else(|error| panic!("prepare {table} debug: {error}"));
    jazz::db::block_on(seeded.core.all_for_identity(
        &prepared,
        ReadOpts {
            tier: DurabilityTier::Global,
            ..ReadOpts::default()
        },
        author,
    ))
    .unwrap_or_default()
}

fn public_fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(PUBLIC_FIXTURE_DIR)
}

fn private_default_fixture_dir() -> Option<PathBuf> {
    let home = std::env::var_os("HOME")?;
    Some(
        PathBuf::from(home)
            .join("jazz-private")
            .join("fixtures")
            .join(format!("{}{}", "bored", "m-real")),
    )
}

fn usable_fixture_dir(dir: &Path) -> bool {
    dir.join("schema.native.bin").is_file() && dir.join(MEMBER_SEED_ROWS).is_file()
}

fn member_seed_manifest(fixture: &Fixture) -> MemberSeedManifest {
    member_seed_dump(fixture).manifest
}

fn member_seed_dump(fixture: &Fixture) -> MemberSeedDump {
    let bytes =
        fs::read(fixture.member_seed_path()).expect("read policy graph member seed row dump");
    let value: JsonValue =
        serde_json::from_slice(&bytes).expect("decode policy graph perf member seed row dump");
    let identity = value
        .get("identity")
        .expect("member seed row dump identity");
    let member_row = identity
        .get("member_row")
        .and_then(JsonValue::as_str)
        .expect("member seed row dump member_row")
        .to_owned();
    let claims = identity
        .get("claims")
        .and_then(JsonValue::as_object)
        .expect("member seed row dump claims")
        .iter()
        .map(|(key, value)| (key.clone(), json_to_claim_value(value, &member_row)))
        .collect::<BTreeMap<_, _>>();
    let tables = value
        .get("subscriptions")
        .and_then(JsonValue::as_array)
        .expect("member seed row dump subscriptions")
        .iter()
        .map(|table| {
            let name = table
                .get("name")
                .and_then(JsonValue::as_str)
                .expect("manifest table name")
                .to_owned();
            let expected = table
                .get("expected")
                .and_then(JsonValue::as_u64)
                .expect("manifest table expected") as usize;
            ManifestTable { name, expected }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        tables.len(),
        39,
        "member seed row dump must cover all 39 subscriptions"
    );
    let rows = value
        .get("rows")
        .and_then(JsonValue::as_array)
        .expect("member seed row dump rows")
        .iter()
        .map(|row| SeedRow {
            table: row
                .get("table")
                .and_then(JsonValue::as_str)
                .expect("seed row table")
                .to_owned(),
            id: row
                .get("id")
                .and_then(JsonValue::as_str)
                .expect("seed row id")
                .to_owned(),
            cells: row
                .get("cells")
                .and_then(JsonValue::as_object)
                .expect("seed row cells")
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        })
        .collect::<Vec<_>>();
    MemberSeedDump {
        identity: MemberSeedIdentity { member_row, claims },
        manifest: MemberSeedManifest { tables },
        rows,
    }
}

impl MemberSeedManifest {
    fn expected_counts(&self) -> BTreeMap<String, usize> {
        self.tables
            .iter()
            .map(|table| (table.name.clone(), table.expected))
            .collect()
    }
}

struct Config {
    seed: u64,
    max_ticks: usize,
    phases: Vec<String>,
    fresh_seed: bool,
    identity: BenchIdentity,
    fixture: Fixture,
}

#[derive(Clone, Debug)]
struct Fixture {
    dir: PathBuf,
    label: &'static str,
}

impl Config {
    fn from_env() -> Self {
        let fixture = Fixture::from_env_or_exit();
        Self {
            seed: env_u64("JAZZ_POLICY_GRAPH_SEED", 0xC039_0039),
            max_ticks: env_usize("JAZZ_POLICY_GRAPH_MAX_TICKS", 50_000),
            phases: std::env::var("JAZZ_POLICY_GRAPH_PHASES")
                .unwrap_or_else(|_| "cold,warm".to_owned())
                .split(',')
                .map(str::trim)
                .filter(|phase| !phase.is_empty())
                .map(str::to_owned)
                .collect(),
            fresh_seed: std::env::var_os("JAZZ_POLICY_GRAPH_FRESH_SEED").is_some(),
            identity: match std::env::var("JAZZ_POLICY_GRAPH_IDENTITY")
                .unwrap_or_else(|_| "member".to_owned())
                .as_str()
            {
                "system" => BenchIdentity::System,
                "member" => BenchIdentity::Member,
                other => panic!(
                    "unsupported JAZZ_POLICY_GRAPH_IDENTITY {other:?}; expected system or member"
                ),
            },
            fixture,
        }
    }

    fn runs_phase(&self, phase: &str) -> bool {
        self.phases.iter().any(|candidate| candidate == phase)
    }
}

impl Fixture {
    fn from_env_or_exit() -> Self {
        if let Some(path) = std::env::var_os(FIXTURE_DIR_ENV).map(PathBuf::from) {
            return Self::require(path, "env");
        }

        if let Some(path) = private_default_fixture_dir().filter(|path| usable_fixture_dir(path)) {
            return Self {
                dir: path,
                label: "private-real",
            };
        }

        let public = public_fixture_dir();
        if usable_fixture_dir(&public) {
            return Self {
                dir: public,
                label: "anonymized",
            };
        }

        eprintln!(
            "POLICY_GRAPH_CONCURRENT_SKIP no usable fixture found; set {FIXTURE_DIR_ENV} to a directory containing schema.native.bin and {MEMBER_SEED_ROWS}"
        );
        std::process::exit(0);
    }

    fn require(dir: PathBuf, label: &'static str) -> Self {
        if usable_fixture_dir(&dir) {
            Self { dir, label }
        } else {
            eprintln!(
                "POLICY_GRAPH_CONCURRENT_SKIP fixture directory is missing schema.native.bin or {MEMBER_SEED_ROWS}: {}",
                dir.display()
            );
            std::process::exit(0);
        }
    }

    fn schema_path(&self) -> PathBuf {
        self.dir.join("schema.native.bin")
    }

    fn member_seed_path(&self) -> PathBuf {
        self.dir.join(MEMBER_SEED_ROWS)
    }

    fn member_seed_manifest(&self) -> MemberSeedManifest {
        member_seed_manifest(self)
    }

    fn cache_discriminator(&self) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.dir.hash(&mut hasher);
        if let Ok(metadata) = fs::metadata(self.schema_path()) {
            metadata.len().hash(&mut hasher);
            metadata.modified().ok().hash(&mut hasher);
        }
        if let Ok(metadata) = fs::metadata(self.member_seed_path()) {
            metadata.len().hash(&mut hasher);
            metadata.modified().ok().hash(&mut hasher);
        }
        hasher.finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BenchIdentity {
    System,
    Member,
}

impl BenchIdentity {
    fn author(self, seeded: &Seeded) -> AuthorId {
        match self {
            Self::System => AuthorId::SYSTEM,
            Self::Member => seeded.member,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Member => "member",
        }
    }
}

struct Seeded {
    _core_dir: Rc<tempfile::TempDir>,
    core: Db<RocksDbStorage>,
    schema: JazzSchema,
    member: AuthorId,
    claims: BTreeMap<String, Value>,
    seed_cache_hit: bool,
    seed_ms: u128,
    subscription_tables: Vec<String>,
}

impl Seeded {
    fn subscription_tables(&self) -> Vec<String> {
        self.subscription_tables.clone()
    }
}

struct DbNode {
    _dir: Rc<tempfile::TempDir>,
    db: Db<RocksDbStorage>,
}

struct OpenSubscription {
    name: String,
    expected: usize,
    stream: SubscriptionStream,
    rows: BTreeSet<RowUuid>,
    first_settled_ms: Option<u128>,
    raw_expected_count_ms: Option<u128>,
    materialized_ms: Option<u128>,
}

struct RunSummary {
    wall_ms: u128,
    server_open_bundle_ms: u128,
    subscribe_ms: u128,
    settle_loop_ms: u128,
    client_apply_tick_ms: u128,
    settled_first_callback_all_ms: u128,
    expected_count_all_ms: u128,
    raw_expected_count_all_ms: u128,
    one_shot_validate_ms: u128,
    consolidated_windows: usize,
    consolidated_window_records: usize,
    history_window_consolidation_us: u128,
    ticks: usize,
    subscriptions: usize,
    rows_materialized: usize,
    expected_rows: usize,
    server_to_client_messages: u64,
    server_to_client_view_updates: u64,
    seed_cache_hit: bool,
    seed_ms: u128,
    slowest_subscription: String,
    slowest_subscription_ms: u128,
    timelines: Vec<SubscriptionTimeline>,
}

struct SubscriptionTimeline {
    name: String,
    rows: usize,
    expected: usize,
    first_settled_ms: u128,
    raw_expected_count_ms: u128,
    materialized_ms: u128,
}

#[derive(Clone, Debug)]
struct MemberSeedDump {
    identity: MemberSeedIdentity,
    manifest: MemberSeedManifest,
    rows: Vec<SeedRow>,
}

#[derive(Clone, Debug)]
struct MemberSeedIdentity {
    member_row: String,
    claims: BTreeMap<String, Value>,
}

#[derive(Clone, Debug)]
struct SeedRow {
    table: String,
    id: String,
    cells: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug)]
struct MemberSeedManifest {
    tables: Vec<ManifestTable>,
}

#[derive(Clone, Debug)]
struct ManifestTable {
    name: String,
    expected: usize,
}

#[derive(Default)]
struct TransportMetrics {
    messages: Cell<u64>,
    view_updates: Cell<u64>,
}

struct QueueTransport {
    outbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    metrics: Rc<TransportMetrics>,
}

struct CountedDuplex {
    left_transport: Box<dyn Transport>,
    right_transport: Box<dyn Transport>,
    right_inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    right_to_left: Rc<TransportMetrics>,
}

impl Transport for QueueTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.metrics.messages.set(self.metrics.messages.get() + 1);
        if matches!(
            message,
            SyncMessage::ViewUpdate { .. } | SyncMessage::ViewUpdateChunk { .. }
        ) {
            self.metrics
                .view_updates
                .set(self.metrics.view_updates.get() + 1);
        }
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
    let left_to_right = Rc::new(TransportMetrics::default());
    let right_to_left = Rc::new(TransportMetrics::default());
    CountedDuplex {
        left_transport: Box::new(QueueTransport {
            outbound: Rc::clone(&left),
            inbound: Rc::clone(&right),
            metrics: Rc::clone(&left_to_right),
        }),
        right_transport: Box::new(QueueTransport {
            outbound: Rc::clone(&right),
            inbound: Rc::clone(&left),
            metrics: Rc::clone(&right_to_left),
        }),
        right_inbound: right,
        right_to_left,
    }
}

fn policy_graph_schema_fixture(fixture: &Fixture) -> JazzSchema {
    let bytes = fs::read(fixture.schema_path()).expect("read policy graph native schema fixture");
    postcard::from_bytes(&bytes).expect("decode policy graph native schema fixture")
}

fn seed_core(schema: &JazzSchema, config: &Config) -> Seeded {
    let start = Instant::now();
    let dump = member_seed_dump(&config.fixture);
    let cache_key = format!(
        "{}-{:016x}-{:016x}",
        SEED_CACHE_VERSION,
        config.seed,
        config.fixture.cache_discriminator()
    );
    let cache_dir = seed_cache_root().join(cache_key);
    let cache_hit = !config.fresh_seed && cache_dir.join(SEED_CACHE_READY).is_file();
    if !cache_hit {
        if cache_dir.exists() {
            fs::remove_dir_all(&cache_dir).expect("remove stale policy graph seed cache");
        }
        let tmp_cache = cache_dir.with_extension(format!("tmp-{}", std::process::id()));
        if tmp_cache.exists() {
            fs::remove_dir_all(&tmp_cache).expect("remove stale temporary policy graph cache");
        }
        fs::create_dir_all(&tmp_cache).expect("create policy graph seed cache");
        {
            let core = open_history_complete_node_at(&tmp_cache, schema.clone(), node(1));
            write_seed_rows(&core, schema, &dump.rows);
        }
        fs::write(tmp_cache.join(SEED_CACHE_READY), SEED_CACHE_VERSION)
            .expect("write policy graph seed ready marker");
        fs::rename(&tmp_cache, &cache_dir).expect("install policy graph seed cache");
    }

    let core_dir = tempfile::tempdir().expect("create core dir");
    copy_dir_contents(&cache_dir, core_dir.path()).expect("copy cached policy graph seed");
    let core_db =
        open_history_complete_db_at(core_dir.path(), schema.clone(), node(1), AuthorId::SYSTEM);
    let member = AuthorId(
        uuid::Uuid::parse_str(&dump.identity.member_row)
            .expect("member seed row dump member_row uuid"),
    );
    let claims = dump.identity.claims;
    core_db.set_identity_claims(member, claims.clone());
    Seeded {
        _core_dir: Rc::new(core_dir),
        core: core_db,
        schema: schema.clone(),
        member,
        claims,
        seed_cache_hit: cache_hit,
        seed_ms: start.elapsed().as_millis(),
        subscription_tables: config
            .fixture
            .member_seed_manifest()
            .tables
            .into_iter()
            .map(|table| table.name)
            .collect(),
    }
}

fn write_seed_rows(core: &Node<RocksDbStorage>, schema: &JazzSchema, rows: &[SeedRow]) {
    let node = core.node();
    for (idx, row) in rows.iter().enumerate() {
        let table = find_table(schema, &row.table);
        let row_id = RowUuid(
            uuid::Uuid::parse_str(&row.id)
                .unwrap_or_else(|error| panic!("seed row uuid {}: {error}", row.id)),
        );
        let cells = row
            .cells
            .iter()
            .map(|(column, value)| {
                let column_schema = table
                    .columns
                    .iter()
                    .find(|candidate| candidate.name == *column)
                    .unwrap_or_else(|| panic!("seed row missing column {}/{}", row.table, column));
                (
                    column.clone(),
                    json_to_cell_value(value, &column_schema.column_type),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let tx_id = node
            .borrow_mut()
            .commit_mergeable(
                MergeableCommit::new(&row.table, row_id, (idx + 1) as u64)
                    .made_by(AuthorId::SYSTEM)
                    .cells(cells),
            )
            .unwrap_or_else(|error| panic!("seed commit row {}/{row_id:?}: {error}", row.table));
        node.borrow_mut()
            .finalize_local_mergeable_commit(tx_id)
            .unwrap_or_else(|error| panic!("seed finalize row {}/{row_id:?}: {error}", row.table));
    }
}

fn json_to_claim_value(value: &JsonValue, member_row: &str) -> Value {
    match value {
        JsonValue::Bool(value) => Value::Bool(*value),
        JsonValue::String(value) if value == member_row => {
            Value::Uuid(uuid::Uuid::parse_str(value).expect("claim member uuid"))
        }
        JsonValue::String(value) => Value::String(value.clone()),
        other => panic!("unsupported identity claim value {other:?}"),
    }
}

fn json_to_cell_value(value: &JsonValue, column_type: &ColumnType) -> Value {
    match column_type {
        ColumnType::Nullable(inner) => {
            if value.is_null() {
                nullable(None)
            } else {
                nullable(Some(json_to_cell_value(value, inner)))
            }
        }
        ColumnType::U8 => Value::U8(json_u64(value) as u8),
        ColumnType::U16 => Value::U16(json_u64(value) as u16),
        ColumnType::U32 => Value::U32(json_u64(value) as u32),
        ColumnType::U64 => Value::U64(json_u64(value)),
        ColumnType::F64 => Value::F64(json_f64(value)),
        ColumnType::Bool => Value::Bool(
            value
                .as_bool()
                .unwrap_or_else(|| panic!("expected bool cell, got {value:?}")),
        ),
        ColumnType::String => Value::String(match value {
            JsonValue::String(value) => value.clone(),
            JsonValue::Null => String::new(),
            other => serde_json::to_string(other).expect("serialize json cell as string"),
        }),
        ColumnType::Bytes => Value::Bytes(match value {
            JsonValue::Array(values) => values.iter().map(|value| json_u64(value) as u8).collect(),
            JsonValue::String(value) => value.as_bytes().to_vec(),
            other => panic!("expected bytes cell, got {other:?}"),
        }),
        ColumnType::Uuid => Value::Uuid(
            uuid::Uuid::parse_str(
                value
                    .as_str()
                    .unwrap_or_else(|| panic!("expected uuid string cell, got {value:?}")),
            )
            .unwrap_or_else(|error| panic!("invalid uuid cell {value:?}: {error}")),
        ),
        ColumnType::Enum(schema) => {
            let label = value
                .as_str()
                .unwrap_or_else(|| panic!("expected enum string cell, got {value:?}"));
            Value::Enum(
                schema
                    .discriminant(label)
                    .unwrap_or_else(|error| panic!("enum label {label} missing: {error}")),
            )
        }
        ColumnType::Tuple(members) => {
            let values = value
                .as_array()
                .unwrap_or_else(|| panic!("expected tuple array cell, got {value:?}"));
            assert_eq!(
                values.len(),
                members.len(),
                "tuple value/member length mismatch"
            );
            Value::Tuple(
                values
                    .iter()
                    .zip(members)
                    .map(|(value, member_type)| json_to_cell_value(value, member_type))
                    .collect(),
            )
        }
        ColumnType::Array(member_type) => Value::Array(
            value
                .as_array()
                .unwrap_or_else(|| panic!("expected array cell, got {value:?}"))
                .iter()
                .map(|value| json_to_cell_value(value, member_type))
                .collect(),
        ),
    }
}

fn json_u64(value: &JsonValue) -> u64 {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok()))
        .or_else(|| {
            value
                .as_str()
                .and_then(|value| value.parse::<u64>().ok().or_else(|| parse_iso_ms(value)))
        })
        .unwrap_or_else(|| panic!("expected unsigned integer cell, got {value:?}"))
}

fn parse_iso_ms(value: &str) -> Option<u64> {
    let value = value.strip_suffix('Z')?;
    let (date, time) = value.split_once('T')?;
    let mut date_parts = date.split('-');
    let year = date_parts.next()?.parse::<i64>().ok()?;
    let month = date_parts.next()?.parse::<u32>().ok()?;
    let day = date_parts.next()?.parse::<u32>().ok()?;
    if date_parts.next().is_some() {
        return None;
    }

    let mut time_parts = time.split(':');
    let hour = time_parts.next()?.parse::<u32>().ok()?;
    let minute = time_parts.next()?.parse::<u32>().ok()?;
    let second_fraction = time_parts.next()?;
    if time_parts.next().is_some() {
        return None;
    }
    let (second, millis) = match second_fraction.split_once('.') {
        Some((second, fraction)) => {
            let mut padded = fraction.chars().take(3).collect::<String>();
            while padded.len() < 3 {
                padded.push('0');
            }
            (second.parse::<u32>().ok()?, padded.parse::<u32>().ok()?)
        }
        None => (second_fraction.parse::<u32>().ok()?, 0),
    };
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }

    let days = days_from_civil(year, month, day);
    let seconds = days
        .checked_mul(86_400)?
        .checked_add(i64::from(hour) * 3_600 + i64::from(minute) * 60 + i64::from(second))?;
    let ms = seconds.checked_mul(1_000)?.checked_add(i64::from(millis))?;
    u64::try_from(ms).ok()
}

fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = i64::from(month);
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn json_f64(value: &JsonValue) -> f64 {
    value
        .as_f64()
        .unwrap_or_else(|| panic!("expected float cell, got {value:?}"))
}

fn run_cold(
    schema: &JazzSchema,
    seeded: &Seeded,
    expected: &BTreeMap<String, usize>,
    config: &Config,
) -> RunSummary {
    let relay = open_db(schema.clone(), node(2), AuthorId::SYSTEM);
    let client = open_db(schema.clone(), node(3), config.identity.author(seeded));
    run_connect_and_subscribe(seeded, relay, client, expected, config)
}

fn run_warm(
    schema: &JazzSchema,
    seeded: &Seeded,
    expected: &BTreeMap<String, usize>,
    config: &Config,
) -> RunSummary {
    let relay = open_db(schema.clone(), node(4), AuthorId::SYSTEM);
    let client = open_db(schema.clone(), node(5), config.identity.author(seeded));
    let first = run_connect_and_subscribe(seeded, relay, client, expected, config);

    let relay = open_db(schema.clone(), node(6), AuthorId::SYSTEM);
    let client = open_db(schema.clone(), node(7), config.identity.author(seeded));
    let second = run_connect_and_subscribe(seeded, relay, client, expected, config);
    assert!(
        second.wall_ms <= first.wall_ms.saturating_mul(3).max(1),
        "warm run unexpectedly slower than prime run: prime={}ms warm={}ms",
        first.wall_ms,
        second.wall_ms
    );
    second
}

fn run_connect_and_subscribe(
    seeded: &Seeded,
    relay: DbNode,
    client: DbNode,
    expected: &BTreeMap<String, usize>,
    config: &Config,
) -> RunSummary {
    let start = Instant::now();
    let relay_core = duplex_counted();
    let client_relay = duplex_counted();
    let _relay_upstream = relay.db.connect_upstream(relay_core.left_transport);
    let _core_sub = seeded
        .core
        .accept_subscriber(relay_core.right_transport, AuthorId::SYSTEM);
    let _client_upstream = client.db.connect_upstream(client_relay.left_transport);
    let _relay_sub = relay.db.accept_edge_subscriber_with_claims(
        client_relay.right_transport,
        config.identity.author(seeded),
        seeded.claims.clone(),
    );
    relay
        .db
        .set_identity_claims(config.identity.author(seeded), seeded.claims.clone());
    client
        .db
        .set_identity_claims(config.identity.author(seeded), seeded.claims.clone());
    let connect_ms = start.elapsed().as_millis();

    let subscribe_start = Instant::now();
    let mut subscriptions = Vec::new();
    let read_opts = ReadOpts {
        tier: DurabilityTier::Global,
        ..ReadOpts::default()
    };
    for table in subscription_tables(&config.fixture) {
        let query = Query::from(table.as_str());
        let prepared = client
            .db
            .prepare_query(&query)
            .unwrap_or_else(|error| panic!("prepare {table} failed: {error}"));
        let stream = jazz::db::block_on(client.db.subscribe(&prepared, read_opts.clone()))
            .unwrap_or_else(|error| panic!("subscribe {table} failed: {error}"));
        subscriptions.push(OpenSubscription {
            name: table.clone(),
            expected: *expected
                .get(&table)
                .unwrap_or_else(|| panic!("missing expected count for {table}")),
            stream,
            rows: BTreeSet::new(),
            first_settled_ms: None,
            raw_expected_count_ms: None,
            materialized_ms: None,
        });
    }
    let subscribe_ms = subscribe_start.elapsed().as_millis();

    let settle_start = Instant::now();
    let mut server_open_bundle_ms = 0_u128;
    let mut client_apply_tick_ms = 0_u128;
    let mut consolidated_windows = 0_usize;
    let mut consolidated_window_records = 0_usize;
    let mut history_window_consolidation_us = 0_u128;
    let mut ticks = 0_usize;
    while !subscriptions
        .iter()
        .all(|sub| sub.materialized_ms.is_some() && sub.rows.len() == sub.expected)
    {
        if ticks >= config.max_ticks {
            panic!(
                "timed out settling policy graph subscriptions; {}",
                pending_description(&subscriptions)
            );
        }

        let server_start = Instant::now();
        let core_tick = seeded.core.tick_stats().expect("core tick");
        let relay_tick = relay.db.tick_stats().expect("relay tick");
        server_open_bundle_ms += server_start.elapsed().as_millis();
        accumulate_tick_stats(
            core_tick,
            &mut consolidated_windows,
            &mut consolidated_window_records,
            &mut history_window_consolidation_us,
        );
        accumulate_tick_stats(
            relay_tick,
            &mut consolidated_windows,
            &mut consolidated_window_records,
            &mut history_window_consolidation_us,
        );

        let _queued_to_client = client_relay.right_inbound.borrow().len();
        let client_start = Instant::now();
        let client_tick = client.db.tick_stats().expect("client tick");
        drain_subscriptions(start, &mut subscriptions);
        client_apply_tick_ms += client_start.elapsed().as_millis();
        accumulate_tick_stats(
            client_tick,
            &mut consolidated_windows,
            &mut consolidated_window_records,
            &mut history_window_consolidation_us,
        );
        ticks += 1;
    }
    let settle_loop_ms = settle_start.elapsed().as_millis();
    let settled_first_callback_all_ms = subscriptions
        .iter()
        .map(|sub| sub.first_settled_ms.unwrap_or_default())
        .max()
        .unwrap_or_default();
    let expected_count_all_ms = subscriptions
        .iter()
        .map(|sub| sub.materialized_ms.unwrap_or_default())
        .max()
        .unwrap_or_default();
    let raw_expected_count_all_ms = subscriptions
        .iter()
        .map(|sub| sub.raw_expected_count_ms.unwrap_or_default())
        .max()
        .unwrap_or_default();

    let one_shot_validate_start = Instant::now();
    for sub in &subscriptions {
        let prepared = client
            .db
            .prepare_query(&Query::from(sub.name.as_str()))
            .unwrap();
        let rows = jazz::db::block_on(client.db.all(&prepared, read_opts.clone()))
            .unwrap_or_else(|error| panic!("one-shot {} failed: {error}", sub.name));
        assert_eq!(
            rows.len(),
            sub.expected,
            "expected-count mismatch for {}",
            sub.name
        );
    }
    let one_shot_validate_ms = one_shot_validate_start.elapsed().as_millis();

    let rows_materialized = subscriptions
        .iter()
        .map(|sub| sub.rows.len())
        .sum::<usize>();
    let timelines = subscriptions
        .into_iter()
        .map(|sub| SubscriptionTimeline {
            name: sub.name,
            rows: sub.rows.len(),
            expected: sub.expected,
            first_settled_ms: sub.first_settled_ms.unwrap_or_default(),
            raw_expected_count_ms: sub.raw_expected_count_ms.unwrap_or_default(),
            materialized_ms: sub.materialized_ms.unwrap_or_default(),
        })
        .collect::<Vec<_>>();
    let slowest = timelines
        .iter()
        .max_by_key(|timeline| timeline.materialized_ms)
        .expect("at least one subscription");
    RunSummary {
        wall_ms: start.elapsed().as_millis(),
        server_open_bundle_ms: server_open_bundle_ms + connect_ms,
        subscribe_ms,
        settle_loop_ms,
        client_apply_tick_ms,
        settled_first_callback_all_ms,
        expected_count_all_ms,
        raw_expected_count_all_ms,
        one_shot_validate_ms,
        consolidated_windows,
        consolidated_window_records,
        history_window_consolidation_us,
        ticks,
        subscriptions: timelines.len(),
        rows_materialized,
        expected_rows: expected.values().sum(),
        server_to_client_messages: client_relay.right_to_left.messages.get(),
        server_to_client_view_updates: client_relay.right_to_left.view_updates.get(),
        seed_cache_hit: seeded.seed_cache_hit,
        seed_ms: seeded.seed_ms,
        slowest_subscription: slowest.name.clone(),
        slowest_subscription_ms: slowest.materialized_ms,
        timelines,
    }
}

fn subscription_tables(fixture: &Fixture) -> Vec<String> {
    let tables = fixture
        .member_seed_manifest()
        .tables
        .into_iter()
        .map(|table| table.name)
        .collect::<Vec<_>>();
    assert_eq!(tables.len(), 39);
    tables
}

fn drain_subscriptions(start: Instant, subscriptions: &mut [OpenSubscription]) {
    for sub in subscriptions {
        while let Some(event) = sub.stream.try_next_event() {
            let settled = apply_event(&mut sub.rows, event);
            let elapsed = start.elapsed().as_millis();
            if sub.rows.len() == sub.expected && sub.raw_expected_count_ms.is_none() {
                sub.raw_expected_count_ms = Some(elapsed);
            }
            if settled && sub.rows.len() == sub.expected && sub.first_settled_ms.is_none() {
                sub.first_settled_ms = Some(elapsed);
            }
            if settled && sub.rows.len() == sub.expected && sub.materialized_ms.is_none() {
                sub.materialized_ms = Some(elapsed);
            }
        }
    }
}

fn apply_event(rows: &mut BTreeSet<RowUuid>, event: SubscriptionEvent) -> bool {
    match event {
        SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            settled,
            ..
        } => {
            if reset {
                rows.clear();
            }
            for row in removed {
                rows.remove(&row.row_uuid);
            }
            for row in added.into_iter().chain(updated) {
                rows.insert(row.row_uuid());
            }
            settled
        }
        SubscriptionEvent::Closed => false,
    }
}

fn accumulate_tick_stats(
    stats: jazz::db::DbTickStats,
    consolidated_windows: &mut usize,
    consolidated_window_records: &mut usize,
    history_window_consolidation_us: &mut u128,
) {
    *consolidated_windows += stats.consolidated_windows;
    *consolidated_window_records += stats.consolidated_window_records;
    *history_window_consolidation_us += stats.history_window_consolidation_us;
}

fn emit_summary(config: &Config, session_id: &str, phase: &str, summary: &RunSummary) {
    let mut fields = metadata_fields("policy_graph_concurrent", "native", config.seed, "full");
    fields.insert("session_id".to_owned(), json!(session_id));
    fields.insert("phase".to_owned(), json!(phase));
    fields.insert("identity".to_owned(), json!(config.identity.label()));
    fields.insert("fixture_label".to_owned(), json!(config.fixture.label));
    fields.insert("wall_ms".to_owned(), json!(summary.wall_ms));
    fields.insert(
        "settled_first_callback_all_ms".to_owned(),
        json!(summary.settled_first_callback_all_ms),
    );
    fields.insert(
        "expected_count_all_ms".to_owned(),
        json!(summary.expected_count_all_ms),
    );
    fields.insert(
        "raw_expected_count_all_ms".to_owned(),
        json!(summary.raw_expected_count_all_ms),
    );
    fields.insert(
        "phase_breakdown".to_owned(),
        json!({
            "server_open_bundle_ms": summary.server_open_bundle_ms,
            "subscribe_ms": summary.subscribe_ms,
            "settle_loop_ms": summary.settle_loop_ms,
            "client_apply_tick_ms": summary.client_apply_tick_ms,
            "one_shot_validate_ms": summary.one_shot_validate_ms,
            "history_window_consolidation_ms": summary.history_window_consolidation_us as f64 / 1000.0,
        }),
    );
    fields.insert(
        "one_shot_validate_ms".to_owned(),
        json!(summary.one_shot_validate_ms),
    );
    fields.insert(
        "consolidated_windows".to_owned(),
        json!(summary.consolidated_windows),
    );
    fields.insert(
        "consolidated_window_records".to_owned(),
        json!(summary.consolidated_window_records),
    );
    fields.insert(
        "history_window_consolidation_us".to_owned(),
        json!(summary.history_window_consolidation_us),
    );
    fields.insert("ticks".to_owned(), json!(summary.ticks));
    fields.insert("subscriptions".to_owned(), json!(summary.subscriptions));
    fields.insert(
        "rows_materialized".to_owned(),
        json!(summary.rows_materialized),
    );
    fields.insert("expected_rows".to_owned(), json!(summary.expected_rows));
    fields.insert(
        "server_to_client_messages".to_owned(),
        json!(summary.server_to_client_messages),
    );
    fields.insert(
        "server_to_client_view_updates".to_owned(),
        json!(summary.server_to_client_view_updates),
    );
    fields.insert("seed_cache_hit".to_owned(), json!(summary.seed_cache_hit));
    fields.insert("seed_ms".to_owned(), json!(summary.seed_ms));
    fields.insert(
        "slowest_subscription".to_owned(),
        json!(summary.slowest_subscription),
    );
    fields.insert(
        "slowest_subscription_ms".to_owned(),
        json!(summary.slowest_subscription_ms),
    );
    fields.insert(
        "fixture_note".to_owned(),
        json!("native schema fixture plus holder/access/inherits rows loaded from the selected fixture directory; private real data is read in place and never copied into this repository"),
    );
    fields.insert(
        "seed_note".to_owned(),
        json!("uses the same history-complete core import primitive as existing simulator seed code; Db::transaction insert_with_id leaves rows locally pending in this in-process topology and cannot satisfy Global subscriptions without an authority settlement loop"),
    );
    fields.insert(
        "transport_note".to_owned(),
        json!("in-process queue transport; no websocket framing, NAPI bridge, artifact copy, or local-server process startup"),
    );
    fields.insert(
        "subscription_timeline".to_owned(),
        JsonValue::Array(
            summary
                .timelines
                .iter()
                .map(|timeline| {
                    json!({
                        "name": timeline.name,
                        "rows": timeline.rows,
                        "expected": timeline.expected,
                        "first_settled_ms": timeline.first_settled_ms,
                        "raw_expected_count_ms": timeline.raw_expected_count_ms,
                        "materialized_ms": timeline.materialized_ms,
                    })
                })
                .collect(),
        ),
    );
    let line = serde_json::to_string(&fields).expect("serialize policy graph receipt");
    emit_json_line("policy_graph_concurrent", &line);
}

fn open_db(schema: JazzSchema, node_uuid: NodeUuid, author: AuthorId) -> DbNode {
    let dir = Rc::new(tempfile::tempdir().expect("create db tempdir"));
    let db = open_db_at(dir.path(), schema, node_uuid, author, false);
    DbNode { _dir: dir, db }
}

fn open_history_complete_db_at(
    path: &Path,
    schema: JazzSchema,
    node_uuid: NodeUuid,
    author: AuthorId,
) -> Db<RocksDbStorage> {
    open_db_at(path, schema, node_uuid, author, true)
}

fn open_history_complete_node_at(
    path: &Path,
    schema: JazzSchema,
    node_uuid: NodeUuid,
) -> Node<RocksDbStorage> {
    let storage = open_storage_at(path, &schema);
    let state =
        NodeState::new_history_complete(node_uuid, schema, storage).expect("open seed node");
    Node::new(state)
}

fn open_db_at(
    path: &Path,
    schema: JazzSchema,
    node_uuid: NodeUuid,
    author: AuthorId,
    history_complete: bool,
) -> Db<RocksDbStorage> {
    let storage = open_storage_at(path, &schema);
    let config = DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: node_uuid,
            author,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(node_seed(node_uuid)))),
        large_value_checkpoint_op_interval: 1024,
    };
    if history_complete {
        jazz::db::block_on(Db::open_history_complete(config)).expect("open history-complete db")
    } else {
        jazz::db::block_on(Db::open(config)).expect("open db")
    }
}

fn open_storage_at(path: &Path, schema: &JazzSchema) -> RocksDbStorage {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    RocksDbStorage::open_with_durability(path.join("rocksdb"), &refs, Durability::WalNoSync)
        .expect("open rocksdb storage")
}

fn find_table<'a>(schema: &'a JazzSchema, table: &str) -> &'a TableSchema {
    schema
        .tables
        .iter()
        .find(|candidate| candidate.name == table)
        .unwrap_or_else(|| panic!("missing table {table}"))
}

fn nullable(value: Option<Value>) -> Value {
    Value::Nullable(value.map(Box::new))
}

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn node_seed(node_uuid: NodeUuid) -> u64 {
    let bytes = node_uuid.to_bytes();
    u64::from_be_bytes(bytes[..8].try_into().unwrap())
}

fn seed_cache_root() -> PathBuf {
    std::env::var_os("JAZZ_POLICY_GRAPH_SEED_CACHE")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("jazz-policy-graph-concurrent-cache"))
}

fn copy_dir_contents(from: &Path, to: &Path) -> std::io::Result<()> {
    fs::create_dir_all(to)?;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let source = entry.path();
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_contents(&source, &target)?;
        } else {
            fs::copy(&source, &target)?;
        }
    }
    Ok(())
}

fn pending_description(subscriptions: &[OpenSubscription]) -> String {
    subscriptions
        .iter()
        .filter(|sub| sub.rows.len() != sub.expected)
        .map(|sub| {
            format!(
                "{}={}/{} first_settled={:?}",
                sub.name,
                sub.rows.len(),
                sub.expected,
                sub.first_settled_ms
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}
