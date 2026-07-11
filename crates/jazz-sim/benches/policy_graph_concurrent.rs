use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
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
use jazz::node::{MergeableCommit, NodeState};
use jazz::protocol::SyncMessage;
use jazz::query::Query;
use jazz::schema::{JazzSchema, TableSchema};
use jazz::tx::DurabilityTier;
use jazz::wire::TransportError;
use jazz_sim::{emit_json_line, metadata_fields};
use serde_json::{Value as JsonValue, json};

const FIXTURE_DIR: &str = "../../packages/jazz-tools/src/testing/fixtures/policy-graph-perf";
const HEAVY_CHILD_TABLE: &str = "t67";
const HEAVY_CHILD_ROWS: usize = 19_894;
const HOLDER_PARENT_ROWS: usize = 30;
const SEED_CACHE_VERSION: &str = "policy-graph-concurrent-seed-v8";
const ACCESS_ROLE_DISCRIMINANTS: &[u8] = &[0, 1, 2];
const ACCESS_ROLE_STRINGS: &[&str] = &[
    "EDITOR",
    "e17",
    "e18",
    "e19",
    "00000000-0000-4000-8000-000000000004",
    "00000000-0000-4000-8000-000000000005",
    "00000000-0000-4000-8000-000000000006",
];
const SEED_CACHE_READY: &str = ".jazz_policy_graph_seed_ready";

const HOLDER_TABLES: &[&str] = &[
    "t1", "t100", "t101", "t102", "t103", "t104", "t105", "t106", "t107", "t108", "t112", "t121",
    "t142", "t143", "t164", "t168", "t19", "t191", "t195", "t2", "t23", "t27", "t3", "t30", "t56",
    "t58", "t68", "t7", "t75", "t91", "t95", "t96", "t97",
];

const INHERITS_TABLES: &[(&str, &str, &str)] = &[
    ("t16", "c145", "t19"),
    ("t160", "c1770", "t101"),
    ("t166", "c1842", "t164"),
    ("t67", "c724", "t68"),
];

const ACCESS_TABLES: &[(&str, &str)] = &[
    ("t109", "t100"),
    ("t115", "t101"),
    ("t123", "t102"),
    ("t126", "t112"),
    ("t145", "t19"),
    ("t162", "t103"),
    ("t165", "t164"),
    ("t167", "t97"),
    ("t181", "t104"),
    ("t187", "t1"),
    ("t190", "t105"),
    ("t192", "t191"),
    ("t196", "t195"),
    ("t202", "t106"),
    ("t206", "t107"),
    ("t210", "t108"),
    ("t24", "t23"),
    ("t28", "t27"),
    ("t31", "t30"),
    ("t59", "t58"),
    ("t63", "t56"),
    ("t69", "t68"),
    ("t76", "t75"),
    ("t90", "t7"),
    ("t92", "t2"),
    ("t93", "t3"),
    ("t98", "t96"),
    ("t99", "t91"),
];

fn main() {
    let session_id = std::env::var("CODEX_SESSION_ID")
        .or_else(|_| std::env::var("JAZZ_CODEX_SESSION_ID"))
        .unwrap_or_else(|_| format!("pid-{}", std::process::id()));
    eprintln!("POLICY_GRAPH_CONCURRENT_SESSION_ID {session_id}");

    let config = Config::from_env();
    let schema = policy_graph_schema_fixture();
    let seeded = seed_core(&schema, &config);
    let expected = expected_counts();
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
    for table in subscription_tables() {
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
        assert_eq!(
            rows.len(),
            expected_count,
            "core visibility mismatch for {table}"
        );
    }
}

struct Config {
    seed: u64,
    max_ticks: usize,
    phases: Vec<String>,
    fresh_seed: bool,
    identity: BenchIdentity,
}

impl Config {
    fn from_env() -> Self {
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
                .unwrap_or_else(|_| "system".to_owned())
                .as_str()
            {
                "system" => BenchIdentity::System,
                "member" => BenchIdentity::Member,
                other => panic!(
                    "unsupported JAZZ_POLICY_GRAPH_IDENTITY {other:?}; expected system or member"
                ),
            },
        }
    }

    fn runs_phase(&self, phase: &str) -> bool {
        self.phases.iter().any(|candidate| candidate == phase)
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
    member: AuthorId,
    claims: BTreeMap<String, Value>,
    seed_cache_hit: bool,
    seed_ms: u128,
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
    opened_ms: Option<u128>,
    materialized_ms: Option<u128>,
}

struct RunSummary {
    wall_ms: u128,
    server_open_bundle_ms: u128,
    subscribe_ms: u128,
    client_apply_ms: u128,
    expected_count_ms: u128,
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
    opened_ms: u128,
    materialized_ms: u128,
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

fn policy_graph_schema_fixture() -> JazzSchema {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(FIXTURE_DIR)
        .join("schema.native.bin");
    let bytes = fs::read(path).expect("read policy graph perf native schema fixture");
    postcard::from_bytes(&bytes).expect("decode policy graph perf native schema fixture")
}

fn seed_core(schema: &JazzSchema, config: &Config) -> Seeded {
    let start = Instant::now();
    let cache_key = format!("{}-{:016x}", SEED_CACHE_VERSION, config.seed);
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
            write_seed_plan(&core, schema);
        }
        fs::write(tmp_cache.join(SEED_CACHE_READY), SEED_CACHE_VERSION)
            .expect("write policy graph seed ready marker");
        fs::rename(&tmp_cache, &cache_dir).expect("install policy graph seed cache");
    }

    let core_dir = tempfile::tempdir().expect("create core dir");
    copy_dir_contents(&cache_dir, core_dir.path()).expect("copy cached policy graph seed");
    let core_db =
        open_history_complete_db_at(core_dir.path(), schema.clone(), node(1), AuthorId::SYSTEM);
    let member = policy_graph_author(0x31, 1);
    let member_uuid = member.0;
    let claims = BTreeMap::from([
        ("sub".to_owned(), Value::Uuid(member_uuid)),
        ("user_id".to_owned(), Value::Uuid(member_uuid)),
        ("userId".to_owned(), Value::Uuid(member_uuid)),
        ("c787".to_owned(), Value::Uuid(member_uuid)),
        ("isAdmin".to_owned(), Value::Bool(false)),
    ]);
    core_db.set_identity_claims(member, claims.clone());
    Seeded {
        _core_dir: Rc::new(core_dir),
        core: core_db,
        member,
        claims,
        seed_cache_hit: cache_hit,
        seed_ms: start.elapsed().as_millis(),
    }
}

fn write_seed_plan(core: &Node<RocksDbStorage>, schema: &JazzSchema) {
    let member = policy_graph_row(0x31, 1);
    let corporation = policy_graph_row(0x32, 1);
    let mut writes = Vec::<(String, RowUuid, BTreeMap<String, Value>)>::new();

    writes.push((
        "t1".to_owned(),
        member,
        table_cells(schema, "t1", member, member, 0, None),
    ));
    writes.push((
        "t1".to_owned(),
        corporation,
        table_cells(schema, "t1", corporation, member, 1, None),
    ));
    writes.push((
        "t50".to_owned(),
        policy_graph_row(0x35, 1),
        BTreeMap::from([
            ("c457".to_owned(), Value::Uuid(member.0)),
            ("c632".to_owned(), Value::U32(0)),
        ]),
    ));
    writes.push((
        "t188".to_owned(),
        policy_graph_row(0x34, 1),
        BTreeMap::from([
            ("c457".to_owned(), Value::Uuid(member.0)),
            ("c1948".to_owned(), Value::Uuid(corporation.0)),
            ("c1949".to_owned(), nullable(Some(Value::Uuid(member.0)))),
            ("c459".to_owned(), Value::Bool(false)),
            ("c1950".to_owned(), Value::U64(1)),
        ]),
    ));
    writes.push((
        "t188".to_owned(),
        policy_graph_row(0x34, 2),
        BTreeMap::from([
            ("c457".to_owned(), Value::Uuid(corporation.0)),
            ("c1948".to_owned(), Value::Uuid(corporation.0)),
            ("c1949".to_owned(), nullable(Some(Value::Uuid(member.0)))),
            ("c459".to_owned(), Value::Bool(false)),
            ("c1950".to_owned(), Value::U64(1)),
        ]),
    ));

    let mut holder_rows = BTreeMap::<String, Vec<RowUuid>>::new();
    for (table_idx, table) in HOLDER_TABLES.iter().enumerate() {
        let count = if *table == "t68" {
            HOLDER_PARENT_ROWS
        } else if *table == "t1" {
            continue;
        } else {
            1
        };
        for row_idx in 0..count {
            let row = policy_graph_row(holder_kind(table_idx), row_idx as u32);
            holder_rows
                .entry((*table).to_owned())
                .or_default()
                .push(row);
            writes.push((
                (*table).to_owned(),
                row,
                table_cells(schema, table, member, member, row_idx, None),
            ));
        }
    }
    holder_rows.insert("t1".to_owned(), vec![member, corporation]);

    for (access_idx, (access_table, holder_table)) in ACCESS_TABLES.iter().enumerate() {
        if let Some(resources) = holder_rows.get(*holder_table) {
            for (idx, resource) in resources.iter().enumerate() {
                let roles = access_role_values(schema, access_table);
                for (role_idx, role) in roles.into_iter().enumerate() {
                    writes.push((
                        (*access_table).to_owned(),
                        policy_graph_row(
                            0x80_u8.wrapping_add(access_idx as u8),
                            (idx * 8 + role_idx) as u32,
                        ),
                        BTreeMap::from([
                            ("c456".to_owned(), Value::Uuid(resource.0)),
                            ("c457".to_owned(), Value::Uuid(corporation.0)),
                            ("c458".to_owned(), role),
                            ("c459".to_owned(), Value::Bool(false)),
                        ]),
                    ));
                }
            }
        }
    }

    for (idx, (table, via, parent_table)) in INHERITS_TABLES.iter().enumerate() {
        let count = if *table == HEAVY_CHILD_TABLE {
            HEAVY_CHILD_ROWS
        } else {
            1
        };
        let parents = holder_rows
            .get(*parent_table)
            .unwrap_or_else(|| panic!("missing parent rows for {parent_table}"));
        for row_idx in 0..count {
            let parent = parents[row_idx % parents.len()];
            let row = policy_graph_row(0x50_u8.wrapping_add(idx as u8), row_idx as u32);
            writes.push((
                (*table).to_owned(),
                row,
                table_cells(
                    schema,
                    table,
                    corporation,
                    member,
                    row_idx,
                    Some((*via, parent)),
                ),
            ));
        }
    }

    let node = core.node();
    for (idx, (table, row, cells)) in writes.into_iter().enumerate() {
        let tx_id = node
            .borrow_mut()
            .commit_mergeable(
                MergeableCommit::new(&table, row, (idx + 1) as u64)
                    .made_by(AuthorId::SYSTEM)
                    .cells(cells),
            )
            .unwrap_or_else(|error| panic!("seed commit row {table}/{row:?}: {error}"));
        node.borrow_mut()
            .finalize_local_mergeable_commit(tx_id)
            .unwrap_or_else(|error| panic!("seed finalize row {table}/{row:?}: {error}"));
    }
}

fn access_role_values(schema: &JazzSchema, access_table: &str) -> Vec<Value> {
    let table = find_table(schema, access_table);
    let role_type = &table
        .columns
        .iter()
        .find(|column| column.name == "c458")
        .unwrap_or_else(|| panic!("missing c458 on {access_table}"))
        .column_type;
    match role_type {
        ColumnType::Enum(_) => ACCESS_ROLE_DISCRIMINANTS
            .iter()
            .copied()
            .map(Value::Enum)
            .collect(),
        ColumnType::String => ACCESS_ROLE_STRINGS
            .iter()
            .map(|role| Value::String((*role).to_owned()))
            .collect(),
        other => panic!("unsupported access role type on {access_table}: {other:?}"),
    }
}

fn table_cells(
    schema: &JazzSchema,
    table: &str,
    corporation: RowUuid,
    member: RowUuid,
    idx: usize,
    inherited_parent: Option<(&str, RowUuid)>,
) -> BTreeMap<String, Value> {
    let table_schema = find_table(schema, table);
    let mut cells = BTreeMap::new();
    for column in &table_schema.columns {
        if matches!(column.column_type, ColumnType::Nullable(_))
            && !matches!(
                column.name.as_str(),
                "c146" | "c728" | "c733" | "c734" | "c735"
            )
        {
            continue;
        }
        let value = match column.name.as_str() {
            "c449" => Value::Uuid(corporation.0),
            "c450" | "c451" => Value::Uuid(member.0),
            "c452" => Value::Bool(false),
            "c142" => Value::String(format!("{table}-holder-{idx}")),
            "c453" | "c454" => Value::U64(1),
            "c146" => maybe_nullable(
                &column.column_type,
                Value::String("policy graph perf".to_owned()),
            ),
            "c724" if table == HEAVY_CHILD_TABLE => {
                Value::Uuid(inherited_parent.expect("heavy child parent").1.0)
            }
            name if inherited_parent.is_some_and(|(via, _)| via == name) => {
                Value::Uuid(inherited_parent.unwrap().1.0)
            }
            "c725" => Value::String("data_entry".to_owned()),
            "c726" => Value::String(format!("field_{idx}")),
            "c727" => Value::Array(vec![
                Value::String(format!("option_{idx}_a")),
                Value::String(format!("option_{idx}_b")),
            ]),
            "c728" => maybe_nullable(&column.column_type, Value::Bool(false)),
            "c729" => Value::Bool(true),
            "c730" => Value::Bool(idx.is_multiple_of(3)),
            "c731" | "c732" | "c734" | "c735" => {
                maybe_nullable(&column.column_type, Value::String("solid".to_owned()))
            }
            "c733" | "c488" => maybe_nullable(&column.column_type, Value::U32(idx as u32)),
            "c736" => Value::String("{}".to_owned()),
            _ => sample_value(&column.column_type, idx as u64, corporation, member),
        };
        cells.insert(column.name.clone(), value);
    }
    cells
}

fn sample_value(
    column_type: &ColumnType,
    seed: u64,
    corporation: RowUuid,
    member: RowUuid,
) -> Value {
    match column_type {
        ColumnType::U8 => Value::U8(seed as u8),
        ColumnType::U16 => Value::U16(seed as u16),
        ColumnType::U32 => Value::U32(seed as u32),
        ColumnType::U64 => Value::U64(seed + 1),
        ColumnType::F64 => Value::F64(seed as f64 + 0.5),
        ColumnType::Bool => Value::Bool(seed.is_multiple_of(2)),
        ColumnType::String => Value::String(format!("v{seed}")),
        ColumnType::Bytes => Value::Bytes(vec![seed as u8]),
        ColumnType::Uuid => Value::Uuid(if seed.is_multiple_of(2) {
            corporation.0
        } else {
            member.0
        }),
        ColumnType::Enum(_) => Value::Enum(0),
        ColumnType::Tuple(members) => Value::Tuple(
            members
                .iter()
                .enumerate()
                .map(|(idx, member_type)| {
                    sample_value(member_type, seed + idx as u64 + 1, corporation, member)
                })
                .collect(),
        ),
        ColumnType::Array(member_type) => Value::Array(vec![sample_value(
            member_type,
            seed + 1,
            corporation,
            member,
        )]),
        ColumnType::Nullable(member_type) => nullable(Some(sample_value(
            member_type,
            seed + 1,
            corporation,
            member,
        ))),
    }
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
    for table in subscription_tables() {
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
            opened_ms: None,
            materialized_ms: None,
        });
    }
    let subscribe_ms = subscribe_start.elapsed().as_millis();

    let apply_start = Instant::now();
    let mut server_open_bundle_ms = 0_u128;
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
        seeded.core.tick().expect("core tick");
        relay.db.tick().expect("relay tick");
        server_open_bundle_ms += server_start.elapsed().as_millis();

        let _queued_to_client = client_relay.right_inbound.borrow().len();
        client.db.tick().expect("client tick");
        drain_subscriptions(start, &mut subscriptions);
        ticks += 1;
    }
    let client_apply_ms = apply_start.elapsed().as_millis();

    let expected_count_start = Instant::now();
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
    let expected_count_ms = expected_count_start.elapsed().as_millis();

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
            opened_ms: sub.opened_ms.unwrap_or_default(),
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
        client_apply_ms,
        expected_count_ms,
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

fn subscription_tables() -> Vec<String> {
    let mut tables = HOLDER_TABLES
        .iter()
        .map(|table| (*table).to_owned())
        .collect::<Vec<_>>();
    tables.extend(
        INHERITS_TABLES
            .iter()
            .map(|(table, _, _)| (*table).to_owned()),
    );
    tables.push("t50".to_owned());
    tables.push("t188".to_owned());
    tables.sort();
    tables.dedup();
    assert_eq!(tables.len(), 39);
    tables
}

fn expected_counts() -> BTreeMap<String, usize> {
    subscription_tables()
        .into_iter()
        .map(|table| {
            let count = match table.as_str() {
                "t1" => 2,
                "t188" => 2,
                "t68" => HOLDER_PARENT_ROWS,
                HEAVY_CHILD_TABLE => HEAVY_CHILD_ROWS,
                _ => 1,
            };
            (table, count)
        })
        .collect()
}

fn drain_subscriptions(start: Instant, subscriptions: &mut [OpenSubscription]) {
    let elapsed = start.elapsed().as_millis();
    for sub in subscriptions {
        while let Some(event) = sub.stream.try_next_event() {
            apply_event(&mut sub.rows, event);
            if sub.opened_ms.is_none() {
                sub.opened_ms = Some(elapsed);
            }
            if sub.rows.len() == sub.expected && sub.materialized_ms.is_none() {
                sub.materialized_ms = Some(elapsed);
            }
        }
    }
}

fn apply_event(rows: &mut BTreeSet<RowUuid>, event: SubscriptionEvent) {
    match event {
        SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
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
        }
        SubscriptionEvent::Closed => {}
    }
}

fn emit_summary(config: &Config, session_id: &str, phase: &str, summary: &RunSummary) {
    let mut fields = metadata_fields("policy_graph_concurrent", "native", config.seed, "full");
    fields.insert("session_id".to_owned(), json!(session_id));
    fields.insert("phase".to_owned(), json!(phase));
    fields.insert("identity".to_owned(), json!(config.identity.label()));
    fields.insert("wall_ms".to_owned(), json!(summary.wall_ms));
    fields.insert(
        "settled_first_callback_all_ms".to_owned(),
        json!(summary.client_apply_ms),
    );
    fields.insert(
        "expected_count_all_ms".to_owned(),
        json!(summary.expected_count_ms),
    );
    fields.insert(
        "phase_breakdown".to_owned(),
        json!({
            "server_open_bundle_ms": summary.server_open_bundle_ms,
            "subscribe_ms": summary.subscribe_ms,
            "client_apply_ms": summary.client_apply_ms,
            "expected_count_ms": summary.expected_count_ms,
        }),
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
        json!("native schema fixture plus anonymized holder/access/inherits tokens; data is generated import-shaped rows with t67 as the 19894-row heavy child table"),
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
                        "opened_ms": timeline.opened_ms,
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

fn maybe_nullable(column_type: &ColumnType, value: Value) -> Value {
    if matches!(column_type, ColumnType::Nullable(_)) {
        nullable(Some(value))
    } else {
        value
    }
}

fn policy_graph_uuid(kind: u8, idx: u32) -> uuid::Uuid {
    let mut bytes = [kind; 16];
    bytes[12..].copy_from_slice(&idx.to_be_bytes());
    uuid::Uuid::from_bytes(bytes)
}

fn policy_graph_row(kind: u8, idx: u32) -> RowUuid {
    RowUuid(policy_graph_uuid(kind, idx))
}

fn policy_graph_author(kind: u8, idx: u32) -> AuthorId {
    AuthorId(policy_graph_uuid(kind, idx))
}

fn holder_kind(table_idx: usize) -> u8 {
    0x40_u8.wrapping_add(table_idx as u8)
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
                "{}={}/{} opened={:?}",
                sub.name,
                sub.rows.len(),
                sub.expected,
                sub.opened_ms
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
