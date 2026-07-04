use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;
use std::time::Instant;

use jazz::db::{
    Db, DbConfig, DbIdentity, Node, ReadOpts, SeededRowIdSource, SubscriptionEvent,
    SubscriptionStream, Transport,
};
use jazz::groove::records::{EnumSchema, Value};
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::{Durability, RocksDbStorage};
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::MergeableCommit;
use jazz::protocol::{SubscriptionKey, SyncMessage};
use jazz::query::{Query, col, eq, lit};
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::wire::TransportError;
use jazz_sim::{emit_json_line, metadata_fields};
use serde_json::{Value as JsonValue, json};

// Customer-shaped cold-start fixture. Child tables use the real customer
// semantics: child rows inherit read permission from their referenced parent
// resource via `inherits(parent_id)`. The generator constrains a small fixed
// subset of resource access edges to groups reached by the member at depth 1
// and depth 2 so every scale exercises the member->resource and
// member->child-inherits paths.

const ORG: &str = "org";
const GROUP: &str = "group";
const GROUP_ACCESS: &str = "group_access_edges";
const GROUP_ENTRY: &str = "group_entry";
const PROFILE: &str = "profile";
const CHILD_TABLES: usize = 6;

const RESOURCE_SPECS: [ResourceSpec; 14] = [
    ResourceSpec::new("res_a", 4, 7, Some(108)),
    ResourceSpec::new("res_b", 5, 14, None),
    ResourceSpec::new("res_c", 2, 3, None),
    ResourceSpec::new("res_d", 3, 3, Some(45)),
    ResourceSpec::new("res_e", 17, 46, None),
    ResourceSpec::new("res_f", 13, 23, None),
    ResourceSpec::new("res_g", 10, 22, None),
    ResourceSpec::new("res_h", 3, 12, Some(1)),
    ResourceSpec::new("res_i", 22, 69, None),
    ResourceSpec::new("res_j", 7, 24, None),
    ResourceSpec::new("res_k", 10, 45, None),
    ResourceSpec::new("res_l", 30, 35, Some(19_894)),
    ResourceSpec::new("res_m", 8, 10, None),
    ResourceSpec::new("res_n", 1, 2, None),
];

fn main() {
    let config = Config::from_env();
    let schema = schema();
    let seeded = seed_core(&schema, &config);
    let expected = expected_visible_counts(&seeded, config.identity);
    if config.identity == BenchIdentity::Member {
        assert_policy_active(&seeded, &expected);
    }

    if config.runs_phase("cold") {
        let cold = run_cold(&schema, &seeded, &expected, &config);
        emit_summary(&config, "cold", &cold);
    }

    if config.runs_phase("warm") {
        let warm = run_warm(&schema, &seeded, &expected, &config);
        emit_summary(&config, "warm", &warm);
    }
}

#[derive(Clone, Copy)]
struct ResourceSpec {
    table: &'static str,
    rows: usize,
    edges: usize,
    child_rows: Option<usize>,
}

impl ResourceSpec {
    const fn new(
        table: &'static str,
        rows: usize,
        edges: usize,
        child_rows: Option<usize>,
    ) -> Self {
        Self {
            table,
            rows,
            edges,
            child_rows,
        }
    }

    fn access_table(self) -> String {
        format!("{}_access_edges", self.table)
    }

    fn child_table(self, index: usize) -> String {
        format!("{}_child_{}", self.table, index)
    }
}

struct Config {
    seed: u64,
    scale: f64,
    max_ticks: usize,
    phases: Vec<String>,
    identity: BenchIdentity,
}

impl Config {
    fn from_env() -> Self {
        let identity = match std::env::var("JAZZ_CUSTOMER_IDENTITY")
            .unwrap_or_else(|_| "member".to_owned())
            .as_str()
        {
            "member" => BenchIdentity::Member,
            "spy" => BenchIdentity::Spy,
            "admin" => BenchIdentity::Admin,
            other => {
                panic!(
                    "unsupported JAZZ_CUSTOMER_IDENTITY {other:?}; supported: member, spy, admin"
                )
            }
        };
        Self {
            seed: env_u64("JAZZ_CUSTOMER_SEED", 0xC057_A271),
            scale: env_f64("JAZZ_CUSTOMER_SCALE", 1.0),
            max_ticks: env_usize("JAZZ_CUSTOMER_MAX_TICKS", 20_000),
            phases: std::env::var("JAZZ_CUSTOMER_PHASES")
                .unwrap_or_else(|_| "cold,warm".to_owned())
                .split(',')
                .map(str::trim)
                .filter(|phase| !phase.is_empty())
                .map(str::to_owned)
                .collect(),
            identity,
        }
    }

    fn runs_phase(&self, phase: &str) -> bool {
        self.phases.iter().any(|candidate| candidate == phase)
    }

    fn client_author(&self, seeded: &Seeded) -> AuthorId {
        match self.identity {
            BenchIdentity::Member => AuthorId(seeded.ordinary_user.0),
            BenchIdentity::Spy => AuthorId(row(9_999_999).0),
            BenchIdentity::Admin => AuthorId::SYSTEM,
        }
    }

    fn scaled_count(&self, count: usize) -> usize {
        ((count as f64 * self.scale).round() as usize).clamp(1, count.max(1))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BenchIdentity {
    Member,
    Spy,
    Admin,
}

struct Seeded {
    _core_dir: Rc<tempfile::TempDir>,
    core: Node<RocksDbStorage>,
    ordinary_user: RowUuid,
    visible_groups: BTreeSet<RowUuid>,
    table_rows: BTreeMap<String, Vec<RowUuid>>,
    access: BTreeMap<String, Vec<(RowUuid, RowUuid)>>,
    child_parent: BTreeMap<String, Vec<(RowUuid, RowUuid)>>,
}

struct RunSummary {
    wall_ms: u128,
    connect_ms: u128,
    subscribe_ms: u128,
    settle_ms: u128,
    materialize_ms: u128,
    ticks: usize,
    subscriptions: usize,
    rows_materialized: usize,
    expected_rows: usize,
    server_to_client_messages: u64,
    server_to_client_view_updates: u64,
    server_to_client_bytes: u64,
    client_to_relay_messages: u64,
    relay_to_core_messages: u64,
    known_state_declared: u64,
    relay_known_state_declared: u64,
    relay_receiver_bulk_bundle_ingests: u64,
    relay_receiver_per_bundle_ingests: u64,
    relay_receiver_bulk_ingest_commits: u64,
    client_receiver_bulk_bundle_ingests: u64,
    client_receiver_per_bundle_ingests: u64,
    client_receiver_bulk_ingest_commits: u64,
    peak_rss_bytes: u64,
    core_encoded_storage_bytes: u64,
    relay_encoded_storage_bytes: u64,
    client_encoded_storage_bytes: u64,
    encoded_storage_bytes: u64,
    memory_amplification: f64,
    slowest_subscription: String,
    slowest_subscription_ms: u128,
    served_view_updates: Vec<ViewUpdateSummary>,
    timelines: Vec<SubscriptionTimeline>,
}

#[derive(Clone, Default)]
struct ViewUpdateSummary {
    subscription: String,
    messages: u64,
    resets: u64,
    bundles: u64,
    reset_bundles: u64,
    non_reset_bundles: u64,
    result_adds: u64,
}

struct SubscriptionTimeline {
    name: String,
    rows: usize,
    expected: usize,
    opened_ms: u128,
    materialized_ms: u128,
}

struct DbNode {
    _dir: Rc<tempfile::TempDir>,
    db: Db<RocksDbStorage>,
}

struct DbClient {
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

#[derive(Default)]
struct TransportMetrics {
    messages: Cell<u64>,
    view_updates: Cell<u64>,
    bytes: Cell<u64>,
    known_state_subscribes: Cell<u64>,
    view_updates_by_subscription: RefCell<BTreeMap<SubscriptionKey, ViewUpdateSummary>>,
}

struct DuplexTransport {
    outbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    metrics: Rc<TransportMetrics>,
}

struct CountedDuplex {
    left_transport: Box<dyn Transport>,
    right_transport: Box<dyn Transport>,
    right_inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    left_to_right: Rc<TransportMetrics>,
    right_to_left: Rc<TransportMetrics>,
}

impl Transport for DuplexTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.metrics.messages.set(self.metrics.messages.get() + 1);
        if let SyncMessage::ViewUpdate {
            subscription,
            reset_result_set,
            version_bundles,
            result_member_adds,
            ..
        } = &message
        {
            self.metrics
                .view_updates
                .set(self.metrics.view_updates.get() + 1);
            let mut by_subscription = self.metrics.view_updates_by_subscription.borrow_mut();
            let entry = by_subscription
                .entry(*subscription)
                .or_insert_with(|| ViewUpdateSummary {
                    subscription: format!("{subscription:?}"),
                    ..ViewUpdateSummary::default()
                });
            entry.messages += 1;
            entry.resets += u64::from(*reset_result_set);
            let bundles = version_bundles.len() as u64;
            entry.bundles += bundles;
            if *reset_result_set {
                entry.reset_bundles += bundles;
            } else {
                entry.non_reset_bundles += bundles;
            }
            entry.result_adds += result_member_adds.len() as u64;
        }
        if let SyncMessage::Subscribe(subscribe) = &message {
            if subscribe.known_state.is_some() {
                self.metrics
                    .known_state_subscribes
                    .set(self.metrics.known_state_subscribes.get() + 1);
            }
        }
        self.metrics
            .bytes
            .set(self.metrics.bytes.get() + encoded_message_len(&message));
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
        left_transport: Box::new(DuplexTransport {
            outbound: Rc::clone(&left),
            inbound: Rc::clone(&right),
            metrics: Rc::clone(&left_to_right),
        }),
        right_transport: Box::new(DuplexTransport {
            outbound: Rc::clone(&right),
            inbound: Rc::clone(&left),
            metrics: Rc::clone(&right_to_left),
        }),
        right_inbound: right,
        left_to_right,
        right_to_left,
    }
}

fn schema() -> JazzSchema {
    let mut tables = Vec::new();
    tables.push(TableSchema::new(
        ORG,
        [
            ColumnSchema::new("label", ColumnType::String),
            ColumnSchema::new("created_at", ColumnType::U64),
            ColumnSchema::new("settings", ColumnType::String),
        ],
    ));
    tables.push(
        TableSchema::new(
            GROUP,
            [
                ColumnSchema::new("org_id", ColumnType::Uuid),
                ColumnSchema::new("label", ColumnType::String),
                ColumnSchema::new("description", ColumnType::String.nullable()),
                ColumnSchema::new("archived", ColumnType::Bool),
                ColumnSchema::new("sort", ColumnType::U64),
            ],
        )
        .with_reference("org_id", ORG),
    );
    tables.push(
        TableSchema::new(
            GROUP_ACCESS,
            [
                ColumnSchema::new("group_id", ColumnType::Uuid),
                ColumnSchema::new("user_id", ColumnType::Uuid),
                ColumnSchema::new("role", role_type("group_access_role")),
            ],
        )
        .with_reference("group_id", GROUP),
    );
    tables.push(
        TableSchema::new(
            GROUP_ENTRY,
            [
                ColumnSchema::new("member_id", ColumnType::Uuid),
                ColumnSchema::new("target_id", ColumnType::Uuid),
                ColumnSchema::new("administrator", ColumnType::Bool),
                ColumnSchema::new("date_added", ColumnType::U64),
            ],
        )
        .with_reference("member_id", GROUP)
        .with_reference("target_id", GROUP),
    );
    tables.push(
        TableSchema::new(
            PROFILE,
            [
                ColumnSchema::new("group_id", ColumnType::Uuid),
                ColumnSchema::new("email", ColumnType::String),
                ColumnSchema::new("display", ColumnType::String),
                ColumnSchema::new("last_login", ColumnType::U64.nullable()),
                ColumnSchema::new("prefs", ColumnType::String),
            ],
        )
        .with_reference("group_id", GROUP),
    );

    let mut child_slot = 0;
    for spec in RESOURCE_SPECS {
        let policy = resource_policy(spec.table, &spec.access_table());
        tables.push(
            TableSchema::new(spec.table, resource_columns())
                .with_reference("org_id", ORG)
                .with_reference("created_by", GROUP)
                .with_reference("updated_by", GROUP)
                .with_read_policy(policy),
        );
        tables.push(
            TableSchema::new(
                spec.access_table(),
                [
                    ColumnSchema::new("resource", ColumnType::Uuid),
                    ColumnSchema::new("team", ColumnType::Uuid),
                    ColumnSchema::new("grant_role", role_type(&format!("{}_role", spec.table))),
                    ColumnSchema::new("administrator", ColumnType::Bool),
                ],
            )
            .with_reference("resource", spec.table)
            .with_reference("team", GROUP),
        );
        if spec.child_rows.is_some() {
            let table = spec.child_table(child_slot);
            child_slot += 1;
            tables.push(
                TableSchema::new(
                    &table,
                    [
                        ColumnSchema::new("parent_id", ColumnType::Uuid),
                        ColumnSchema::new("label", ColumnType::String),
                        ColumnSchema::new("value_text", ColumnType::String),
                        ColumnSchema::new("value_json", ColumnType::String),
                        ColumnSchema::new("sort", ColumnType::U64),
                    ],
                )
                .with_reference("parent_id", spec.table)
                .with_read_policy(Policy::shape(
                    Query::from(table.as_str()).inherits("parent_id"),
                )),
            );
        }
    }
    while child_slot < CHILD_TABLES {
        let table = format!("empty_child_{child_slot}");
        tables.push(TableSchema::new(
            &table,
            [
                ColumnSchema::new("parent_id", ColumnType::Uuid),
                ColumnSchema::new("label", ColumnType::String),
                ColumnSchema::new("value_text", ColumnType::String),
                ColumnSchema::new("value_json", ColumnType::String),
                ColumnSchema::new("sort", ColumnType::U64),
            ],
        ));
        child_slot += 1;
    }
    JazzSchema::new(tables)
}

fn resource_columns() -> [ColumnSchema; 13] {
    [
        ColumnSchema::new("org_id", ColumnType::Uuid),
        ColumnSchema::new("created_by", ColumnType::Uuid),
        ColumnSchema::new("updated_by", ColumnType::Uuid),
        ColumnSchema::new("archived", ColumnType::Bool),
        ColumnSchema::new("label", ColumnType::String),
        ColumnSchema::new("date_created", ColumnType::U64),
        ColumnSchema::new("date_updated", ColumnType::U64),
        ColumnSchema::new("col_text_a", ColumnType::String.nullable()),
        ColumnSchema::new("col_text_b", ColumnType::String.nullable()),
        ColumnSchema::new("col_float", ColumnType::F64.nullable()),
        ColumnSchema::new("col_int", ColumnType::U64.nullable()),
        ColumnSchema::new("col_json", ColumnType::String.nullable()),
        ColumnSchema::new("col_tags", ColumnType::String.nullable()),
    ]
}

fn role_type(name: &str) -> ColumnType {
    ColumnType::Enum(EnumSchema::new(name, ["viewer", "editor", "manager"]).unwrap())
}

fn resource_policy(table: &str, access_table: &str) -> Option<Query> {
    Policy::shape(
        Query::from(table)
            .reachable_via_with_access_filters(
                access_table,
                "resource",
                "team",
                lit("relation-seeded"),
                [eq(col("administrator"), lit(false))],
                GROUP_ENTRY,
                "member_id",
                "target_id",
                [eq(col("administrator"), lit(false))],
            )
            .seeded_by(GROUP_ACCESS, "user_id", "sub", "group_id"),
    )
}

fn seed_core(schema: &JazzSchema, config: &Config) -> Seeded {
    let (core_dir, core) = open_node(node(1), schema.clone());
    let org = row(1);
    seed_db(&core, ORG, org, org_cells());

    let mut groups = Vec::new();
    for i in 0..38 {
        let group = row(1_000 + i as u64);
        groups.push(group);
        seed_db(&core, GROUP, group, group_cells(org, i));
    }
    let ordinary_user = groups[0];
    for i in 0..21 {
        seed_db(
            &core,
            PROFILE,
            row(2_000 + i as u64),
            profile_cells(groups[i % groups.len()], i),
        );
    }

    let mut group_edges = Vec::new();
    for i in 0..34 {
        let group = if i < 24 { groups[i] } else { groups[i - 24] };
        seed_db(
            &core,
            GROUP_ACCESS,
            row(3_000 + i as u64),
            group_access_cells(group, ordinary_user, i),
        );
        group_edges.push((group, ordinary_user));
    }

    let mut group_entries = Vec::new();
    for i in 0..42 {
        let (member_index, target_index) = if i < 10 {
            (i, 24 + i)
        } else if i < 18 {
            (i - 10, 26 + (i - 10))
        } else if i < 24 {
            // A few shallow transitive chains; max depth stays below the
            // public v0 reachable default while still exercising recursion.
            (24 + (i - 18), 30 + (i - 18) % 4)
        } else {
            (i % 24, 24 + (i % 10))
        };
        let member = groups[member_index];
        let target = groups[target_index];
        seed_db(
            &core,
            GROUP_ENTRY,
            row(4_000 + i as u64),
            group_entry_cells(member, target, i),
        );
        group_entries.push((member, target));
    }
    let visible_groups = reachable_groups(ordinary_user, &group_edges, &group_entries);

    let mut table_rows = BTreeMap::<String, Vec<RowUuid>>::new();
    table_rows.insert(ORG.to_owned(), vec![org]);
    table_rows.insert(GROUP.to_owned(), groups.clone());
    table_rows.insert(
        GROUP_ACCESS.to_owned(),
        (0..34).map(|i| row(3_000 + i)).collect(),
    );
    table_rows.insert(
        GROUP_ENTRY.to_owned(),
        (0..42).map(|i| row(4_000 + i)).collect(),
    );
    table_rows.insert(
        PROFILE.to_owned(),
        (0..21).map(|i| row(2_000 + i)).collect(),
    );

    let mut access = BTreeMap::<String, Vec<(RowUuid, RowUuid)>>::new();
    let mut child_parent = BTreeMap::<String, Vec<(RowUuid, RowUuid)>>::new();
    let mut child_slot = 0_usize;
    let mut resource_base = 10_000_u64;
    let mut access_base = 100_000_u64;
    for (kind, spec) in RESOURCE_SPECS.iter().copied().enumerate() {
        let resource_count = if spec.child_rows.is_some() {
            spec.rows
        } else {
            config.scaled_count(spec.rows)
        };
        let edge_count = config.scaled_count(spec.edges);
        let resource_rows = (0..resource_count)
            .map(|i| row(resource_base + i as u64))
            .collect::<Vec<_>>();
        for (i, resource) in resource_rows.iter().copied().enumerate() {
            seed_db(
                &core,
                spec.table,
                resource,
                resource_cells(org, groups[i % 34], i),
            );
        }
        table_rows.insert(spec.table.to_owned(), resource_rows.clone());

        let mut edges = Vec::new();
        for i in 0..edge_count {
            let resource = resource_rows[i % resource_rows.len()];
            let group = resource_access_group(spec, kind, i, &groups);
            seed_db(
                &core,
                &spec.access_table(),
                row(access_base + i as u64),
                resource_access_cells(resource, group, i),
            );
            edges.push((resource, group));
        }
        table_rows.insert(
            spec.access_table(),
            (0..edge_count)
                .map(|i| row(access_base + i as u64))
                .collect(),
        );
        if let Some(children) = spec.child_rows {
            let child_table = spec.child_table(child_slot);
            let distribution = child_counts(config.scaled_count(children), resource_rows.len());
            let mut rows = Vec::new();
            let mut parents = Vec::new();
            let mut idx = 0_u64;
            for (parent_index, count) in distribution.into_iter().enumerate() {
                for _ in 0..count {
                    let child = row(500_000 + (child_slot as u64 * 100_000) + idx);
                    let parent = resource_rows[parent_index % resource_rows.len()];
                    seed_db(
                        &core,
                        &child_table,
                        child,
                        child_cells(parent, idx as usize, child_slot),
                    );
                    rows.push(child);
                    parents.push((child, parent));
                    idx += 1;
                }
            }
            table_rows.insert(child_table.clone(), rows);
            child_parent.insert(child_table, parents);
            child_slot += 1;
        }
        access.insert(spec.table.to_owned(), edges);
        resource_base += 10_000;
        access_base += 10_000;
    }
    while child_slot < CHILD_TABLES {
        let child_table = format!("empty_child_{child_slot}");
        table_rows.insert(child_table, Vec::new());
        child_slot += 1;
    }

    Seeded {
        _core_dir: Rc::new(core_dir),
        core,
        ordinary_user,
        visible_groups,
        table_rows,
        access,
        child_parent,
    }
}

fn resource_access_group(
    spec: ResourceSpec,
    kind: usize,
    edge_index: usize,
    groups: &[RowUuid],
) -> RowUuid {
    match (spec.table, edge_index) {
        // Direct member group: keeps at least one parent-visible child-bearing
        // resource at every scale.
        ("res_a", 0) => groups[1],
        // Transitive member group reached through group_entry: exercises the
        // recursive policy path even at the smallest scale.
        ("res_l", 0) => groups[24],
        // Another direct visible resource kind without children so the member
        // slice is not child-only.
        ("res_e", 0) => groups[2],
        _ if spec.table == "res_n" || edge_index % 5 == 0 => groups[34 + (edge_index % 4)],
        _ => groups[(edge_index + kind) % 34],
    }
}

fn expected_visible_counts(seeded: &Seeded, identity: BenchIdentity) -> BTreeMap<String, usize> {
    let mut out = BTreeMap::new();
    out.insert(ORG.to_owned(), seeded.table_rows[ORG].len());
    out.insert(GROUP.to_owned(), seeded.table_rows[GROUP].len());
    out.insert(
        GROUP_ACCESS.to_owned(),
        seeded.table_rows[GROUP_ACCESS].len(),
    );
    out.insert(GROUP_ENTRY.to_owned(), seeded.table_rows[GROUP_ENTRY].len());
    out.insert(PROFILE.to_owned(), 21);
    for spec in RESOURCE_SPECS {
        let visible_resources = match identity {
            BenchIdentity::Member => seeded
                .access
                .get(spec.table)
                .into_iter()
                .flatten()
                .filter_map(|(resource, group)| {
                    seeded.visible_groups.contains(group).then_some(*resource)
                })
                .collect::<BTreeSet<_>>(),
            BenchIdentity::Spy => BTreeSet::new(),
            BenchIdentity::Admin => seeded.table_rows[spec.table].iter().copied().collect(),
        };
        out.insert(spec.table.to_owned(), visible_resources.len());
        out.insert(
            spec.access_table(),
            seeded.table_rows[&spec.access_table()].len(),
        );
    }
    let mut child_slot = 0;
    for spec in RESOURCE_SPECS {
        if spec.child_rows.is_some() {
            let child_table = spec.child_table(child_slot);
            let visible_resources = match identity {
                BenchIdentity::Member => seeded
                    .access
                    .get(spec.table)
                    .into_iter()
                    .flatten()
                    .filter_map(|(resource, group)| {
                        seeded.visible_groups.contains(group).then_some(*resource)
                    })
                    .collect::<BTreeSet<_>>(),
                BenchIdentity::Spy => BTreeSet::new(),
                BenchIdentity::Admin => seeded.table_rows[spec.table].iter().copied().collect(),
            };
            let visible_children = seeded
                .child_parent
                .get(&child_table)
                .into_iter()
                .flatten()
                .filter(|(_child, parent)| visible_resources.contains(parent))
                .count();
            out.insert(child_table, visible_children);
            child_slot += 1;
        }
    }
    for slot in 0..CHILD_TABLES {
        out.entry(format!("empty_child_{slot}")).or_insert(0);
    }
    out
}

fn assert_policy_active(seeded: &Seeded, expected: &BTreeMap<String, usize>) {
    let mut hidden = 0_usize;
    for spec in RESOURCE_SPECS {
        hidden += seeded.table_rows[spec.table].len() - expected[spec.table];
    }
    assert!(hidden > 0, "ordinary identity must not see every resource");
}

fn run_cold(
    schema: &JazzSchema,
    seeded: &Seeded,
    expected: &BTreeMap<String, usize>,
    config: &Config,
) -> RunSummary {
    let relay = open_db_node(
        node(2),
        schema.clone(),
        AuthorId::SYSTEM,
        Some(Rc::new(tempfile::tempdir().unwrap())),
    );
    let client = open_client_db(node(3), schema.clone(), config.client_author(seeded), None);
    run_connect_and_subscribe("cold", seeded, relay, client, expected, config)
}

fn run_warm(
    schema: &JazzSchema,
    seeded: &Seeded,
    expected: &BTreeMap<String, usize>,
    config: &Config,
) -> RunSummary {
    let relay_dir = Rc::new(tempfile::tempdir().unwrap());
    let relay = open_db_node(
        node(4),
        schema.clone(),
        AuthorId::SYSTEM,
        Some(Rc::clone(&relay_dir)),
    );
    let client = open_client_db(node(5), schema.clone(), config.client_author(seeded), None);
    let mut first =
        run_connect_and_subscribe("warm_prime", seeded, relay, client, expected, config);
    assert_eq!(first.rows_materialized, first.expected_rows);
    drop(first);

    let relay = open_db_node(
        node(4),
        schema.clone(),
        AuthorId::SYSTEM,
        Some(Rc::clone(&relay_dir)),
    );
    let client = open_client_db(node(5), schema.clone(), config.client_author(seeded), None);
    first = run_connect_and_subscribe("warm", seeded, relay, client, expected, config);
    assert!(
        first.relay_known_state_declared > 0,
        "warm relay reconnect must declare known-state to core"
    );
    first
}

fn run_connect_and_subscribe(
    label: &str,
    seeded: &Seeded,
    relay: DbNode,
    client: DbClient,
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
    let _relay_sub = relay
        .db
        .accept_subscriber(client_relay.right_transport, config.client_author(seeded));
    let connect_ms = start.elapsed().as_millis();

    let subscribe_start = Instant::now();
    let mut subscriptions = Vec::new();
    for table in subscription_tables() {
        let query = Query::from(table.as_str());
        let prepared = client
            .db
            .prepare_query(&query)
            .unwrap_or_else(|error| panic!("prepare {table} failed: {error}"));
        let stream = block_on(client.db.subscribe(&prepared, ReadOpts::default()))
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

    let settle_start = Instant::now();
    let mut ticks = 0_usize;
    while !subscriptions
        .iter()
        .all(|sub| sub.materialized_ms.is_some() && sub.rows.len() == sub.expected)
    {
        if ticks >= config.max_ticks {
            let group_entry_query = client.db.prepare_query(&Query::from(GROUP_ENTRY)).unwrap();
            let group_entry_one_shot =
                block_on(client.db.all(&group_entry_query, ReadOpts::default()))
                    .map(|rows| rows.len())
                    .map_err(|error| error.to_string());
            let relay_group_entry_query =
                relay.db.prepare_query(&Query::from(GROUP_ENTRY)).unwrap();
            let relay_group_entry_one_shot =
                block_on(relay.db.all(&relay_group_entry_query, ReadOpts::default()))
                    .map(|rows| rows.len())
                    .map_err(|error| error.to_string());
            panic!(
                "timed out settling subscriptions; {}; group_entry_one_shot={group_entry_one_shot:?}; relay_group_entry_one_shot={relay_group_entry_one_shot:?}",
                pending_description(&subscriptions)
            );
        }
        let trace_ticks = std::env::var_os("JAZZ_CUSTOMER_TRACE_TICKS").is_some();
        let before_core_to_relay = relay_core.right_to_left.messages.get();
        seeded.core.tick().unwrap();
        let after_core_to_relay = relay_core.right_to_left.messages.get();
        let relay_inbound_before = relay_core.right_inbound.borrow().len();
        let relay_to_core_before = relay_core.left_to_right.messages.get();
        let relay_to_client_before = client_relay.right_to_left.messages.get();
        relay.db.tick().unwrap();
        let relay_to_core_after = relay_core.left_to_right.messages.get();
        let relay_to_client_after = client_relay.right_to_left.messages.get();
        let client_inbound_before = client_relay.right_inbound.borrow().len();
        let client_to_relay_before = client_relay.left_to_right.messages.get();
        client.db.tick().unwrap();
        let client_to_relay_after = client_relay.left_to_right.messages.get();
        if trace_ticks {
            eprintln!(
                "CUSTOMER_TICK tick={ticks} core_sent_to_relay={} relay_inbound_before={} relay_sent_to_core={} relay_sent_to_client={} client_inbound_before={} client_sent_to_relay={}",
                after_core_to_relay.saturating_sub(before_core_to_relay),
                relay_inbound_before,
                relay_to_core_after.saturating_sub(relay_to_core_before),
                relay_to_client_after.saturating_sub(relay_to_client_before),
                client_inbound_before,
                client_to_relay_after.saturating_sub(client_to_relay_before),
            );
        }
        drain_subscriptions(start, &mut subscriptions);
        ticks += 1;
    }
    let settle_ms = settle_start.elapsed().as_millis();
    if label == "warm" {
        // Warm readiness is relay-local, but the benchmark also asserts that
        // the hot relay declares known state when it reconnects upstream. Drive
        // one post-readiness relay/core cycle so the queued coverage subscribe
        // reaches the core without changing the client readiness condition.
        relay.db.tick().unwrap();
        seeded.core.tick().unwrap();
    }
    let materialize_start = Instant::now();
    for sub in &subscriptions {
        let prepared = client
            .db
            .prepare_query(&Query::from(sub.name.as_str()))
            .unwrap();
        let rows = block_on(client.db.all(&prepared, ReadOpts::default())).unwrap();
        assert_eq!(
            rows.len(),
            sub.expected,
            "materialized one-shot count mismatch for {}",
            sub.name
        );
    }
    let materialize_ms = materialize_start.elapsed().as_millis();

    let rows_materialized = subscriptions
        .iter()
        .map(|sub| sub.rows.len())
        .sum::<usize>();
    if label == "warm_prime" {
        relay
            .db
            .flush_for_test()
            .expect("warm-prime relay state should flush before reopen");
    }
    let expected_rows = expected.values().sum::<usize>();
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
        .unwrap();
    let relay_sync_metrics = relay.db.sync_metrics_for_test();
    let client_sync_metrics = client.db.sync_metrics_for_test();
    let core_encoded_storage_bytes = seeded.core.encoded_storage_bytes_for_test().unwrap();
    let relay_encoded_storage_bytes = relay.db.encoded_storage_bytes_for_test().unwrap();
    let client_encoded_storage_bytes = client.db.encoded_storage_bytes_for_test().unwrap();
    let encoded_storage_bytes =
        core_encoded_storage_bytes + relay_encoded_storage_bytes + client_encoded_storage_bytes;
    let peak_rss_bytes = peak_rss_bytes();
    let memory_amplification = if encoded_storage_bytes == 0 {
        0.0
    } else {
        peak_rss_bytes as f64 / encoded_storage_bytes as f64
    };
    RunSummary {
        wall_ms: start.elapsed().as_millis(),
        connect_ms,
        subscribe_ms,
        settle_ms,
        materialize_ms,
        ticks,
        subscriptions: timelines.len(),
        rows_materialized,
        expected_rows,
        server_to_client_messages: client_relay.right_to_left.messages.get(),
        server_to_client_view_updates: client_relay.right_to_left.view_updates.get(),
        server_to_client_bytes: client_relay.right_to_left.bytes.get(),
        client_to_relay_messages: client_relay.left_to_right.messages.get(),
        relay_to_core_messages: relay_core.left_to_right.messages.get(),
        known_state_declared: client_relay.left_to_right.known_state_subscribes.get(),
        relay_known_state_declared: relay_core.left_to_right.known_state_subscribes.get(),
        relay_receiver_bulk_bundle_ingests: relay_sync_metrics.receiver_bulk_bundle_ingests,
        relay_receiver_per_bundle_ingests: relay_sync_metrics.receiver_per_bundle_ingests,
        relay_receiver_bulk_ingest_commits: relay_sync_metrics.receiver_bulk_ingest_commits,
        client_receiver_bulk_bundle_ingests: client_sync_metrics.receiver_bulk_bundle_ingests,
        client_receiver_per_bundle_ingests: client_sync_metrics.receiver_per_bundle_ingests,
        client_receiver_bulk_ingest_commits: client_sync_metrics.receiver_bulk_ingest_commits,
        peak_rss_bytes,
        core_encoded_storage_bytes,
        relay_encoded_storage_bytes,
        client_encoded_storage_bytes,
        encoded_storage_bytes,
        memory_amplification,
        slowest_subscription: slowest.name.clone(),
        slowest_subscription_ms: slowest.materialized_ms,
        served_view_updates: summarized_view_updates(&client_relay.right_to_left),
        timelines,
    }
}

fn summarized_view_updates(metrics: &TransportMetrics) -> Vec<ViewUpdateSummary> {
    let mut summaries = metrics
        .view_updates_by_subscription
        .borrow()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    summaries.sort_by(|left, right| {
        right
            .messages
            .cmp(&left.messages)
            .then_with(|| right.bundles.cmp(&left.bundles))
            .then_with(|| left.subscription.cmp(&right.subscription))
    });
    summaries
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
        SubscriptionEvent::Opened { current, .. } | SubscriptionEvent::Reset { current, .. } => {
            *rows = current.rows.into_iter().map(|row| row.row_uuid()).collect();
        }
        SubscriptionEvent::Delta {
            added,
            updated,
            removed,
            ..
        } => {
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

fn subscription_tables() -> Vec<String> {
    let mut tables = vec![
        ORG.to_owned(),
        GROUP.to_owned(),
        GROUP_ACCESS.to_owned(),
        GROUP_ENTRY.to_owned(),
        PROFILE.to_owned(),
    ];
    let mut child_slot = 0;
    for spec in RESOURCE_SPECS {
        tables.push(spec.table.to_owned());
        tables.push(spec.access_table());
        if spec.child_rows.is_some() {
            tables.push(spec.child_table(child_slot));
            child_slot += 1;
        }
    }
    while child_slot < CHILD_TABLES {
        tables.push(format!("empty_child_{child_slot}"));
        child_slot += 1;
    }
    assert_eq!(tables.len(), 39);
    tables
}

fn seed_db(core: &Node<RocksDbStorage>, table: &str, row: RowUuid, cells: BTreeMap<String, Value>) {
    let node = core.node();
    let tx_id = node
        .borrow_mut()
        .commit_mergeable(
            MergeableCommit::new(table, row, next_seed_time())
                .made_by(AuthorId::SYSTEM)
                .cells(cells),
        )
        .unwrap();
    node.borrow_mut()
        .finalize_local_mergeable_commit(tx_id)
        .unwrap();
}

fn open_node(node_uuid: NodeUuid, schema: JazzSchema) -> (tempfile::TempDir, Node<RocksDbStorage>) {
    let dir = tempfile::tempdir().unwrap();
    let storage = open_storage(dir.path(), &schema);
    let state = jazz::node::NodeState::new_history_complete(node_uuid, schema, storage).unwrap();
    (dir, Node::new(state))
}

fn open_db_node(
    node_uuid: NodeUuid,
    schema: JazzSchema,
    author: AuthorId,
    dir: Option<Rc<tempfile::TempDir>>,
) -> DbNode {
    let dir = dir.unwrap_or_else(|| Rc::new(tempfile::tempdir().unwrap()));
    let storage = open_storage(dir.path(), &schema);
    let db = block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: node_uuid,
            author,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(node_uuid_seed(node_uuid)))),
        large_value_checkpoint_op_interval: 1024,
    }))
    .unwrap();
    DbNode { _dir: dir, db }
}

fn open_client_db(
    node_uuid: NodeUuid,
    schema: JazzSchema,
    author: AuthorId,
    dir: Option<Rc<tempfile::TempDir>>,
) -> DbClient {
    let dir = dir.unwrap_or_else(|| Rc::new(tempfile::tempdir().unwrap()));
    let storage = open_storage(dir.path(), &schema);
    let db = block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: node_uuid,
            author,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(node_uuid_seed(node_uuid)))),
        large_value_checkpoint_op_interval: 1024,
    }))
    .unwrap();
    DbClient { _dir: dir, db }
}

fn open_storage(path: &std::path::Path, schema: &JazzSchema) -> RocksDbStorage {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    RocksDbStorage::open_with_durability(path, &refs, Durability::WalNoSync).unwrap()
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let std::task::Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value;
        }
    }
}

fn org_cells() -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("label".to_owned(), Value::String(sized_string("org", 24))),
        ("created_at".to_owned(), Value::U64(1)),
        ("settings".to_owned(), Value::String(sized_json(128))),
    ])
}

fn group_cells(org: RowUuid, i: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("org_id".to_owned(), Value::Uuid(org.0)),
        ("label".to_owned(), Value::String(sized_string("group", 32))),
        (
            "description".to_owned(),
            Value::Nullable(Some(Box::new(Value::String(sized_string("desc", 96))))),
        ),
        ("archived".to_owned(), Value::Bool(false)),
        ("sort".to_owned(), Value::U64(i as u64)),
    ])
}

fn group_access_cells(group: RowUuid, user: RowUuid, i: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("group_id".to_owned(), Value::Uuid(group.0)),
        ("user_id".to_owned(), Value::Uuid(user.0)),
        ("role".to_owned(), Value::Enum((i % 3) as u8)),
    ])
}

fn group_entry_cells(member: RowUuid, target: RowUuid, i: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("member_id".to_owned(), Value::Uuid(member.0)),
        ("target_id".to_owned(), Value::Uuid(target.0)),
        ("administrator".to_owned(), Value::Bool(false)),
        ("date_added".to_owned(), Value::U64(10_000 + i as u64)),
    ])
}

fn profile_cells(group: RowUuid, i: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("group_id".to_owned(), Value::Uuid(group.0)),
        (
            "email".to_owned(),
            Value::String(format!("user-{i}@example.invalid")),
        ),
        (
            "display".to_owned(),
            Value::String(sized_string("profile", 18)),
        ),
        (
            "last_login".to_owned(),
            Value::Nullable(Some(Box::new(Value::U64(1_000_000 + i as u64)))),
        ),
        ("prefs".to_owned(), Value::String(sized_json(96))),
    ])
}

fn resource_cells(org: RowUuid, group: RowUuid, i: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("org_id".to_owned(), Value::Uuid(org.0)),
        ("created_by".to_owned(), Value::Uuid(group.0)),
        ("updated_by".to_owned(), Value::Uuid(group.0)),
        ("archived".to_owned(), Value::Bool(false)),
        (
            "label".to_owned(),
            Value::String(sized_string("resource", 40)),
        ),
        ("date_created".to_owned(), Value::U64(100_000 + i as u64)),
        ("date_updated".to_owned(), Value::U64(200_000 + i as u64)),
        (
            "col_text_a".to_owned(),
            Value::Nullable(Some(Box::new(Value::String(sized_string("text_a", 80))))),
        ),
        (
            "col_text_b".to_owned(),
            Value::Nullable(Some(Box::new(Value::String(sized_string("text_b", 44))))),
        ),
        (
            "col_float".to_owned(),
            Value::Nullable(Some(Box::new(Value::F64(i as f64 * 1.25)))),
        ),
        (
            "col_int".to_owned(),
            Value::Nullable(Some(Box::new(Value::U64(i as u64)))),
        ),
        (
            "col_json".to_owned(),
            Value::Nullable(Some(Box::new(Value::String(sized_json(160))))),
        ),
        (
            "col_tags".to_owned(),
            Value::Nullable(Some(Box::new(Value::String(sized_json(96))))),
        ),
    ])
}

fn resource_access_cells(resource: RowUuid, group: RowUuid, i: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("resource".to_owned(), Value::Uuid(resource.0)),
        ("team".to_owned(), Value::Uuid(group.0)),
        ("grant_role".to_owned(), Value::Enum((i % 3) as u8)),
        ("administrator".to_owned(), Value::Bool(false)),
    ])
}

fn child_cells(parent: RowUuid, i: usize, slot: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("parent_id".to_owned(), Value::Uuid(parent.0)),
        ("label".to_owned(), Value::String(sized_string("child", 32))),
        (
            "value_text".to_owned(),
            Value::String(sized_string("value", 72)),
        ),
        (
            "value_json".to_owned(),
            Value::String(sized_json(128 + slot * 8)),
        ),
        ("sort".to_owned(), Value::U64(i as u64)),
    ])
}

fn reachable_groups(
    user: RowUuid,
    direct: &[(RowUuid, RowUuid)],
    entries: &[(RowUuid, RowUuid)],
) -> BTreeSet<RowUuid> {
    let mut groups = direct
        .iter()
        .filter_map(|(group, direct_user)| (*direct_user == user).then_some(*group))
        .collect::<BTreeSet<_>>();
    loop {
        let before = groups.len();
        for (member, target) in entries {
            if groups.contains(member) {
                groups.insert(*target);
            }
        }
        if groups.len() == before {
            break;
        }
    }
    groups
}

fn child_counts(total: usize, parents: usize) -> Vec<usize> {
    if parents == 0 {
        return Vec::new();
    }
    let mut counts = vec![total / parents; parents];
    for i in 0..(total % parents) {
        counts[i] += 1;
    }
    counts
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

fn emit_summary(config: &Config, phase: &str, summary: &RunSummary) {
    let mut fields = metadata_fields("customer_cold_start", "native", config.seed, "full");
    fields.insert("phase".to_owned(), json!(phase));
    fields.insert("scale".to_owned(), json!(config.scale));
    fields.insert("wall_ms".to_owned(), json!(summary.wall_ms));
    fields.insert("target_ms".to_owned(), json!(1000));
    fields.insert("under_target".to_owned(), json!(summary.wall_ms < 1000));
    fields.insert("connect_ms".to_owned(), json!(summary.connect_ms));
    fields.insert("subscribe_ms".to_owned(), json!(summary.subscribe_ms));
    fields.insert("settle_ms".to_owned(), json!(summary.settle_ms));
    fields.insert("materialize_ms".to_owned(), json!(summary.materialize_ms));
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
    fields.insert(
        "server_to_client_bytes".to_owned(),
        json!(summary.server_to_client_bytes),
    );
    fields.insert(
        "client_to_relay_messages".to_owned(),
        json!(summary.client_to_relay_messages),
    );
    fields.insert(
        "relay_to_core_messages".to_owned(),
        json!(summary.relay_to_core_messages),
    );
    fields.insert(
        "known_state_declared".to_owned(),
        json!(summary.known_state_declared),
    );
    fields.insert(
        "relay_known_state_declared".to_owned(),
        json!(summary.relay_known_state_declared),
    );
    fields.insert(
        "relay_receiver_bulk_bundle_ingests".to_owned(),
        json!(summary.relay_receiver_bulk_bundle_ingests),
    );
    fields.insert(
        "relay_receiver_per_bundle_ingests".to_owned(),
        json!(summary.relay_receiver_per_bundle_ingests),
    );
    fields.insert(
        "relay_receiver_bulk_ingest_commits".to_owned(),
        json!(summary.relay_receiver_bulk_ingest_commits),
    );
    fields.insert(
        "client_receiver_bulk_bundle_ingests".to_owned(),
        json!(summary.client_receiver_bulk_bundle_ingests),
    );
    fields.insert(
        "client_receiver_per_bundle_ingests".to_owned(),
        json!(summary.client_receiver_per_bundle_ingests),
    );
    fields.insert(
        "client_receiver_bulk_ingest_commits".to_owned(),
        json!(summary.client_receiver_bulk_ingest_commits),
    );
    fields.insert("peak_rss_bytes".to_owned(), json!(summary.peak_rss_bytes));
    fields.insert(
        "core_encoded_storage_bytes".to_owned(),
        json!(summary.core_encoded_storage_bytes),
    );
    fields.insert(
        "relay_encoded_storage_bytes".to_owned(),
        json!(summary.relay_encoded_storage_bytes),
    );
    fields.insert(
        "client_encoded_storage_bytes".to_owned(),
        json!(summary.client_encoded_storage_bytes),
    );
    fields.insert(
        "encoded_storage_bytes".to_owned(),
        json!(summary.encoded_storage_bytes),
    );
    fields.insert(
        "memory_amplification".to_owned(),
        json!(summary.memory_amplification),
    );
    fields.insert(
        "slowest_subscription".to_owned(),
        json!(summary.slowest_subscription),
    );
    fields.insert(
        "slowest_subscription_ms".to_owned(),
        json!(summary.slowest_subscription_ms),
    );
    fields.insert(
        "assumption_child_skew".to_owned(),
        json!("uniform fanout over 30 parents; env JAZZ_CUSTOMER_SCALE scales children per parent"),
    );
    fields.insert(
        "assumption_group_graph".to_owned(),
        json!("mostly flat with several 2-4-hop chains"),
    );
    fields.insert(
        "shape_note".to_owned(),
        json!("39 subscriptions: org/group/group_access_edges/group_entry/profile, fourteen resource tables, fourteen resource-access tables, and six child tables; child rows inherit read through parent_id"),
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
    fields.insert(
        "served_view_updates".to_owned(),
        JsonValue::Array(
            summary
                .served_view_updates
                .iter()
                .map(|served| {
                    json!({
                        "subscription": served.subscription,
                        "messages": served.messages,
                        "resets": served.resets,
                        "bundles": served.bundles,
                        "reset_bundles": served.reset_bundles,
                        "non_reset_bundles": served.non_reset_bundles,
                        "result_adds": served.result_adds,
                    })
                })
                .collect(),
        ),
    );
    emit_json_line(
        "customer_cold_start",
        &JsonValue::Object(fields).to_string(),
    );
}

fn encoded_message_len(message: &SyncMessage) -> u64 {
    postcard::to_allocvec(message)
        .map(|bytes| bytes.len() as u64)
        .unwrap_or_default()
}

fn sized_string(prefix: &str, len: usize) -> String {
    let mut value = prefix.to_owned();
    while value.len() < len {
        value.push_str("_anon");
    }
    value.truncate(len);
    value
}

fn sized_json(len: usize) -> String {
    let mut value = "{\"value\":\"".to_owned();
    while value.len() + 2 < len {
        value.push('x');
    }
    value.push_str("\"}");
    value
}

fn row(id: u64) -> RowUuid {
    RowUuid::from_bytes(id.to_be_bytes().repeat(2).try_into().unwrap())
}

fn node(id: u64) -> NodeUuid {
    NodeUuid::from_bytes(id.to_be_bytes().repeat(2).try_into().unwrap())
}

fn node_uuid_seed(node: NodeUuid) -> u64 {
    u64::from_le_bytes(node.as_bytes()[0..8].try_into().unwrap())
}

fn next_seed_time() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(default)
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(default)
}

fn peak_rss_bytes() -> u64 {
    #[cfg(target_os = "macos")]
    unsafe {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        if libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) == 0 {
            return usage.assume_init().ru_maxrss as u64;
        }
        0
    }
    #[cfg(not(target_os = "macos"))]
    {
        0
    }
}
