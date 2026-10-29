//! Deterministic permissioned-resource example: an empty client's first
//! sync of a permissioned view from Core. Core seeds 39 tables (resources,
//! permission inputs and children inheriting their parent's access) and
//! authorizes the fixed Member identity; the client subscribes to every table
//! and settles once each holds exactly the rows the policies allow.
//!
//! Workload revision 3: Core -> Client over the encoded wire, with a zstd
//! roundtrip per frame. Its relay variant puts the device-local native relay
//! between them, as on a device: a file-backed relay database and the client
//! as its in-memory foreground. Revision 2 had a RocksDB relay there.

mod policy;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;
use std::time::Instant;

use jazz_native_relay::{
    NATIVE_RELAY_ABI_V1, NativeRelay, NativeRelayAbiRange, NativeRelayClient, NativeRelayWire,
    RelayError, RelayOpenConfig, RelayScope,
};

use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, SubscriptionEvent,
    SubscriptionStream, TransportError, WireTransport, WireTransportAdapter, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType as PublicColumnType, Operation, PolicyExpr, SchemaBuilder, TablePolicies,
    TableSchemaBuilder, Value as PublicValue,
};
use tempfile::TempDir;

pub use jazz_benchmark_guard::Allocator as SelectedAllocator;

const ORG: &str = "org";
const GROUP: &str = "group";
const GROUP_ACCESS: &str = "group_access_edges";
const GROUP_ENTRY: &str = "group_entry";
const PROFILE: &str = "profile";
const CHILD_TABLES: usize = 6;
const DOMINANT_CHILD_VISIBLE_PARENTS: usize = 36;
const DOMINANT_CHILD_TOTAL_PARENTS: usize = 65;

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
    ResourceSpec::new("res_l", DOMINANT_CHILD_TOTAL_PARENTS, 3_000, Some(43_000)),
    ResourceSpec::new("res_m", 8, 10, None),
    ResourceSpec::new("res_n", 1, 2, None),
];

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

fn schema() -> JazzSchema {
    // This fixture exposes supporting directories and grant rows to every reader;
    // resource rows and their children retain the restricted policies below.
    let mut schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new(ORG)
                .policies(TablePolicies::new().with_select(PolicyExpr::True))
                .column("label", PublicColumnType::Text)
                .column("created_at", PublicColumnType::Timestamp)
                .column("settings", PublicColumnType::Text),
        )
        .table(
            TableSchemaBuilder::new(GROUP)
                .policies(TablePolicies::new().with_select(PolicyExpr::True))
                .fk_column("org_id", ORG)
                .column("label", PublicColumnType::Text)
                .nullable_column("description", PublicColumnType::Text)
                .column("archived", PublicColumnType::Boolean)
                .column("sort", PublicColumnType::Timestamp),
        )
        .table(
            TableSchemaBuilder::new(GROUP_ACCESS)
                .policies(TablePolicies::new().with_select(PolicyExpr::True))
                .fk_column("group_id", GROUP)
                .column("user_id", PublicColumnType::Uuid)
                .column("role", public_role_type("group_access_role")),
        )
        .table(
            TableSchemaBuilder::new(GROUP_ENTRY)
                .policies(TablePolicies::new().with_select(PolicyExpr::True))
                .fk_column("member_id", GROUP)
                .fk_column("target_id", GROUP)
                .column("administrator", PublicColumnType::Boolean)
                .column("date_added", PublicColumnType::Timestamp),
        )
        .table(
            TableSchemaBuilder::new(PROFILE)
                .policies(TablePolicies::new().with_select(PolicyExpr::True))
                .fk_column("group_id", GROUP)
                .column("email", PublicColumnType::Text)
                .column("display", PublicColumnType::Text)
                .nullable_column("last_login", PublicColumnType::Timestamp)
                .column("prefs", PublicColumnType::Text),
        );

    let mut child_slot = 0;
    for spec in RESOURCE_SPECS {
        let access_table = spec.access_table();
        let policy = resource_policy(&access_table);
        schema = schema
            .table(resource_table(spec.table).policies(TablePolicies::new().with_select(policy)));
        schema = schema.table(
            TableSchemaBuilder::new(&access_table)
                .policies(TablePolicies::new().with_select(PolicyExpr::True))
                .fk_column("resource", spec.table)
                .fk_column("team", GROUP)
                .column(
                    "grant_role",
                    public_role_type(&format!("{}_role", spec.table)),
                )
                .column("administrator", PublicColumnType::Boolean),
        );
        if spec.child_rows.is_some() {
            let table = spec.child_table(child_slot);
            child_slot += 1;
            schema = schema.table(
                TableSchemaBuilder::new(&table)
                    .fk_column("parent_id", spec.table)
                    .column("label", PublicColumnType::Text)
                    .column("value_text", PublicColumnType::Text)
                    .column("value_json", PublicColumnType::Text)
                    .column("sort", PublicColumnType::Timestamp)
                    .policies(TablePolicies::new().with_select(PolicyExpr::Inherits {
                        operation: Operation::Select,
                        via_column: "parent_id".to_owned(),
                        max_depth: None,
                    })),
            );
        }
    }
    while child_slot < CHILD_TABLES {
        let table = format!("empty_child_{child_slot}");
        schema = schema.table(
            TableSchemaBuilder::new(&table)
                .policies(TablePolicies::new().with_select(PolicyExpr::True))
                .column("parent_id", PublicColumnType::Uuid)
                .column("label", PublicColumnType::Text)
                .column("value_text", PublicColumnType::Text)
                .column("value_json", PublicColumnType::Text)
                .column("sort", PublicColumnType::Timestamp),
        );
        child_slot += 1;
    }
    JazzSchema::new(&schema.build()).expect("permissioned schema compiles")
}

fn resource_table(table: &str) -> TableSchemaBuilder {
    TableSchemaBuilder::new(table)
        .fk_column("org_id", ORG)
        .fk_column("created_by", GROUP)
        .fk_column("updated_by", GROUP)
        .column("archived", PublicColumnType::Boolean)
        .column("label", PublicColumnType::Text)
        .column("date_created", PublicColumnType::Timestamp)
        .column("date_updated", PublicColumnType::Timestamp)
        .nullable_column("col_text_a", PublicColumnType::Text)
        .nullable_column("col_text_b", PublicColumnType::Text)
        .nullable_column("col_float", PublicColumnType::Double)
        .nullable_column("col_int", PublicColumnType::Timestamp)
        .nullable_column("col_json", PublicColumnType::Text)
        .nullable_column("col_tags", PublicColumnType::Text)
}

fn public_role_type(name: &str) -> PublicColumnType {
    PublicColumnType::ScalarEnum {
        name: name.to_owned(),
        variants: vec![
            "viewer".to_owned(),
            "editor".to_owned(),
            "manager".to_owned(),
        ],
    }
}

fn resource_policy(access_table: &str) -> PolicyExpr {
    policy::seeded_recursive_access_policy(
        access_table,
        "resource",
        "team",
        &[("administrator", PublicValue::Boolean(false))],
        GROUP,
        GROUP_ENTRY,
        "member_id",
        "target_id",
        &[("administrator", PublicValue::Boolean(false))],
        GROUP_ACCESS,
        "user_id",
        &["user", "account"],
        "group_id",
    )
}

fn build_seed_plan(config: &Config) -> SeedPlan {
    let mut writes = Vec::new();
    let org = row(1);
    push_seed(&mut writes, ORG, org, org_cells());

    let mut groups = Vec::new();
    for i in 0..38 {
        let group = row(1_000 + i as u64);
        groups.push(group);
        push_seed(&mut writes, GROUP, group, group_cells(org, i));
    }
    let ordinary_user = groups[0];
    for i in 0..21 {
        push_seed(
            &mut writes,
            PROFILE,
            row(2_000 + i as u64),
            profile_cells(groups[i % groups.len()], i),
        );
    }

    let mut group_edges = Vec::new();
    for i in 0..34 {
        let group = if i < 24 { groups[i] } else { groups[i - 24] };
        push_seed(
            &mut writes,
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
        push_seed(
            &mut writes,
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
            config.child_parent_count(spec.rows)
        } else {
            config.scaled_count(spec.rows)
        };
        let edge_count = config.scaled_count(spec.edges);
        let resource_rows = (0..resource_count)
            .map(|i| row(resource_base + i as u64))
            .collect::<Vec<_>>();
        for (i, resource) in resource_rows.iter().copied().enumerate() {
            push_seed(
                &mut writes,
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
            push_seed(
                &mut writes,
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
                    push_seed(
                        &mut writes,
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

    SeedPlan {
        ordinary_user,
        visible_groups,
        table_rows,
        access,
        child_parent,
        writes,
    }
}

fn push_seed(
    writes: &mut Vec<SeedWrite>,
    table: &str,
    row: RowUuid,
    cells: BTreeMap<String, Value>,
) {
    writes.push(SeedWrite {
        table: table.to_owned(),
        row,
        cells,
    });
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
        ("res_l", _)
            if edge_index % DOMINANT_CHILD_TOTAL_PARENTS < DOMINANT_CHILD_VISIBLE_PARENTS =>
        {
            groups[24]
        }
        ("res_l", _) => groups[34 + (edge_index % 4)],
        // Transitive member group reached through group_entry: exercises the
        // recursive policy path even at the smallest scale.
        // Another direct visible resource kind without children so the member
        // slice is not child-only.
        ("res_e", 0) => groups[2],
        _ if spec.table == "res_n" || edge_index.is_multiple_of(5) => groups[34 + (edge_index % 4)],
        _ => groups[(edge_index + kind) % 34],
    }
}

fn expected_visible_rows(
    seeded: &Seeded,
    identity: BenchIdentity,
) -> BTreeMap<String, BTreeSet<RowUuid>> {
    let mut out = BTreeMap::new();
    out.insert(
        ORG.to_owned(),
        seeded.table_rows[ORG].iter().copied().collect(),
    );
    out.insert(
        GROUP.to_owned(),
        seeded.table_rows[GROUP].iter().copied().collect(),
    );
    out.insert(
        GROUP_ACCESS.to_owned(),
        seeded.table_rows[GROUP_ACCESS].iter().copied().collect(),
    );
    out.insert(
        GROUP_ENTRY.to_owned(),
        seeded.table_rows[GROUP_ENTRY].iter().copied().collect(),
    );
    out.insert(
        PROFILE.to_owned(),
        seeded.table_rows[PROFILE].iter().copied().collect(),
    );
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
        out.insert(spec.table.to_owned(), visible_resources);
        out.insert(
            spec.access_table(),
            seeded.table_rows[&spec.access_table()]
                .iter()
                .copied()
                .collect(),
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
                .filter_map(|(child, parent)| visible_resources.contains(parent).then_some(*child))
                .collect();
            out.insert(child_table, visible_children);
            child_slot += 1;
        }
    }
    for slot in 0..CHILD_TABLES {
        out.entry(format!("empty_child_{slot}")).or_default();
    }
    out
}

fn expected_visible_counts(seeded: &Seeded, identity: BenchIdentity) -> BTreeMap<String, usize> {
    expected_visible_rows(seeded, identity)
        .into_iter()
        .map(|(table, rows)| (table, rows.len()))
        .collect()
}

fn assert_policy_active(seeded: &Seeded, expected: &BTreeMap<String, usize>) {
    let mut hidden = 0_usize;
    for spec in RESOURCE_SPECS {
        hidden += seeded.table_rows[spec.table].len() - expected[spec.table];
    }
    assert!(hidden > 0, "ordinary identity must not see every resource");
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
                rows.insert(row.row.row_uuid());
            }
        }
        SubscriptionEvent::Rejected { reason } => {
            panic!("subscription rejected unexpectedly: {reason:?}")
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
        (
            "user_id".to_owned(),
            Value::Uuid(
                AuthorSubject::for_test_uuid(user.0)
                    .account_id()
                    .expect("fixture identity has an account")
                    .0,
            ),
        ),
        ("role".to_owned(), Value::EnumTag((i % 3) as u8)),
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
        ("grant_role".to_owned(), Value::EnumTag((i % 3) as u8)),
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
    for count in counts.iter_mut().take(total % parents) {
        *count += 1;
    }
    counts
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

struct Config {
    scale: f64,
    max_ticks: usize,
    identity: BenchIdentity,
}

impl Config {
    fn client_author(&self, seeded: &Seeded) -> AuthorSubject {
        match self.identity {
            BenchIdentity::Member => AuthorSubject::for_test_uuid(seeded.ordinary_user.0),
            BenchIdentity::Spy => AuthorSubject::for_test_uuid(row(9_999_999).0),
            BenchIdentity::Admin => AuthorSubject::SYSTEM,
        }
    }

    fn scaled_count(&self, count: usize) -> usize {
        ((count as f64 * self.scale).round() as usize).max(1)
    }

    fn child_parent_count(&self, count: usize) -> usize {
        if self.scale <= 1.0 {
            count
        } else {
            self.scaled_count(count)
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BenchIdentity {
    Member,
    Spy,
    Admin,
}

struct Seeded {
    _core_dir: TempDir,
    core: Db,
    ordinary_user: RowUuid,
    visible_groups: BTreeSet<RowUuid>,
    table_rows: BTreeMap<String, Vec<RowUuid>>,
    access: BTreeMap<String, Vec<(RowUuid, RowUuid)>>,
    child_parent: BTreeMap<String, Vec<(RowUuid, RowUuid)>>,
}

struct SeedPlan {
    ordinary_user: RowUuid,
    visible_groups: BTreeSet<RowUuid>,
    table_rows: BTreeMap<String, Vec<RowUuid>>,
    access: BTreeMap<String, Vec<(RowUuid, RowUuid)>>,
    child_parent: BTreeMap<String, Vec<(RowUuid, RowUuid)>>,
    writes: Vec<SeedWrite>,
}

struct SeedWrite {
    table: String,
    row: RowUuid,
    cells: BTreeMap<String, Value>,
}

type VisibleRows = BTreeMap<String, BTreeSet<RowUuid>>;

/// Seeds a file-backed Core in one transaction; outside every measurement.
fn seed_core(schema: &JazzSchema, config: &Config) -> Seeded {
    let plan = build_seed_plan(config);
    let dir = tempfile::tempdir().expect("create core directory");
    let core = block_on(Db::open_at(
        DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: node(1),
                author: AuthorSubject::SYSTEM,
            },
        ),
        &dir.path().join("core.sqlite"),
        FileOptions::default(),
    ))
    .expect("open core");
    block_on(core.transaction(async |tx| {
        for write in &plan.writes {
            tx.insert(
                &write.table,
                write.cells.clone(),
                InsertOptions {
                    row_id: Some(write.row),
                    ..Default::default()
                },
            )
            .await?;
        }
        Ok(())
    }))
    .expect("seed core");
    Seeded {
        _core_dir: dir,
        core,
        ordinary_user: plan.ordinary_user,
        visible_groups: plan.visible_groups,
        table_rows: plan.table_rows,
        access: plan.access,
        child_parent: plan.child_parent,
    }
}

/// One direction of an in-memory byte link. Each frame takes a zstd
/// roundtrip, standing in for the compressed network channel.
struct ZstdFrames {
    outbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    inbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    bytes: Rc<RefCell<usize>>,
}

impl WireTransport for ZstdFrames {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        let compressed = zstd::bulk::compress(&frame, 3).expect("compress frame");
        *self.bytes.borrow_mut() += compressed.len();
        let frame = zstd::bulk::decompress(&compressed, frame.len()).expect("decompress frame");
        self.outbound.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.inbound.borrow_mut().pop_front()
    }
}

struct OpenSubscription {
    name: String,
    expected: usize,
    stream: SubscriptionStream,
    rows: BTreeSet<RowUuid>,
}

#[derive(Debug, Default)]
pub struct RunSummary {
    actual_rows: VisibleRows,
    expected_rows_by_table: Option<Rc<VisibleRows>>,
    expected_rows: usize,
    rows_materialized: usize,
    subscriptions: usize,
    pub connect_ms: u128,
    pub subscribe_ms: u128,
    pub settle_ms: u128,
    pub wall_ms: u128,
    pub ticks: usize,
    pub bytes_to_client: usize,
    /// Dropped after the measured interval, with the summary.
    _relay: Option<RelayTeardown>,
}

/// Opens an empty file-backed client, connects it to Core, subscribes to all
/// 39 tables and ticks both sides until every subscription holds its
/// expected row count.
fn run_cold(
    schema: &JazzSchema,
    seeded: &Seeded,
    expected: &BTreeMap<String, usize>,
    config: &Config,
) -> RunSummary {
    let start = Instant::now();
    let author = config.client_author(seeded);
    let dir = tempfile::tempdir().expect("create client directory");
    let client = block_on(Db::open_at(
        DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: node(3),
                author,
            },
        ),
        &dir.path().join("client.sqlite"),
        FileOptions::default(),
    ))
    .expect("open client");
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let bytes = Rc::new(RefCell::new(0));
    let upstream = block_on(
        client.connect_upstream(Box::new(WireTransportAdapter::current(ZstdFrames {
            outbound: Rc::clone(&up),
            inbound: Rc::clone(&down),
            bytes: Rc::new(RefCell::new(0)),
        }))),
    );
    let subscriber = seeded.core.accept_subscriber(
        Box::new(WireTransportAdapter::current(ZstdFrames {
            outbound: down,
            inbound: up,
            bytes: Rc::clone(&bytes),
        })),
        author,
    );
    let connect_ms = start.elapsed().as_millis();

    let subscribe_start = Instant::now();
    let mut subscriptions = Vec::new();
    for table in subscription_tables() {
        let prepared = client
            .prepare_query(&Query::from(table.as_str()))
            .unwrap_or_else(|error| panic!("prepare {table} failed: {error}"));
        let stream = block_on(client.subscribe(&prepared, ReadOpts::default()))
            .unwrap_or_else(|error| panic!("subscribe {table} failed: {error}"));
        subscriptions.push(OpenSubscription {
            expected: *expected
                .get(&table)
                .unwrap_or_else(|| panic!("missing expected count for {table}")),
            name: table,
            stream,
            rows: BTreeSet::new(),
        });
    }
    let subscribe_ms = subscribe_start.elapsed().as_millis();

    let settle_start = Instant::now();
    let mut ticks = 0;
    loop {
        for sub in &mut subscriptions {
            while let Some(event) = sub.stream.try_next_event() {
                apply_event(&mut sub.rows, event);
            }
        }
        if subscriptions
            .iter()
            .all(|sub| sub.rows.len() == sub.expected)
        {
            break;
        }
        assert!(
            ticks < config.max_ticks,
            "timed out settling subscriptions; {}",
            pending_description(&subscriptions)
        );
        block_on(client.tick()).expect("client tick");
        block_on(seeded.core.tick()).expect("core tick");
        ticks += 1;
    }
    let settle_ms = settle_start.elapsed().as_millis();
    let wall_ms = start.elapsed().as_millis();

    seeded.core.disconnect_subscriber(subscriber);
    client.detach_connection(&upstream);
    let actual_rows = subscriptions
        .into_iter()
        .map(|sub| (sub.name, sub.rows))
        .collect::<VisibleRows>();
    let bytes_to_client = *bytes.borrow();
    RunSummary {
        rows_materialized: actual_rows.values().map(BTreeSet::len).sum(),
        subscriptions: actual_rows.len(),
        actual_rows,
        expected_rows_by_table: None,
        expected_rows: expected.values().sum(),
        connect_ms,
        subscribe_ms,
        settle_ms,
        wall_ms,
        ticks,
        bytes_to_client,
        _relay: None,
    }
}

/// Frames Core sends towards the device: each takes a zstd roundtrip and
/// waits here until the relay's bounded inbound queue admits it.
struct CoreToRelay {
    pending: Rc<RefCell<VecDeque<Vec<u8>>>>,
    received: VecDeque<Vec<u8>>,
    wire: NativeRelayWire,
    bytes: Rc<RefCell<usize>>,
}

impl WireTransport for CoreToRelay {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        let compressed = zstd::bulk::compress(&frame, 3).expect("compress frame");
        *self.bytes.borrow_mut() += compressed.len();
        let frame = zstd::bulk::decompress(&compressed, frame.len()).expect("decompress frame");
        self.pending.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        if self.received.is_empty() {
            for frame in self
                .wire
                .take_outbound_encoded()
                .expect("relay upstream frames")
            {
                let compressed = zstd::bulk::compress(&frame, 3).expect("compress frame");
                let frame =
                    zstd::bulk::decompress(&compressed, frame.len()).expect("decompress frame");
                self.received.push_back(frame);
            }
        }
        self.received.pop_front()
    }
}

/// Hands the relay the frames Core queued, as many as its inbound queue
/// admits now; the rest stay queued for the next round.
fn deliver_to_relay(pending: &RefCell<VecDeque<Vec<u8>>>, wire: &NativeRelayWire) -> bool {
    let mut pending = pending.borrow_mut();
    let mut delivered = false;
    while let Some(frame) = pending.front() {
        if wire.push_inbound_encoded(frame).is_err() {
            break;
        }
        pending.pop_front();
        delivered = true;
    }
    delivered
}

thread_local! {
    /// The client's subscriptions. Its database lives on the relay owner
    /// thread, so its streams live there too.
    static RELAY_CLIENT_SUBSCRIPTIONS: RefCell<Vec<OpenSubscription>> =
        const { RefCell::new(Vec::new()) };
}

/// The relay and its client, kept alive until after the measured interval.
struct RelayTeardown {
    _relay: NativeRelay,
    client: Option<NativeRelayClient>,
    _dir: TempDir,
}

impl std::fmt::Debug for RelayTeardown {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RelayTeardown")
    }
}

impl Drop for RelayTeardown {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            let _ = client.with_db(|_| {
                RELAY_CLIENT_SUBSCRIPTIONS.with(|subs| subs.borrow_mut().clear());
                Ok(())
            });
            let _ = client.close();
        }
    }
}

/// Like [`run_cold`], through the device-local native relay: Core serves a
/// fresh file-backed relay over the encoded wire, and the client is the
/// relay's in-memory foreground, subscribed on the relay's owner thread.
/// The relay drives itself after every call; the harness carries frames
/// between Core and the relay and ticks Core.
fn run_cold_via_relay(
    schema: &JazzSchema,
    seeded: &Seeded,
    expected: &BTreeMap<String, usize>,
    config: &Config,
) -> RunSummary {
    let start = Instant::now();
    let author = config.client_author(seeded);
    let dir = tempfile::tempdir().expect("create relay directory");
    let relay = NativeRelay::spawn(RelayOpenConfig {
        supported_abi: NativeRelayAbiRange {
            minimum: NATIVE_RELAY_ABI_V1,
            maximum: NATIVE_RELAY_ABI_V1,
        },
        scope: RelayScope {
            app_namespace: "permissioned-resources".to_owned(),
            storage_namespace: "benchmark".to_owned(),
            auth_scope: Some("benchmark-reader".to_owned()),
        },
        sqlite_path: dir.path().join("relay.sqlite"),
        schema: schema.clone(),
        identity: DbIdentity {
            node: node(2),
            author,
        },
    })
    .expect("open relay");
    let wire = relay.wire().expect("relay upstream");
    let pending = Rc::new(RefCell::new(VecDeque::new()));
    let bytes = Rc::new(RefCell::new(0));
    let subscriber = seeded.core.accept_subscriber(
        Box::new(WireTransportAdapter::current(CoreToRelay {
            pending: Rc::clone(&pending),
            received: VecDeque::new(),
            wire: wire.clone(),
            bytes: Rc::clone(&bytes),
        })),
        author,
    );
    let client = relay
        .attach_client(
            DbIdentity {
                node: node(3),
                author,
            },
            BTreeMap::new(),
        )
        .expect("attach client");
    let connect_ms = start.elapsed().as_millis();

    let subscribe_start = Instant::now();
    let planned = subscription_tables()
        .into_iter()
        .map(|table| {
            let count = *expected
                .get(&table)
                .unwrap_or_else(|| panic!("missing expected count for {table}"));
            (table, count)
        })
        .collect::<Vec<_>>();
    client
        .with_db(move |client| {
            let mut subscriptions = Vec::new();
            for (table, expected) in planned {
                let prepared = client
                    .prepare_query(&Query::from(table.as_str()))
                    .unwrap_or_else(|error| panic!("prepare {table} failed: {error}"));
                let stream = block_on(client.subscribe(&prepared, ReadOpts::default()))
                    .unwrap_or_else(|error| panic!("subscribe {table} failed: {error}"));
                subscriptions.push(OpenSubscription {
                    name: table,
                    expected,
                    stream,
                    rows: BTreeSet::new(),
                });
            }
            RELAY_CLIENT_SUBSCRIPTIONS.with(|subs| *subs.borrow_mut() = subscriptions);
            Ok(())
        })
        .expect("subscribe on relay client");
    let subscribe_ms = subscribe_start.elapsed().as_millis();

    let settle_start = Instant::now();
    let mut ticks = 0;
    loop {
        // A raw `Db` call does not schedule the relay's foreground tick the
        // way a binding command does, so the client ticks itself here, as
        // the direct run's harness ticks its client.
        let pending_subscriptions = client
            .with_db(|client| {
                block_on(client.tick()).map_err(|error| RelayError::Drive(error.to_string()))?;
                Ok(RELAY_CLIENT_SUBSCRIPTIONS.with(|subs| {
                    let mut subs = subs.borrow_mut();
                    for sub in subs.iter_mut() {
                        while let Some(event) = sub.stream.try_next_event() {
                            apply_event(&mut sub.rows, event);
                        }
                    }
                    if subs.iter().all(|sub| sub.rows.len() == sub.expected) {
                        None
                    } else {
                        Some(pending_description(&subs))
                    }
                }))
            })
            .expect("read relay client subscriptions");
        let Some(pending_subscriptions) = pending_subscriptions else {
            break;
        };
        assert!(
            ticks < config.max_ticks,
            "timed out settling subscriptions; {pending_subscriptions}"
        );
        block_on(seeded.core.tick()).expect("core tick");
        deliver_to_relay(&pending, &wire);
        relay.pump().expect("relay drive");
        ticks += 1;
    }
    let settle_ms = settle_start.elapsed().as_millis();
    let wall_ms = start.elapsed().as_millis();

    seeded.core.disconnect_subscriber(subscriber);
    let actual_rows = client
        .with_db(|_| {
            Ok(RELAY_CLIENT_SUBSCRIPTIONS.with(|subs| {
                std::mem::take(&mut *subs.borrow_mut())
                    .into_iter()
                    .map(|sub| (sub.name, sub.rows))
                    .collect::<VisibleRows>()
            }))
        })
        .expect("collect relay client rows");
    let bytes_to_client = *bytes.borrow();
    RunSummary {
        rows_materialized: actual_rows.values().map(BTreeSet::len).sum(),
        subscriptions: actual_rows.len(),
        actual_rows,
        expected_rows_by_table: None,
        expected_rows: expected.values().sum(),
        connect_ms,
        subscribe_ms,
        settle_ms,
        wall_ms,
        ticks,
        bytes_to_client,
        _relay: Some(RelayTeardown {
            _relay: relay,
            client: Some(client),
            _dir: dir,
        }),
    }
}

fn pending_description(subscriptions: &[OpenSubscription]) -> String {
    subscriptions
        .iter()
        .filter(|sub| sub.rows.len() != sub.expected)
        .map(|sub| format!("{}={}/{}", sub.name, sub.rows.len(), sub.expected))
        .collect::<Vec<_>>()
        .join(", ")
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// Local profile driver: `JAZZ_CUSTOMER_IDENTITY` (member, spy, admin),
/// `JAZZ_CUSTOMER_TOPOLOGY` (direct, relay), `JAZZ_CUSTOMER_SCALE` and
/// `JAZZ_CUSTOMER_REPEATS`; prints one receipt per run.
pub fn profile_main() {
    let identity = match std::env::var("JAZZ_CUSTOMER_IDENTITY")
        .unwrap_or_else(|_| "member".to_owned())
        .as_str()
    {
        "member" => BenchIdentity::Member,
        "spy" => BenchIdentity::Spy,
        "admin" => BenchIdentity::Admin,
        other => {
            panic!("unsupported JAZZ_CUSTOMER_IDENTITY {other:?}; supported: member, spy, admin")
        }
    };
    let via_relay = match std::env::var("JAZZ_CUSTOMER_TOPOLOGY")
        .unwrap_or_else(|_| "direct".to_owned())
        .as_str()
    {
        "direct" => false,
        "relay" => true,
        other => panic!("unsupported JAZZ_CUSTOMER_TOPOLOGY {other:?}; supported: direct, relay"),
    };
    let mut fixture = Fixture::with_identity(env_or("JAZZ_CUSTOMER_SCALE", 1.0), identity);
    for run in 0..env_or("JAZZ_CUSTOMER_REPEATS", 3_usize) {
        let result = if via_relay {
            fixture.first_sync_via_relay()
        } else {
            fixture.first_sync()
        };
        println!(
            "run={run} rows={} wall_ms={} connect_ms={} subscribe_ms={} settle_ms={} ticks={} bytes_to_client={}",
            result.rows_materialized,
            result.wall_ms,
            result.connect_ms,
            result.subscribe_ms,
            result.settle_ms,
            result.ticks,
            result.bytes_to_client
        );
    }
}

pub struct Fixture {
    expected_rows_by_table: Rc<VisibleRows>,
    schema: JazzSchema,
    seeded: Seeded,
    expected: BTreeMap<String, usize>,
    config: Config,
}

impl Fixture {
    pub fn new(scale: f64) -> Self {
        jazz_benchmark_guard::refuse_contaminated_measurement();
        Self::with_identity(scale, BenchIdentity::Member)
    }

    fn with_identity(scale: f64, identity: BenchIdentity) -> Self {
        let config = Config {
            scale,
            max_ticks: 200_000,
            identity,
        };
        let schema = schema();
        let seeded = seed_core(&schema, &config);
        let expected = expected_visible_counts(&seeded, config.identity);
        if identity != BenchIdentity::Admin {
            assert_policy_active(&seeded, &expected);
        }
        if scale == 1.0 && identity == BenchIdentity::Member {
            assert_eq!(expected.values().sum::<usize>(), 27_518);
        }
        let mut expected_rows_by_table = expected_visible_rows(&seeded, config.identity);
        let tables = subscription_tables();
        expected_rows_by_table.retain(|table, _| tables.contains(table));
        Self {
            expected_rows_by_table: Rc::new(expected_rows_by_table),
            schema,
            seeded,
            expected,
            config,
        }
    }

    #[inline(never)]
    pub fn first_sync(&mut self) -> CompletedSync {
        let mut result = run_cold(&self.schema, &self.seeded, &self.expected, &self.config);
        result.expected_rows_by_table = Some(Rc::clone(&self.expected_rows_by_table));
        CompletedSync(result)
    }

    /// The same first sync through a fresh device-local native relay.
    #[inline(never)]
    pub fn first_sync_via_relay(&mut self) -> CompletedSync {
        let mut result =
            run_cold_via_relay(&self.schema, &self.seeded, &self.expected, &self.config);
        result.expected_rows_by_table = Some(Rc::clone(&self.expected_rows_by_table));
        CompletedSync(result)
    }
}

impl RunSummary {
    pub fn rows(&self) -> usize {
        self.rows_materialized
    }

    pub fn verify(&self) {
        assert_eq!(self.rows_materialized, self.expected_rows);
        if let Some(expected) = &self.expected_rows_by_table {
            assert!(
                self.actual_rows.keys().eq(expected.keys()),
                "complete planned subscription inventory"
            );
            for (table, actual) in &self.actual_rows {
                assert_eq!(actual, &expected[table], "{table}");
            }
            assert_eq!(self.actual_rows.len(), self.subscriptions);
        }
    }
}

// Divan drops returned outputs after the measured interval, before fixture teardown.
pub struct CompletedSync(RunSummary);
impl std::ops::Deref for CompletedSync {
    type Target = RunSummary;
    fn deref(&self) -> &RunSummary {
        &self.0
    }
}
impl Drop for CompletedSync {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            self.0.verify();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permissioned_first_sync_completes() {
        let mut fixture = Fixture::new(0.01);
        let result = fixture.first_sync();
        result.verify();
        assert!(result.rows() > 0);
    }

    #[test]
    fn a_second_client_syncs_the_same_exact_scope() {
        let mut fixture = Fixture::new(0.01);
        fixture.first_sync().verify();
        fixture.first_sync().verify();
    }

    #[test]
    fn first_sync_through_the_native_relay_matches_direct() {
        let mut fixture = Fixture::new(0.01);
        let direct = fixture.first_sync();
        direct.verify();
        let relayed = fixture.first_sync_via_relay();
        relayed.verify();
        assert_eq!(relayed.actual_rows, direct.actual_rows);
        fixture.first_sync_via_relay().verify();
    }

    #[test]
    fn an_unrelated_reader_gets_no_member_access() {
        let member = Fixture::new(0.01).expected.values().sum::<usize>();
        let mut fixture = Fixture::with_identity(0.01, BenchIdentity::Spy);
        let result = fixture.first_sync();
        result.verify();
        assert!(result.rows() < member);
    }

    #[test]
    fn admin_sees_every_resource() {
        let mut fixture = Fixture::with_identity(0.01, BenchIdentity::Admin);
        let result = fixture.first_sync();
        result.verify();
        let total = fixture
            .seeded
            .table_rows
            .values()
            .map(Vec::len)
            .sum::<usize>();
        assert_eq!(result.rows(), total);
    }
}
