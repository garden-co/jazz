use std::collections::BTreeMap;

use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::{Durability, RocksDbStorage};
use jazz::ids::{AuthorId, NodeUuid};
use jazz::node::{CurrentRow, NodeState};
use jazz::peer::PeerState;
use jazz::schema::{JazzSchema, TableSchema};
use jazz::tx::DurabilityTier;
use jazz_sim::fixture::{
    CellValueGen, CurrentRowsSync, EdgeSet, EntitySet, Fixture, FixtureBuilder, FixtureCommitApply,
    RefDistribution, apply_fixture_commit, sync_current_rows,
};
use jazz_sim::{
    DeterministicDriver, DriverContext, NodeRole, PeerProfile, ThreadedDriver, Topology,
    emit_json_line, metadata_fields,
};
use serde_json::{Value as JsonValue, json};

const USERS: &str = "users";
const ISSUES: &str = "issues";
const ISSUE_MEMBERS: &str = "issue_members";

fn main() {
    let seed = env_u64("JAZZ_SEED", 0x000f_17c7_51e5);
    let profile_name = std::env::var("JAZZ_PROFILE").unwrap_or_else(|_| "fixture-local".into());
    let profile = PeerProfile::new(profile_name.clone(), 0, 0, 0);
    let topology = topology(profile.clone());

    let mut deterministic = DeterministicDriver::new(topology.clone(), seed);
    let left = execute_fixture_smoke(&mut deterministic, seed);
    let mut deterministic_again = DeterministicDriver::new(topology.clone(), seed);
    let right = execute_fixture_smoke(&mut deterministic_again, seed);
    assert_eq!(left.final_state_hash, right.final_state_hash);
    emit_summary("deterministic", seed, &profile_name, &left);

    let mut threaded = ThreadedDriver::new(topology, seed);
    let threaded_summary = execute_fixture_smoke(&mut threaded, seed);
    emit_summary("threaded", seed, &profile_name, &threaded_summary);
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Summary {
    fixture_hash: u64,
    final_state_hash: u64,
    total_rows: usize,
    fixture_commits_applied: usize,
    fixture_view_updates_applied: usize,
    users: usize,
    issues: usize,
    issue_members: usize,
    top_author_share_pct: u64,
}

fn execute_fixture_smoke(ctx: &mut dyn DriverContext, seed: u64) -> Summary {
    let schema = schema();
    let fixture = fixture(seed);
    let expected = BTreeMap::from([
        (USERS.to_owned(), fixture.table_count(USERS)),
        (ISSUES.to_owned(), fixture.table_count(ISSUES)),
        (ISSUE_MEMBERS.to_owned(), fixture.table_count(ISSUE_MEMBERS)),
    ]);
    let top_share = top_assignee_share_pct(&fixture);
    assert!(
        (20..=80).contains(&top_share),
        "zipf top author share {top_share}% outside sanity band"
    );

    let (writer_a_dir, mut writer_a) = open_node(node(1), schema.clone());
    let (writer_b_dir, mut writer_b) = open_node(node(2), schema.clone());
    let (core_dir, mut core) = open_node(node(250), schema.clone());
    let (reader_dir, mut reader) = open_node(node(3), schema.clone());
    let _dirs = (writer_a_dir, writer_b_dir, core_dir, reader_dir);

    for (idx, commit) in fixture.commits.iter().enumerate() {
        let (writer_name, writer) = if idx % 2 == 0 {
            ("writer_a", &mut writer_a)
        } else {
            ("writer_b", &mut writer_b)
        };
        apply_fixture_commit(
            ctx,
            writer,
            &mut core,
            commit,
            FixtureCommitApply {
                writer_name,
                core_name: "core",
                made_by: AuthorId::SYSTEM,
                now_ms: 1_000 + idx as u64,
            },
        )
        .expect("fixture commit");
    }

    let mut to_writer_a = PeerState::new();
    let mut to_writer_b = PeerState::new();
    let mut to_reader = PeerState::new();
    for table in [USERS, ISSUES, ISSUE_MEMBERS] {
        sync_current_rows(
            ctx,
            &mut core,
            &mut writer_a,
            &mut to_writer_a,
            CurrentRowsSync {
                from_name: "core",
                to_name: "writer_a",
                table,
            },
        )
        .expect("sync writer a");
        sync_current_rows(
            ctx,
            &mut core,
            &mut writer_b,
            &mut to_writer_b,
            CurrentRowsSync {
                from_name: "core",
                to_name: "writer_b",
                table,
            },
        )
        .expect("sync writer b");
        sync_current_rows(
            ctx,
            &mut core,
            &mut reader,
            &mut to_reader,
            CurrentRowsSync {
                from_name: "core",
                to_name: "reader",
                table,
            },
        )
        .expect("sync reader");
    }

    for node in [&mut writer_a, &mut writer_b, &mut core, &mut reader] {
        assert_counts(node, &schema, &expected);
    }

    Summary {
        fixture_hash: fixture.stable_hash(),
        final_state_hash: final_state_hash(
            &mut [&mut writer_a, &mut writer_b, &mut core, &mut reader],
            &schema,
        ),
        total_rows: fixture.commits.len(),
        fixture_commits_applied: fixture.commits.len(),
        fixture_view_updates_applied: 9,
        users: expected[USERS],
        issues: expected[ISSUES],
        issue_members: expected[ISSUE_MEMBERS],
        top_author_share_pct: top_share,
    }
}

fn fixture(seed: u64) -> Fixture {
    FixtureBuilder::new()
        .entity_set(EntitySet::new("users", USERS, 20).as_authors().cell(
            "name",
            CellValueGen::StringPool {
                prefix: "user".to_owned(),
                pool: 20,
            },
        ))
        .entity_set(
            EntitySet::new("issues", ISSUES, 70)
                .cell(
                    "title",
                    CellValueGen::StringPool {
                        prefix: "issue".to_owned(),
                        pool: 50,
                    },
                )
                .cell(
                    "assignee",
                    CellValueGen::UuidRef {
                        set: "users".to_owned(),
                        distribution: RefDistribution::Zipf { s: 1.2 },
                    },
                ),
        )
        .edge_set(
            EdgeSet::new(
                "issue_members",
                ISSUE_MEMBERS,
                "issues",
                "issue",
                "users",
                "user",
            )
            .per_left(1, 3)
            .right_distribution(RefDistribution::Uniform),
        )
        .build(seed)
}

fn topology(profile: PeerProfile) -> Topology {
    let schema = schema();
    Topology::default()
        .node("writer_a", schema.clone(), NodeRole::Writer)
        .node("writer_b", schema.clone(), NodeRole::Writer)
        .node("core", schema.clone(), NodeRole::Core)
        .node("reader", schema, NodeRole::Reader)
        .link("writer_a", "core", profile.clone())
        .link("writer_b", "core", profile.clone())
        .link("core", "writer_a", profile.clone())
        .link("core", "writer_b", profile.clone())
        .link("core", "reader", profile)
}

fn schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new(USERS, [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new(
            ISSUES,
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("assignee", ColumnType::Uuid),
            ],
        ),
        TableSchema::new(
            ISSUE_MEMBERS,
            [
                ColumnSchema::new("issue", ColumnType::Uuid),
                ColumnSchema::new("user", ColumnType::Uuid),
            ],
        ),
    ])
}

fn assert_counts(
    node: &mut NodeState<RocksDbStorage>,
    schema: &JazzSchema,
    expected: &BTreeMap<String, usize>,
) {
    for table in &schema.tables {
        let rows = node
            .current_rows(&table.name, DurabilityTier::Global)
            .expect("current rows");
        assert_eq!(
            rows.len(),
            expected[&table.name],
            "row count mismatch for {}",
            table.name
        );
    }
}

fn final_state_hash(nodes: &mut [&mut NodeState<RocksDbStorage>], schema: &JazzSchema) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for (node_idx, node) in nodes.iter_mut().enumerate() {
        mix_str(&mut hash, &format!("node:{node_idx}"));
        for table in &schema.tables {
            let mut rows = node
                .current_rows(&table.name, DurabilityTier::Global)
                .expect("current rows");
            rows.sort_by_key(|row| row.row_uuid());
            for row in rows {
                mix_current_row(&mut hash, &row, table);
            }
        }
    }
    hash
}

fn mix_current_row(hash: &mut u64, row: &CurrentRow, table: &TableSchema) {
    mix_str(hash, row.table());
    mix_bytes(hash, row.row_uuid().as_bytes());
    for (idx, column) in table.columns.iter().enumerate() {
        mix_str(hash, &column.name);
        if let Some(value) = row.cell_at(idx) {
            mix_str(hash, &format!("{value:?}"));
        }
    }
}

fn top_assignee_share_pct(fixture: &Fixture) -> u64 {
    let mut counts = BTreeMap::<String, u64>::new();
    let mut total = 0_u64;
    for commit in fixture
        .commits
        .iter()
        .filter(|commit| commit.table == ISSUES)
    {
        if let Some(Value::Uuid(assignee)) = commit.cells.get("assignee") {
            *counts.entry(assignee.to_string()).or_default() += 1;
            total += 1;
        }
    }
    counts.values().copied().max().unwrap_or(0) * 100 / total.max(1)
}

fn emit_summary(driver: &str, seed: u64, profile: &str, summary: &Summary) {
    let mut fields = metadata_fields("fixture_smoke", driver, seed, profile);
    fields.insert("fixture_hash".to_owned(), json!(summary.fixture_hash));
    fields.insert(
        "final_state_hash".to_owned(),
        json!(summary.final_state_hash),
    );
    fields.insert("total_rows".to_owned(), json!(summary.total_rows));
    fields.insert(
        "fixture_commits_applied".to_owned(),
        json!(summary.fixture_commits_applied),
    );
    fields.insert(
        "fixture_view_updates_applied".to_owned(),
        json!(summary.fixture_view_updates_applied),
    );
    fields.insert("users".to_owned(), json!(summary.users));
    fields.insert("issues".to_owned(), json!(summary.issues));
    fields.insert("issue_members".to_owned(), json!(summary.issue_members));
    fields.insert(
        "top_author_share_pct".to_owned(),
        json!(summary.top_author_share_pct),
    );
    emit_object(fields);
}

fn emit_object(fields: serde_json::Map<String, JsonValue>) {
    let line = serde_json::to_string(&JsonValue::Object(fields)).expect("json line");
    emit_json_line("fixture_smoke", &line);
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

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn mix_str(hash: &mut u64, value: &str) {
    mix_bytes(hash, value.as_bytes());
}

fn mix_bytes(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
