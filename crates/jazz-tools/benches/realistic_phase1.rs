use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use futures::executor::block_on;
use groove::object::ObjectId;
use groove::query_manager::query::QueryBuilder;
use groove::query_manager::types::{ColumnType, Schema, SchemaBuilder, TableSchema, Value};
use groove::runtime_core::{NoopScheduler, RuntimeCore, VecSyncSender};
use groove::schema_manager::{AppId, SchemaManager};
use groove::storage::MemoryStorage;
use groove::sync_manager::SyncManager;
use serde::Deserialize;

type BenchRuntime = RuntimeCore<MemoryStorage, NoopScheduler, VecSyncSender>;

#[derive(Debug, Clone, Deserialize)]
struct ProfileConfig {
    id: String,
    seed: u64,
    users: usize,
    organizations: usize,
    projects: usize,
    tasks: usize,
    comments: usize,
    watchers_per_task: usize,
    activity_events: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct WeightedOperation {
    operation: String,
    weight: u32,
}

#[derive(Debug, Clone, Deserialize)]
struct R1ScenarioConfig {
    id: String,
    seed: u64,
    operation_count: usize,
    mix: Vec<WeightedOperation>,
}

#[derive(Debug, Clone, Deserialize)]
struct R2ScenarioConfig {
    id: String,
    seed: u64,
    operation_count: usize,
    mix: Vec<WeightedOperation>,
}

#[derive(Debug, Clone, Copy)]
enum CrudOperation {
    InsertTask,
    UpdateTask,
    DeleteTask,
}

#[derive(Debug, Clone)]
struct R1Scenario {
    id: String,
    seed: u64,
    operation_count: usize,
    operations: Vec<CrudOperation>,
    weights: Vec<u32>,
}

#[derive(Debug, Clone, Copy)]
enum ReadOperation {
    QueryBoard,
    QueryMyWork,
    QueryTaskDetail,
    QueryActivityFeed,
}

#[derive(Debug, Clone)]
struct R2Scenario {
    id: String,
    seed: u64,
    operation_count: usize,
    operations: Vec<ReadOperation>,
    weights: Vec<u32>,
}

#[derive(Debug, Clone)]
struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Self {
        Self { state: seed | 1 }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }

    fn next_usize(&mut self, upper: usize) -> usize {
        if upper <= 1 {
            return 0;
        }
        (self.next_u64() as usize) % upper
    }

    fn pick_weighted_index(&mut self, weights: &[u32]) -> usize {
        let total: u32 = weights.iter().sum();
        if total == 0 {
            return 0;
        }
        let mut cursor = (self.next_u64() % total as u64) as u32;
        for (idx, weight) in weights.iter().copied().enumerate() {
            if cursor < weight {
                return idx;
            }
            cursor -= weight;
        }
        weights.len().saturating_sub(1)
    }
}

struct R1State {
    runtime: BenchRuntime,
    rng: Lcg,
    users: Vec<ObjectId>,
    organizations: Vec<ObjectId>,
    projects: Vec<ObjectId>,
    active_tasks: Vec<ObjectId>,
    deleted_tasks: Vec<ObjectId>,
    next_task_seq: usize,
    next_comment_seq: usize,
    timestamp: u64,
    min_task_floor: usize,
}

impl R1State {
    fn new(profile: &ProfileConfig, scenario: &R1Scenario) -> Self {
        Self::seeded(profile, profile.seed ^ scenario.seed)
    }

    fn seeded(profile: &ProfileConfig, seed: u64) -> Self {
        let runtime = create_runtime(project_board_schema());
        let mut state = Self {
            runtime,
            rng: Lcg::new(seed),
            users: Vec::with_capacity(profile.users),
            organizations: Vec::with_capacity(profile.organizations),
            projects: Vec::with_capacity(profile.projects),
            active_tasks: Vec::with_capacity(profile.tasks.max(1)),
            deleted_tasks: Vec::with_capacity(profile.tasks / 4),
            next_task_seq: 0,
            next_comment_seq: 0,
            timestamp: 1_770_000_000_000_000,
            min_task_floor: (profile.tasks / 2).max(1),
        };
        state.seed_dataset(profile);
        state
    }

    fn seed_dataset(&mut self, profile: &ProfileConfig) {
        for user_idx in 0..profile.users {
            let user_id = self
                .runtime
                .insert(
                    "users",
                    vec![
                        Value::Text(format!("User {user_idx}")),
                        Value::Text(format!("user{user_idx}@bench.local")),
                    ],
                    None,
                )
                .expect("seed users");
            self.users.push(user_id);
        }

        for org_idx in 0..profile.organizations {
            let created_at = self.bump_timestamp();
            let org_id = self
                .runtime
                .insert(
                    "organizations",
                    vec![
                        Value::Text(format!("Org {org_idx}")),
                        Value::Timestamp(created_at),
                    ],
                    None,
                )
                .expect("seed organizations");
            self.organizations.push(org_id);
        }

        for user_idx in 0..self.users.len() {
            let org_id = self.organizations[user_idx % self.organizations.len()];
            let role = match user_idx % 3 {
                0 => "owner",
                1 => "editor",
                _ => "member",
            };
            let _membership_id = self
                .runtime
                .insert(
                    "memberships",
                    vec![
                        Value::Uuid(org_id),
                        Value::Uuid(self.users[user_idx]),
                        Value::Text(role.to_string()),
                    ],
                    None,
                )
                .expect("seed memberships");
        }

        for project_idx in 0..profile.projects {
            let org_id = self.organizations[project_idx % self.organizations.len()];
            let updated_at = self.bump_timestamp();
            let project_id = self
                .runtime
                .insert(
                    "projects",
                    vec![
                        Value::Uuid(org_id),
                        Value::Text(format!("Project {project_idx}")),
                        Value::Boolean(false),
                        Value::Timestamp(updated_at),
                    ],
                    None,
                )
                .expect("seed projects");
            self.projects.push(project_id);
        }

        for task_idx in 0..profile.tasks {
            let project_id = self.projects[task_idx % self.projects.len()];
            let assignee_id = self.users[task_idx % self.users.len()];
            let status = match task_idx % 4 {
                0 => "todo",
                1 => "in_progress",
                2 => "in_review",
                _ => "done",
            };
            let priority = ((task_idx % 5) + 1) as i32;
            let updated_at = self.bump_timestamp();
            let task_id = self
                .runtime
                .insert(
                    "tasks",
                    vec![
                        Value::Uuid(project_id),
                        Value::Text(format!("Task {task_idx}")),
                        Value::Text(status.to_string()),
                        Value::Integer(priority),
                        Value::Uuid(assignee_id),
                        Value::Timestamp(updated_at),
                        Value::Null,
                    ],
                    None,
                )
                .expect("seed tasks");
            self.active_tasks.push(task_id);
        }

        for task_id in self.active_tasks.iter().copied() {
            for watcher_offset in 0..profile.watchers_per_task.max(1) {
                let user_id = self.users
                    [(watcher_offset + self.rng.next_usize(self.users.len())) % self.users.len()];
                let _watcher_id = self
                    .runtime
                    .insert(
                        "task_watchers",
                        vec![Value::Uuid(task_id), Value::Uuid(user_id)],
                        None,
                    )
                    .expect("seed task_watchers");
            }
        }

        for _comment_idx in 0..profile.comments {
            let task_id = self.active_tasks[self.rng.next_usize(self.active_tasks.len())];
            let author_id = self.users[self.rng.next_usize(self.users.len())];
            let created_at = self.bump_timestamp();
            let _comment_id = self
                .runtime
                .insert(
                    "task_comments",
                    vec![
                        Value::Uuid(task_id),
                        Value::Uuid(author_id),
                        Value::Text(format!("seed comment {}", self.next_comment_seq)),
                        Value::Timestamp(created_at),
                    ],
                    None,
                )
                .expect("seed task_comments");
            self.next_comment_seq += 1;
        }

        for event_idx in 0..profile.activity_events {
            let task_id = self.active_tasks[self.rng.next_usize(self.active_tasks.len())];
            let project_id = self.projects[event_idx % self.projects.len()];
            let actor_id = self.users[event_idx % self.users.len()];
            let created_at = self.bump_timestamp();
            let _event_id = self
                .runtime
                .insert(
                    "activity_events",
                    vec![
                        Value::Uuid(project_id),
                        Value::Uuid(task_id),
                        Value::Uuid(actor_id),
                        Value::Text("status_change".to_string()),
                        Value::Timestamp(created_at),
                        Value::Text("{\"kind\":\"seed\"}".to_string()),
                    ],
                    None,
                )
                .expect("seed activity_events");
        }
    }

    fn run_crud_batch(&mut self, scenario: &R1Scenario) -> usize {
        let mut executed = 0usize;
        for _ in 0..scenario.operation_count {
            let op_idx = self.rng.pick_weighted_index(&scenario.weights);
            let op = scenario.operations[op_idx];
            match op {
                CrudOperation::InsertTask => self.insert_task(),
                CrudOperation::UpdateTask => self.update_task(),
                CrudOperation::DeleteTask => self.delete_task(),
            }
            executed += 1;
        }
        executed
    }

    fn run_read_batch(&mut self, scenario: &R2Scenario) -> usize {
        let mut total_rows = 0usize;
        for _ in 0..scenario.operation_count {
            let op_idx = self.rng.pick_weighted_index(&scenario.weights);
            let op = scenario.operations[op_idx];
            total_rows += self.execute_read(op);
        }
        total_rows
    }

    fn execute_read(&mut self, op: ReadOperation) -> usize {
        let query = match op {
            ReadOperation::QueryBoard => {
                let project_id = self.projects[self.rng.next_usize(self.projects.len())];
                QueryBuilder::new("tasks")
                    .filter_eq("project_id", Value::Uuid(project_id))
                    .filter_ne("status", Value::Text("done".to_string()))
                    .order_by_desc("updated_at")
                    .limit(50)
                    .build()
            }
            ReadOperation::QueryMyWork => {
                let assignee_id = self.users[self.rng.next_usize(self.users.len())];
                QueryBuilder::new("tasks")
                    .filter_eq("assignee_id", Value::Uuid(assignee_id))
                    .filter_ne("status", Value::Text("done".to_string()))
                    .order_by_desc("updated_at")
                    .limit(50)
                    .build()
            }
            ReadOperation::QueryTaskDetail => {
                let task_id = self.active_tasks[self.rng.next_usize(self.active_tasks.len())];
                QueryBuilder::new("task_comments")
                    .filter_eq("task_id", Value::Uuid(task_id))
                    .order_by_desc("created_at")
                    .limit(20)
                    .build()
            }
            ReadOperation::QueryActivityFeed => {
                let project_id = self.projects[self.rng.next_usize(self.projects.len())];
                QueryBuilder::new("activity_events")
                    .filter_eq("project_id", Value::Uuid(project_id))
                    .order_by_desc("created_at")
                    .limit(100)
                    .build()
            }
        };

        let rows = block_on(self.runtime.query(query, None, None)).expect("read query");
        rows.len()
    }

    fn insert_task(&mut self) {
        let project_id = self.projects[self.rng.next_usize(self.projects.len())];
        let assignee_id = self.users[self.rng.next_usize(self.users.len())];
        let priority = ((self.rng.next_usize(5) + 1) as i32).clamp(1, 5);
        let due_at = if self.rng.next_usize(4) == 0 {
            Value::Timestamp(self.bump_timestamp() + 86_400_000_000)
        } else {
            Value::Null
        };
        let updated_at = self.bump_timestamp();

        let task_id = self
            .runtime
            .insert(
                "tasks",
                vec![
                    Value::Uuid(project_id),
                    Value::Text(format!("r1 task {}", self.next_task_seq)),
                    Value::Text("todo".to_string()),
                    Value::Integer(priority),
                    Value::Uuid(assignee_id),
                    Value::Timestamp(updated_at),
                    due_at,
                ],
                None,
            )
            .expect("insert task");

        self.next_task_seq += 1;
        self.active_tasks.push(task_id);
    }

    fn update_task(&mut self) {
        if self.active_tasks.is_empty() {
            self.insert_task();
            return;
        }

        let task_id = self.active_tasks[self.rng.next_usize(self.active_tasks.len())];
        let assignee_id = self.users[self.rng.next_usize(self.users.len())];
        let status = match self.rng.next_usize(4) {
            0 => "todo",
            1 => "in_progress",
            2 => "in_review",
            _ => "done",
        };
        let priority = ((self.rng.next_usize(5) + 1) as i32).clamp(1, 5);
        let updated_at = self.bump_timestamp();

        self.runtime
            .update(
                task_id,
                vec![
                    ("status".to_string(), Value::Text(status.to_string())),
                    ("priority".to_string(), Value::Integer(priority)),
                    ("assignee_id".to_string(), Value::Uuid(assignee_id)),
                    ("updated_at".to_string(), Value::Timestamp(updated_at)),
                ],
                None,
            )
            .expect("update task");
    }

    fn delete_task(&mut self) {
        if self.active_tasks.len() <= self.min_task_floor {
            self.insert_task();
            return;
        }

        let idx = self.rng.next_usize(self.active_tasks.len());
        let task_id = self.active_tasks.swap_remove(idx);
        self.runtime.delete(task_id, None).expect("delete task");
        self.deleted_tasks.push(task_id);
    }

    fn bump_timestamp(&mut self) -> u64 {
        self.timestamp += 1;
        self.timestamp
    }
}

fn realistic_r1_crud(c: &mut Criterion) {
    let profile: ProfileConfig = load_json("benchmarks/realistic/profiles/s.json");
    let scenario = load_r1_scenario("benchmarks/realistic/scenarios/r1_crud_sustained.json");
    let benchmark_name = format!(
        "{}_{}",
        scenario.id.to_lowercase(),
        profile.id.to_lowercase()
    );

    let mut group = c.benchmark_group("realistic_phase1/crud_sustained");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(scenario.operation_count as u64));

    group.bench_with_input(
        BenchmarkId::from_parameter(benchmark_name),
        &scenario,
        |b, scenario| {
            let mut state = R1State::new(&profile, scenario);
            b.iter(|| {
                let executed = state.run_crud_batch(scenario);
                black_box(executed);
            });
        },
    );

    group.finish();
}

fn realistic_r2_reads(c: &mut Criterion) {
    let profile: ProfileConfig = load_json("benchmarks/realistic/profiles/s.json");
    let scenario = load_r2_scenario("benchmarks/realistic/scenarios/r2_reads_sustained.json");
    let benchmark_name = format!(
        "{}_{}",
        scenario.id.to_lowercase(),
        profile.id.to_lowercase()
    );

    let mut group = c.benchmark_group("realistic_phase1/reads_sustained");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(scenario.operation_count as u64));

    group.bench_with_input(
        BenchmarkId::from_parameter(benchmark_name),
        &scenario,
        |b, scenario| {
            let mut state = R1State::seeded(&profile, profile.seed ^ scenario.seed);
            b.iter(|| {
                let total_rows = state.run_read_batch(scenario);
                black_box(total_rows);
            });
        },
    );

    group.finish();
}

fn load_r1_scenario(path: &str) -> R1Scenario {
    let raw: R1ScenarioConfig = load_json(path);
    let mut operations = Vec::with_capacity(raw.mix.len());
    let mut weights = Vec::with_capacity(raw.mix.len());
    for op in raw.mix {
        let parsed = match op.operation.as_str() {
            "insert_task" => CrudOperation::InsertTask,
            "update_task" => CrudOperation::UpdateTask,
            "delete_task" => CrudOperation::DeleteTask,
            unknown => panic!("unsupported R1 operation: {unknown}"),
        };
        operations.push(parsed);
        weights.push(op.weight);
    }

    R1Scenario {
        id: raw.id,
        seed: raw.seed,
        operation_count: raw.operation_count,
        operations,
        weights,
    }
}

fn load_r2_scenario(path: &str) -> R2Scenario {
    let raw: R2ScenarioConfig = load_json(path);
    let mut operations = Vec::with_capacity(raw.mix.len());
    let mut weights = Vec::with_capacity(raw.mix.len());
    for op in raw.mix {
        let parsed = match op.operation.as_str() {
            "query_board" => ReadOperation::QueryBoard,
            "query_my_work" => ReadOperation::QueryMyWork,
            "query_task_detail" => ReadOperation::QueryTaskDetail,
            "query_activity_feed" => ReadOperation::QueryActivityFeed,
            unknown => panic!("unsupported R2 operation: {unknown}"),
        };
        operations.push(parsed);
        weights.push(op.weight);
    }

    R2Scenario {
        id: raw.id,
        seed: raw.seed,
        operation_count: raw.operation_count,
        operations,
        weights,
    }
}

fn load_json<T: for<'de> Deserialize<'de>>(path: &str) -> T {
    let file = workspace_path(path);
    let bytes =
        fs::read(&file).unwrap_or_else(|e| panic!("failed to read {}: {e}", file.display()));
    serde_json::from_slice(&bytes)
        .unwrap_or_else(|e| panic!("failed to parse {}: {e}", file.display()))
}

fn workspace_path(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path)
}

fn create_runtime(schema: Schema) -> BenchRuntime {
    let sync_manager = SyncManager::new();
    let schema_manager = SchemaManager::new(
        sync_manager,
        schema,
        AppId::from_name("realistic-phase1-bench"),
        "dev",
        "main",
    )
    .expect("create schema manager");

    RuntimeCore::new(
        schema_manager,
        MemoryStorage::new(),
        NoopScheduler,
        VecSyncSender::new(),
    )
}

fn project_board_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("display_name", ColumnType::Text)
                .column("email", ColumnType::Text),
        )
        .table(
            TableSchema::builder("organizations")
                .column("name", ColumnType::Text)
                .column("created_at", ColumnType::Timestamp),
        )
        .table(
            TableSchema::builder("memberships")
                .fk_column("organization_id", "organizations")
                .fk_column("user_id", "users")
                .column("role", ColumnType::Text),
        )
        .table(
            TableSchema::builder("projects")
                .fk_column("organization_id", "organizations")
                .column("name", ColumnType::Text)
                .column("archived", ColumnType::Boolean)
                .column("updated_at", ColumnType::Timestamp),
        )
        .table(
            TableSchema::builder("tasks")
                .fk_column("project_id", "projects")
                .column("title", ColumnType::Text)
                .column("status", ColumnType::Text)
                .column("priority", ColumnType::Integer)
                .fk_column("assignee_id", "users")
                .column("updated_at", ColumnType::Timestamp)
                .nullable_column("due_at", ColumnType::Timestamp),
        )
        .table(
            TableSchema::builder("task_comments")
                .fk_column("task_id", "tasks")
                .fk_column("author_id", "users")
                .column("body", ColumnType::Text)
                .column("created_at", ColumnType::Timestamp),
        )
        .table(
            TableSchema::builder("task_watchers")
                .fk_column("task_id", "tasks")
                .fk_column("user_id", "users"),
        )
        .table(
            TableSchema::builder("activity_events")
                .fk_column("project_id", "projects")
                .nullable_fk_column("task_id", "tasks")
                .fk_column("actor_id", "users")
                .column("kind", ColumnType::Text)
                .column("created_at", ColumnType::Timestamp)
                .column("payload", ColumnType::Text),
        )
        .build()
}

criterion_group!(benches, realistic_r1_crud, realistic_r2_reads);
criterion_main!(benches);
