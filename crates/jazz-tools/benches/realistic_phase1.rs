use std::fs;
#[cfg(all(feature = "fjall", not(target_arch = "wasm32")))]
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
#[cfg(all(feature = "fjall", not(target_arch = "wasm32")))]
use std::time::Instant;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use futures::executor::block_on;
use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::policy::{Operation as PolicyOperation, PolicyExpr};
use jazz_tools::query_manager::query::QueryBuilder;
use jazz_tools::query_manager::session::Session;
use jazz_tools::query_manager::types::{
    ColumnType, Schema, SchemaBuilder, TablePolicies, TableSchema, Value,
};
use jazz_tools::runtime_core::{NoopScheduler, RuntimeCore, VecSyncSender};
use jazz_tools::schema_manager::{AppId, SchemaManager};
#[cfg(all(feature = "fjall", not(target_arch = "wasm32")))]
use jazz_tools::storage::FjallStorage;
use jazz_tools::storage::MemoryStorage;
#[cfg(all(feature = "fjall", not(target_arch = "wasm32")))]
use jazz_tools::storage::Storage;
use jazz_tools::sync_manager::{
    ClientId, ClientRole, Destination, InboxEntry, ServerId, Source, SyncManager,
};
use serde::Deserialize;
#[cfg(all(feature = "fjall", not(target_arch = "wasm32")))]
use tempfile::TempDir;

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
    #[serde(default)]
    background_write_ratio: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg(all(feature = "fjall", not(target_arch = "wasm32")))]
struct R3ScenarioConfig {
    id: String,
    seed: u64,
    profile_path: String,
    cache_size_bytes: usize,
    target_project_index: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct R4ScenarioConfig {
    id: String,
    seed: u64,
    operation_count: usize,
    fanout_clients: Vec<usize>,
    target_project_index: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct R5ScenarioConfig {
    id: String,
    seed: u64,
    operation_count: usize,
    mix: Vec<WeightedOperation>,
    recursive_depths: Vec<usize>,
    shared_chain_depth: usize,
    docs_per_folder: usize,
    denied_docs: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct R7ScenarioConfig {
    id: String,
    seed: u64,
    operation_count: usize,
    hot_task_count: usize,
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
    background_write_ratio: f64,
}

#[derive(Debug, Clone)]
#[cfg(all(feature = "fjall", not(target_arch = "wasm32")))]
struct R3Scenario {
    id: String,
    seed: u64,
    profile_path: String,
    cache_size_bytes: usize,
    target_project_index: usize,
}

#[derive(Debug, Clone)]
struct R4Scenario {
    id: String,
    seed: u64,
    operation_count: usize,
    fanout_clients: Vec<usize>,
    target_project_index: usize,
}

#[derive(Debug, Clone, Copy)]
enum PermissionOperation {
    QueryVisibleDocs,
    UpdateAllowedDoc,
    UpdateDeniedDoc,
}

#[derive(Debug, Clone)]
struct R5Scenario {
    id: String,
    seed: u64,
    operation_count: usize,
    operations: Vec<PermissionOperation>,
    weights: Vec<u32>,
    recursive_depths: Vec<usize>,
    shared_chain_depth: usize,
    docs_per_folder: usize,
    denied_docs: usize,
}

#[derive(Debug, Clone)]
struct R7Scenario {
    id: String,
    seed: u64,
    operation_count: usize,
    hot_task_count: usize,
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

struct SingleHopR1State {
    client: R1State,
    server: BenchRuntime,
    client_id_on_server: ClientId,
    server_id_on_client: ServerId,
    total_routed_messages: usize,
}

struct FanoutReader {
    runtime: BenchRuntime,
    client_id_on_server: ClientId,
    server_id_on_client: ServerId,
}

struct FanoutR4State {
    writer: R1State,
    server: BenchRuntime,
    writer_client_id_on_server: ClientId,
    writer_server_id_on_client: ServerId,
    readers: Vec<FanoutReader>,
    hot_task_ids: Vec<ObjectId>,
    hot_task_cursor: usize,
    total_routed_messages: usize,
    delivered_notifications: Arc<AtomicUsize>,
}

struct PermissionBatchResult {
    total_rows: usize,
    allowed_updates: usize,
    denied_updates: usize,
}

struct PermissionR5State {
    runtime: BenchRuntime,
    rng: Lcg,
    session_alice: Session,
    allowed_doc_ids: Vec<ObjectId>,
    denied_doc_ids: Vec<ObjectId>,
    timestamp: u64,
}

#[cfg(all(feature = "fjall", not(target_arch = "wasm32")))]
struct SeededProjectBoard {
    projects: Vec<ObjectId>,
    active_tasks: Vec<ObjectId>,
}

#[cfg(all(feature = "fjall", not(target_arch = "wasm32")))]
struct ColdLoadSeededDb {
    _tempdir: TempDir,
    db_path: PathBuf,
    target_project_id: ObjectId,
    cache_size_bytes: usize,
}

impl R1State {
    fn new(profile: &ProfileConfig, scenario: &R1Scenario) -> Self {
        Self::seeded(profile, profile.seed ^ scenario.seed)
    }

    fn seeded(profile: &ProfileConfig, seed: u64) -> Self {
        Self::with_runtime(create_runtime(project_board_schema()), profile, seed)
    }

    fn with_runtime(runtime: BenchRuntime, profile: &ProfileConfig, seed: u64) -> Self {
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
            self.run_one_crud_operation(scenario);
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

    fn run_read_batch_with_churn(
        &mut self,
        read_scenario: &R2Scenario,
        write_scenario: &R1Scenario,
    ) -> usize {
        let mut total_rows = 0usize;
        let write_threshold =
            ((read_scenario.background_write_ratio.clamp(0.0, 1.0)) * 10_000.0) as usize;

        for _ in 0..read_scenario.operation_count {
            if write_threshold > 0 && self.rng.next_usize(10_000) < write_threshold {
                self.run_one_crud_operation(write_scenario);
            }
            let op_idx = self.rng.pick_weighted_index(&read_scenario.weights);
            let op = read_scenario.operations[op_idx];
            total_rows += self.execute_read(op);
        }
        total_rows
    }

    fn run_hotspot_update_batch(
        &mut self,
        hot_task_ids: &[ObjectId],
        operation_count: usize,
    ) -> usize {
        if hot_task_ids.is_empty() {
            return 0;
        }

        let mut updates = 0usize;
        for op_idx in 0..operation_count {
            let task_id = hot_task_ids[op_idx % hot_task_ids.len()];
            self.update_task_with_id(task_id);
            updates += 1;
        }
        updates
    }

    fn run_one_crud_operation(&mut self, scenario: &R1Scenario) {
        let op_idx = self.rng.pick_weighted_index(&scenario.weights);
        let op = scenario.operations[op_idx];
        match op {
            CrudOperation::InsertTask => self.insert_task(),
            CrudOperation::UpdateTask => self.update_task(),
            CrudOperation::DeleteTask => self.delete_task(),
        }
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
        self.update_task_with_id(task_id);
    }

    fn update_task_with_id(&mut self, task_id: ObjectId) {
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

#[cfg(all(feature = "fjall", not(target_arch = "wasm32")))]
fn seed_project_board_dataset<S: Storage>(
    runtime: &mut RuntimeCore<S, NoopScheduler, VecSyncSender>,
    profile: &ProfileConfig,
    seed: u64,
) -> SeededProjectBoard {
    let mut rng = Lcg::new(seed);
    let mut users = Vec::with_capacity(profile.users.max(1));
    let mut organizations = Vec::with_capacity(profile.organizations.max(1));
    let mut projects = Vec::with_capacity(profile.projects.max(1));
    let mut active_tasks = Vec::with_capacity(profile.tasks.max(1));
    let mut next_comment_seq = 0usize;
    let mut timestamp = 1_770_000_000_000_000u64;
    let mut next_timestamp = || {
        timestamp += 1;
        timestamp
    };

    for user_idx in 0..profile.users {
        let user_id = runtime
            .insert(
                "users",
                vec![
                    Value::Text(format!("User {user_idx}")),
                    Value::Text(format!("user{user_idx}@bench.local")),
                ],
                None,
            )
            .expect("seed users");
        users.push(user_id);
    }

    for org_idx in 0..profile.organizations {
        let org_id = runtime
            .insert(
                "organizations",
                vec![
                    Value::Text(format!("Org {org_idx}")),
                    Value::Timestamp(next_timestamp()),
                ],
                None,
            )
            .expect("seed organizations");
        organizations.push(org_id);
    }

    for user_idx in 0..users.len() {
        let org_id = organizations[user_idx % organizations.len()];
        let role = match user_idx % 3 {
            0 => "owner",
            1 => "editor",
            _ => "member",
        };
        runtime
            .insert(
                "memberships",
                vec![
                    Value::Uuid(org_id),
                    Value::Uuid(users[user_idx]),
                    Value::Text(role.to_string()),
                ],
                None,
            )
            .expect("seed memberships");
    }

    for project_idx in 0..profile.projects {
        let org_id = organizations[project_idx % organizations.len()];
        let project_id = runtime
            .insert(
                "projects",
                vec![
                    Value::Uuid(org_id),
                    Value::Text(format!("Project {project_idx}")),
                    Value::Boolean(false),
                    Value::Timestamp(next_timestamp()),
                ],
                None,
            )
            .expect("seed projects");
        projects.push(project_id);
    }

    for task_idx in 0..profile.tasks {
        let project_id = projects[task_idx % projects.len()];
        let assignee_id = users[task_idx % users.len()];
        let status = match task_idx % 4 {
            0 => "todo",
            1 => "in_progress",
            2 => "in_review",
            _ => "done",
        };
        let priority = ((task_idx % 5) + 1) as i32;
        let task_id = runtime
            .insert(
                "tasks",
                vec![
                    Value::Uuid(project_id),
                    Value::Text(format!("Task {task_idx}")),
                    Value::Text(status.to_string()),
                    Value::Integer(priority),
                    Value::Uuid(assignee_id),
                    Value::Timestamp(next_timestamp()),
                    Value::Null,
                ],
                None,
            )
            .expect("seed tasks");
        active_tasks.push(task_id);
    }

    for task_id in active_tasks.iter().copied() {
        for watcher_offset in 0..profile.watchers_per_task.max(1) {
            let user_id = users[(watcher_offset + rng.next_usize(users.len())) % users.len()];
            runtime
                .insert(
                    "task_watchers",
                    vec![Value::Uuid(task_id), Value::Uuid(user_id)],
                    None,
                )
                .expect("seed task_watchers");
        }
    }

    for _ in 0..profile.comments {
        let task_id = active_tasks[rng.next_usize(active_tasks.len())];
        let author_id = users[rng.next_usize(users.len())];
        runtime
            .insert(
                "task_comments",
                vec![
                    Value::Uuid(task_id),
                    Value::Uuid(author_id),
                    Value::Text(format!("seed comment {next_comment_seq}")),
                    Value::Timestamp(next_timestamp()),
                ],
                None,
            )
            .expect("seed task_comments");
        next_comment_seq += 1;
    }

    for event_idx in 0..profile.activity_events {
        let task_id = active_tasks[rng.next_usize(active_tasks.len())];
        let project_id = projects[event_idx % projects.len()];
        let actor_id = users[event_idx % users.len()];
        runtime
            .insert(
                "activity_events",
                vec![
                    Value::Uuid(project_id),
                    Value::Uuid(task_id),
                    Value::Uuid(actor_id),
                    Value::Text("status_change".to_string()),
                    Value::Timestamp(next_timestamp()),
                    Value::Text("{\"kind\":\"seed\"}".to_string()),
                ],
                None,
            )
            .expect("seed activity_events");
    }

    SeededProjectBoard {
        projects,
        active_tasks,
    }
}

#[cfg(all(feature = "fjall", not(target_arch = "wasm32")))]
impl ColdLoadSeededDb {
    fn new(profile: &ProfileConfig, scenario: &R3Scenario) -> Self {
        let tempdir = TempDir::new().expect("create tempdir for cold-load benchmark");
        let db_path = tempdir.path().join("r3_cold_load.fjall");

        let seeded = {
            let mut runtime =
                create_fjall_runtime(project_board_schema(), &db_path, scenario.cache_size_bytes);
            let seeded =
                seed_project_board_dataset(&mut runtime, profile, profile.seed ^ scenario.seed);
            runtime.flush_storage();
            runtime.storage().close().expect("close seeded fjall");
            seeded
        };

        assert!(
            !seeded.active_tasks.is_empty(),
            "cold-load dataset must contain tasks"
        );
        let target_project_id =
            seeded.projects[scenario.target_project_index % seeded.projects.len()];
        Self {
            _tempdir: tempdir,
            db_path,
            target_project_id,
            cache_size_bytes: scenario.cache_size_bytes,
        }
    }
}

impl SingleHopR1State {
    fn new(profile: &ProfileConfig, scenario: &R1Scenario) -> Self {
        let mut client_runtime = create_runtime(project_board_schema());
        let mut server_runtime = create_runtime(project_board_schema());
        let client_id_on_server = ClientId::new();
        let server_id_on_client = ServerId::new();

        server_runtime.add_client(client_id_on_server, None);
        server_runtime.set_client_role_by_name(client_id_on_server, ClientRole::Peer);
        client_runtime.add_server(server_id_on_client);

        let client = R1State::with_runtime(client_runtime, profile, profile.seed ^ scenario.seed);
        let mut state = Self {
            client,
            server: server_runtime,
            client_id_on_server,
            server_id_on_client,
            total_routed_messages: 0,
        };

        state.total_routed_messages += state.pump_until_quiescent(64);
        state
    }

    fn run_crud_batch(&mut self, scenario: &R1Scenario) -> usize {
        let executed = self.client.run_crud_batch(scenario);
        self.total_routed_messages += self.pump_until_quiescent(16);
        executed
    }

    fn run_read_batch(&mut self, scenario: &R2Scenario) -> usize {
        let total_rows = self.client.run_read_batch(scenario);
        self.total_routed_messages += self.pump_until_quiescent(16);
        total_rows
    }

    fn pump_until_quiescent(&mut self, max_rounds: usize) -> usize {
        let mut routed_messages = 0usize;
        for _ in 0..max_rounds {
            let (client_to_server, server_to_client) = self.pump_single_round();
            routed_messages += client_to_server + server_to_client;
            if client_to_server == 0 && server_to_client == 0 {
                break;
            }
        }
        routed_messages
    }

    fn pump_single_round(&mut self) -> (usize, usize) {
        let mut client_to_server = 0usize;
        let mut server_to_client = 0usize;

        self.client.runtime.batched_tick();
        for entry in self.client.runtime.sync_sender().take() {
            if entry.destination == Destination::Server(self.server_id_on_client) {
                self.server.park_sync_message(InboxEntry {
                    source: Source::Client(self.client_id_on_server),
                    payload: entry.payload,
                });
                client_to_server += 1;
            }
        }

        self.server.batched_tick();
        for entry in self.server.sync_sender().take() {
            if entry.destination == Destination::Client(self.client_id_on_server) {
                self.client.runtime.park_sync_message(InboxEntry {
                    source: Source::Server(self.server_id_on_client),
                    payload: entry.payload,
                });
                server_to_client += 1;
            }
        }

        self.client.runtime.batched_tick();
        (client_to_server, server_to_client)
    }
}

impl FanoutR4State {
    fn new(
        profile: &ProfileConfig,
        seed: u64,
        target_project_index: usize,
        fanout_clients: usize,
    ) -> Self {
        let mut writer_runtime = create_runtime(project_board_schema());
        let mut server_runtime = create_runtime(project_board_schema());
        let writer_client_id_on_server = ClientId::new();
        let writer_server_id_on_client = ServerId::new();

        server_runtime.add_client(writer_client_id_on_server, None);
        server_runtime.set_client_role_by_name(writer_client_id_on_server, ClientRole::Peer);
        writer_runtime.add_server(writer_server_id_on_client);

        let writer = R1State::with_runtime(writer_runtime, profile, profile.seed ^ seed);
        let mut state = Self {
            writer,
            server: server_runtime,
            writer_client_id_on_server,
            writer_server_id_on_client,
            readers: Vec::with_capacity(fanout_clients),
            hot_task_ids: Vec::new(),
            hot_task_cursor: 0,
            total_routed_messages: 0,
            delivered_notifications: Arc::new(AtomicUsize::new(0)),
        };

        state.total_routed_messages += state.pump_until_quiescent(64);

        let target_project_id =
            state.writer.projects[target_project_index % state.writer.projects.len()];
        state.hot_task_ids = state.collect_project_task_ids(target_project_id);
        if state.hot_task_ids.is_empty() {
            state.hot_task_ids = state.writer.active_tasks.clone();
        }

        for _ in 0..fanout_clients {
            let mut reader_runtime = create_runtime(project_board_schema());
            let reader_client_id_on_server = ClientId::new();
            let reader_server_id_on_client = ServerId::new();

            state.server.add_client(reader_client_id_on_server, None);
            state
                .server
                .set_client_role_by_name(reader_client_id_on_server, ClientRole::Peer);
            reader_runtime.add_server(reader_server_id_on_client);

            let notification_counter = Arc::clone(&state.delivered_notifications);
            let query = QueryBuilder::new("tasks")
                .filter_eq("project_id", Value::Uuid(target_project_id))
                .filter_ne("status", Value::Text("done".to_string()))
                .order_by_desc("updated_at")
                .limit(200)
                .build();
            let _subscription = reader_runtime
                .subscribe(
                    query,
                    move |_delta| {
                        notification_counter.fetch_add(1, Ordering::Relaxed);
                    },
                    None,
                )
                .expect("fanout subscription");

            state.readers.push(FanoutReader {
                runtime: reader_runtime,
                client_id_on_server: reader_client_id_on_server,
                server_id_on_client: reader_server_id_on_client,
            });
        }

        state.total_routed_messages += state.pump_until_quiescent(128);
        state.delivered_notifications.store(0, Ordering::Relaxed);
        state
    }

    fn run_update_batch(&mut self, operation_count: usize) -> (usize, usize) {
        let notifications_before = self.delivered_notifications.load(Ordering::Relaxed);
        let mut updates = 0usize;

        for _ in 0..operation_count {
            if self.hot_task_ids.is_empty() {
                break;
            }
            let task_id = self.hot_task_ids[self.hot_task_cursor % self.hot_task_ids.len()];
            self.hot_task_cursor = self.hot_task_cursor.wrapping_add(1);
            self.writer.update_task_with_id(task_id);
            updates += 1;
        }

        self.total_routed_messages += self.pump_until_quiescent(64);
        let notifications_after = self.delivered_notifications.load(Ordering::Relaxed);
        (
            updates,
            notifications_after.saturating_sub(notifications_before),
        )
    }

    fn collect_project_task_ids(&mut self, project_id: ObjectId) -> Vec<ObjectId> {
        let query = QueryBuilder::new("tasks")
            .filter_eq("project_id", Value::Uuid(project_id))
            .limit(10_000)
            .build();
        let rows = block_on(self.writer.runtime.query(query, None, None)).expect("load hot tasks");
        rows.into_iter().map(|(object_id, _)| object_id).collect()
    }

    fn pump_until_quiescent(&mut self, max_rounds: usize) -> usize {
        let mut routed = 0usize;
        for _ in 0..max_rounds {
            let round_routed = self.pump_single_round();
            routed += round_routed;
            if round_routed == 0 {
                break;
            }
        }
        routed
    }

    fn pump_single_round(&mut self) -> usize {
        let mut routed = 0usize;

        self.writer.runtime.batched_tick();
        for entry in self.writer.runtime.sync_sender().take() {
            if entry.destination == Destination::Server(self.writer_server_id_on_client) {
                self.server.park_sync_message(InboxEntry {
                    source: Source::Client(self.writer_client_id_on_server),
                    payload: entry.payload,
                });
                routed += 1;
            }
        }

        for reader in &mut self.readers {
            reader.runtime.batched_tick();
            for entry in reader.runtime.sync_sender().take() {
                if entry.destination == Destination::Server(reader.server_id_on_client) {
                    self.server.park_sync_message(InboxEntry {
                        source: Source::Client(reader.client_id_on_server),
                        payload: entry.payload,
                    });
                    routed += 1;
                }
            }
        }

        self.server.batched_tick();
        for entry in self.server.sync_sender().take() {
            match entry.destination {
                Destination::Client(client_id) if client_id == self.writer_client_id_on_server => {
                    self.writer.runtime.park_sync_message(InboxEntry {
                        source: Source::Server(self.writer_server_id_on_client),
                        payload: entry.payload,
                    });
                    routed += 1;
                }
                Destination::Client(client_id) => {
                    if let Some(reader) = self
                        .readers
                        .iter_mut()
                        .find(|reader| reader.client_id_on_server == client_id)
                    {
                        reader.runtime.park_sync_message(InboxEntry {
                            source: Source::Server(reader.server_id_on_client),
                            payload: entry.payload,
                        });
                        routed += 1;
                    }
                }
                _ => {}
            }
        }

        self.writer.runtime.batched_tick();
        for reader in &mut self.readers {
            reader.runtime.batched_tick();
        }
        routed
    }
}

impl PermissionR5State {
    fn new(scenario: &R5Scenario, recursive_depth: usize) -> Self {
        let runtime = create_runtime(permission_recursive_schema(recursive_depth));
        let session_alice = Session::new("alice");
        let mut state = Self {
            runtime,
            rng: Lcg::new(scenario.seed ^ recursive_depth as u64),
            session_alice,
            allowed_doc_ids: Vec::new(),
            denied_doc_ids: Vec::new(),
            timestamp: 1_770_000_000_000_000,
        };

        let alice_root = state
            .runtime
            .insert(
                "folders",
                vec![
                    Value::Text("alice".to_string()),
                    Value::Text("alice-root".to_string()),
                    Value::Null,
                ],
                None,
            )
            .expect("seed alice root folder");
        let mut shared_folders = vec![alice_root];
        let mut parent = alice_root;
        for idx in 0..scenario.shared_chain_depth {
            let folder_id = state
                .runtime
                .insert(
                    "folders",
                    vec![
                        Value::Text("bob".to_string()),
                        Value::Text(format!("shared-folder-{idx}")),
                        Value::Uuid(parent),
                    ],
                    None,
                )
                .expect("seed shared folder");
            shared_folders.push(folder_id);
            parent = folder_id;
        }

        for (depth_idx, folder_id) in shared_folders.iter().copied().enumerate() {
            for doc_idx in 0..scenario.docs_per_folder {
                let updated_at = state.next_timestamp();
                let owner_id = if doc_idx % 8 == 0 { "alice" } else { "bob" };
                let doc_id = state
                    .runtime
                    .insert(
                        "documents",
                        vec![
                            Value::Text(owner_id.to_string()),
                            Value::Uuid(folder_id),
                            Value::Text(format!("shared-doc-{depth_idx}-{doc_idx}")),
                            Value::Text("open".to_string()),
                            Value::Timestamp(updated_at),
                            Value::Text("{\"kind\":\"shared\"}".to_string()),
                        ],
                        None,
                    )
                    .expect("seed shared docs");
                if owner_id == "alice" {
                    state.allowed_doc_ids.push(doc_id);
                }
            }
        }

        let private_root = state
            .runtime
            .insert(
                "folders",
                vec![
                    Value::Text("bob".to_string()),
                    Value::Text("private-root".to_string()),
                    Value::Null,
                ],
                None,
            )
            .expect("seed private root folder");
        for doc_idx in 0..scenario.denied_docs {
            let updated_at = state.next_timestamp();
            let doc_id = state
                .runtime
                .insert(
                    "documents",
                    vec![
                        Value::Text("bob".to_string()),
                        Value::Uuid(private_root),
                        Value::Text(format!("private-doc-{doc_idx}")),
                        Value::Text("open".to_string()),
                        Value::Timestamp(updated_at),
                        Value::Text("{\"kind\":\"private\"}".to_string()),
                    ],
                    None,
                )
                .expect("seed private docs");
            state.denied_doc_ids.push(doc_id);
        }

        assert!(
            !state.allowed_doc_ids.is_empty(),
            "permission benchmark needs allowed documents"
        );
        assert!(
            !state.denied_doc_ids.is_empty(),
            "permission benchmark needs denied documents"
        );
        state
    }

    fn run_batch(&mut self, scenario: &R5Scenario) -> PermissionBatchResult {
        let mut result = PermissionBatchResult {
            total_rows: 0,
            allowed_updates: 0,
            denied_updates: 0,
        };

        for _ in 0..scenario.operation_count {
            let op_idx = self.rng.pick_weighted_index(&scenario.weights);
            let op = scenario.operations[op_idx];
            match op {
                PermissionOperation::QueryVisibleDocs => {
                    result.total_rows += self.query_visible_docs();
                }
                PermissionOperation::UpdateAllowedDoc => {
                    self.update_allowed_doc();
                    result.allowed_updates += 1;
                }
                PermissionOperation::UpdateDeniedDoc => {
                    self.update_denied_doc();
                    result.denied_updates += 1;
                }
            }
        }

        result
    }

    fn query_visible_docs(&mut self) -> usize {
        let query = QueryBuilder::new("documents")
            .filter_ne("status", Value::Text("archived".to_string()))
            .order_by_desc("updated_at")
            .limit(200)
            .build();
        let rows = block_on(
            self.runtime
                .query(query, Some(self.session_alice.clone()), None),
        )
        .expect("permission query");
        rows.len()
    }

    fn update_allowed_doc(&mut self) {
        let doc_id = self.allowed_doc_ids[self.rng.next_usize(self.allowed_doc_ids.len())];
        let updated_at = self.next_timestamp();
        self.runtime
            .update(
                doc_id,
                vec![
                    ("status".to_string(), Value::Text("in_review".to_string())),
                    ("updated_at".to_string(), Value::Timestamp(updated_at)),
                ],
                Some(&self.session_alice),
            )
            .expect("allowed permission update");
    }

    fn update_denied_doc(&mut self) {
        let doc_id = self.denied_doc_ids[self.rng.next_usize(self.denied_doc_ids.len())];
        let updated_at = self.next_timestamp();
        let result = self.runtime.update(
            doc_id,
            vec![
                ("status".to_string(), Value::Text("archived".to_string())),
                ("updated_at".to_string(), Value::Timestamp(updated_at)),
            ],
            Some(&self.session_alice),
        );
        assert!(result.is_err(), "expected denied permission update");
    }

    fn next_timestamp(&mut self) -> u64 {
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

fn realistic_r1_crud_single_hop(c: &mut Criterion) {
    let profile: ProfileConfig = load_json("benchmarks/realistic/profiles/s.json");
    let scenario = load_r1_scenario("benchmarks/realistic/scenarios/r1_crud_sustained.json");
    let benchmark_name = format!(
        "{}_{}_single_hop",
        scenario.id.to_lowercase(),
        profile.id.to_lowercase()
    );

    let mut group = c.benchmark_group("realistic_phase1/crud_sustained_single_hop");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(scenario.operation_count as u64));

    group.bench_with_input(
        BenchmarkId::from_parameter(benchmark_name),
        &scenario,
        |b, scenario| {
            let mut state = SingleHopR1State::new(&profile, scenario);
            b.iter(|| {
                let executed = state.run_crud_batch(scenario);
                black_box(executed);
                black_box(state.total_routed_messages);
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

fn realistic_r2_reads_single_hop(c: &mut Criterion) {
    let profile: ProfileConfig = load_json("benchmarks/realistic/profiles/s.json");
    let scenario = load_r2_scenario("benchmarks/realistic/scenarios/r2_reads_sustained.json");
    let seed_scenario = load_r1_scenario("benchmarks/realistic/scenarios/r1_crud_sustained.json");
    let benchmark_name = format!(
        "{}_{}_single_hop",
        scenario.id.to_lowercase(),
        profile.id.to_lowercase()
    );

    let mut group = c.benchmark_group("realistic_phase1/reads_sustained_single_hop");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(scenario.operation_count as u64));

    group.bench_with_input(
        BenchmarkId::from_parameter(benchmark_name),
        &scenario,
        |b, scenario| {
            let mut state = SingleHopR1State::new(&profile, &seed_scenario);
            b.iter(|| {
                let total_rows = state.run_read_batch(scenario);
                black_box(total_rows);
                black_box(state.total_routed_messages);
            });
        },
    );

    group.finish();
}

fn realistic_r2_reads_with_write_churn(c: &mut Criterion) {
    let profile: ProfileConfig = load_json("benchmarks/realistic/profiles/s.json");
    let read_scenario = load_r2_scenario("benchmarks/realistic/scenarios/r2_reads_with_churn.json");
    let write_scenario = load_r1_scenario("benchmarks/realistic/scenarios/r1_crud_sustained.json");
    let benchmark_name = format!(
        "{}_{}_with_churn",
        read_scenario.id.to_lowercase(),
        profile.id.to_lowercase()
    );

    let mut group = c.benchmark_group("realistic_phase1/reads_sustained_with_write_churn");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(read_scenario.operation_count as u64));

    group.bench_with_input(
        BenchmarkId::from_parameter(benchmark_name),
        &read_scenario,
        |b, scenario| {
            let mut state = R1State::seeded(&profile, profile.seed ^ scenario.seed);
            b.iter(|| {
                let total_rows = state.run_read_batch_with_churn(scenario, &write_scenario);
                black_box(total_rows);
            });
        },
    );

    group.finish();
}

#[cfg(all(feature = "fjall", not(target_arch = "wasm32")))]
fn realistic_r3_cold_load_fjall(c: &mut Criterion) {
    let scenario = load_r3_scenario("benchmarks/realistic/scenarios/r3_cold_load_fjall.json");
    let profile: ProfileConfig = load_json(&scenario.profile_path);
    let seeded = ColdLoadSeededDb::new(&profile, &scenario);
    let benchmark_name = format!(
        "{}_{}_fjall",
        scenario.id.to_lowercase(),
        profile.id.to_lowercase()
    );

    let mut group = c.benchmark_group("realistic_phase1/cold_load_fjall");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(1));

    group.bench_with_input(
        BenchmarkId::from_parameter(benchmark_name),
        &scenario,
        |b, _scenario| {
            b.iter(|| {
                let open_start = Instant::now();
                let mut runtime = create_fjall_runtime(
                    project_board_schema(),
                    &seeded.db_path,
                    seeded.cache_size_bytes,
                );
                let open_elapsed = open_start.elapsed();

                let query = QueryBuilder::new("tasks")
                    .filter_eq("project_id", Value::Uuid(seeded.target_project_id))
                    .filter_ne("status", Value::Text("done".to_string()))
                    .order_by_desc("updated_at")
                    .limit(200)
                    .build();

                let query_start = Instant::now();
                let rows = block_on(runtime.query(query, None, None)).expect("cold-load query");
                let query_elapsed = query_start.elapsed();

                runtime.flush_storage();
                runtime.storage().close().expect("close cold-load fjall");

                black_box(open_elapsed);
                black_box(query_elapsed);
                black_box(rows.len());
            });
        },
    );

    group.finish();
}

#[cfg(not(all(feature = "fjall", not(target_arch = "wasm32"))))]
fn realistic_r3_cold_load_fjall(_c: &mut Criterion) {}

fn realistic_r4_fanout_updates(c: &mut Criterion) {
    let profile: ProfileConfig = load_json("benchmarks/realistic/profiles/s.json");
    let scenario = load_r4_scenario("benchmarks/realistic/scenarios/r4_fanout_updates.json");

    let mut group = c.benchmark_group("realistic_phase1/fanout_updates");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(scenario.operation_count as u64));

    for fanout_clients in scenario.fanout_clients.iter().copied() {
        let bench_id = format!(
            "{}_{}_n{}",
            scenario.id.to_lowercase(),
            profile.id.to_lowercase(),
            fanout_clients
        );
        let scenario_seed = scenario.seed;
        let target_project_index = scenario.target_project_index;
        let operation_count = scenario.operation_count;
        group.bench_with_input(
            BenchmarkId::from_parameter(bench_id),
            &fanout_clients,
            |b, fanout_clients| {
                let mut state = FanoutR4State::new(
                    &profile,
                    scenario_seed,
                    target_project_index,
                    *fanout_clients,
                );
                b.iter(|| {
                    let (updates, notifications) = state.run_update_batch(operation_count);
                    black_box(updates);
                    black_box(notifications);
                    black_box(state.total_routed_messages);
                });
            },
        );
    }

    group.finish();
}

fn run_permission_scenario(c: &mut Criterion, group_name: &str, scenario_path: &str) {
    let scenario = load_r5_scenario(scenario_path);
    let mut group = c.benchmark_group(group_name);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(scenario.operation_count as u64));

    for recursive_depth in scenario.recursive_depths.iter().copied() {
        let bench_id = format!("{}_depth{recursive_depth}", scenario.id.to_lowercase());
        group.bench_with_input(
            BenchmarkId::from_parameter(bench_id),
            &recursive_depth,
            |b, recursive_depth| {
                let mut state = PermissionR5State::new(&scenario, *recursive_depth);
                b.iter(|| {
                    let result = state.run_batch(&scenario);
                    black_box(result.total_rows);
                    black_box(result.allowed_updates);
                    black_box(result.denied_updates);
                });
            },
        );
    }

    group.finish();
}

fn realistic_r5_permission_recursive(c: &mut Criterion) {
    run_permission_scenario(
        c,
        "realistic_phase1/permission_recursive",
        "benchmarks/realistic/scenarios/r5_permission_recursive.json",
    );
}

fn realistic_r6_permission_write_heavy(c: &mut Criterion) {
    run_permission_scenario(
        c,
        "realistic_phase1/permission_write_heavy",
        "benchmarks/realistic/scenarios/r6_permission_write_heavy.json",
    );
}

fn realistic_r7_hotspot_history(c: &mut Criterion) {
    let profile: ProfileConfig = load_json("benchmarks/realistic/profiles/s.json");
    let scenario = load_r7_scenario("benchmarks/realistic/scenarios/r7_hotspot_history.json");
    let benchmark_name = format!(
        "{}_{}_hot{}",
        scenario.id.to_lowercase(),
        profile.id.to_lowercase(),
        scenario.hot_task_count
    );

    let mut group = c.benchmark_group("realistic_phase1/hotspot_history");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(scenario.operation_count as u64));

    group.bench_with_input(
        BenchmarkId::from_parameter(benchmark_name),
        &scenario,
        |b, scenario| {
            let mut state = R1State::seeded(&profile, profile.seed ^ scenario.seed);
            let hot_count = scenario.hot_task_count.max(1).min(state.active_tasks.len());
            let hot_task_ids = state.active_tasks[..hot_count].to_vec();
            b.iter(|| {
                let updates =
                    state.run_hotspot_update_batch(&hot_task_ids, scenario.operation_count);
                black_box(updates);
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
        background_write_ratio: raw.background_write_ratio,
    }
}

#[cfg(all(feature = "fjall", not(target_arch = "wasm32")))]
fn load_r3_scenario(path: &str) -> R3Scenario {
    let raw: R3ScenarioConfig = load_json(path);
    R3Scenario {
        id: raw.id,
        seed: raw.seed,
        profile_path: raw.profile_path,
        cache_size_bytes: raw.cache_size_bytes,
        target_project_index: raw.target_project_index,
    }
}

fn load_r4_scenario(path: &str) -> R4Scenario {
    let raw: R4ScenarioConfig = load_json(path);
    R4Scenario {
        id: raw.id,
        seed: raw.seed,
        operation_count: raw.operation_count,
        fanout_clients: raw.fanout_clients,
        target_project_index: raw.target_project_index,
    }
}

fn load_r5_scenario(path: &str) -> R5Scenario {
    let raw: R5ScenarioConfig = load_json(path);
    let mut operations = Vec::with_capacity(raw.mix.len());
    let mut weights = Vec::with_capacity(raw.mix.len());
    for op in raw.mix {
        let parsed = match op.operation.as_str() {
            "query_visible_docs" => PermissionOperation::QueryVisibleDocs,
            "update_allowed_doc" => PermissionOperation::UpdateAllowedDoc,
            "update_denied_doc" => PermissionOperation::UpdateDeniedDoc,
            unknown => panic!("unsupported R5 operation: {unknown}"),
        };
        operations.push(parsed);
        weights.push(op.weight);
    }

    R5Scenario {
        id: raw.id,
        seed: raw.seed,
        operation_count: raw.operation_count,
        operations,
        weights,
        recursive_depths: raw.recursive_depths,
        shared_chain_depth: raw.shared_chain_depth,
        docs_per_folder: raw.docs_per_folder,
        denied_docs: raw.denied_docs,
    }
}

fn load_r7_scenario(path: &str) -> R7Scenario {
    let raw: R7ScenarioConfig = load_json(path);
    R7Scenario {
        id: raw.id,
        seed: raw.seed,
        operation_count: raw.operation_count,
        hot_task_count: raw.hot_task_count,
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

#[cfg(all(feature = "fjall", not(target_arch = "wasm32")))]
fn create_fjall_runtime(
    schema: Schema,
    db_path: &Path,
    cache_size_bytes: usize,
) -> RuntimeCore<FjallStorage, NoopScheduler, VecSyncSender> {
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
        FjallStorage::open(db_path, cache_size_bytes).expect("open fjall storage for benchmark"),
        NoopScheduler,
        VecSyncSender::new(),
    )
}

fn permission_recursive_schema(recursive_depth: usize) -> Schema {
    let folder_select = PolicyExpr::or(vec![
        PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
        PolicyExpr::inherits_with_depth(PolicyOperation::Select, "parent_id", recursive_depth),
    ]);
    let folder_update = PolicyExpr::or(vec![
        PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
        PolicyExpr::inherits_with_depth(PolicyOperation::Update, "parent_id", recursive_depth),
    ]);
    let folder_policies = TablePolicies::new()
        .with_select(folder_select)
        .with_update(Some(folder_update), PolicyExpr::True);

    let doc_select = PolicyExpr::or(vec![
        PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
        PolicyExpr::inherits_with_depth(PolicyOperation::Select, "folder_id", recursive_depth),
    ]);
    let doc_update = PolicyExpr::or(vec![
        PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
        PolicyExpr::inherits_with_depth(PolicyOperation::Update, "folder_id", recursive_depth),
    ]);
    let doc_update_check = PolicyExpr::eq_session("owner_id", vec!["user_id".into()]);
    let doc_policies = TablePolicies::new()
        .with_select(doc_select)
        .with_insert(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
        .with_update(Some(doc_update), doc_update_check);

    SchemaBuilder::new()
        .table(
            TableSchema::builder("folders")
                .column("owner_id", ColumnType::Text)
                .column("name", ColumnType::Text)
                .nullable_fk_column("parent_id", "folders")
                .policies(folder_policies),
        )
        .table(
            TableSchema::builder("documents")
                .column("owner_id", ColumnType::Text)
                .fk_column("folder_id", "folders")
                .column("title", ColumnType::Text)
                .column("status", ColumnType::Text)
                .column("updated_at", ColumnType::Timestamp)
                .column("payload", ColumnType::Text)
                .policies(doc_policies),
        )
        .build()
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

criterion_group!(
    benches,
    realistic_r1_crud,
    realistic_r1_crud_single_hop,
    realistic_r2_reads,
    realistic_r2_reads_single_hop,
    realistic_r2_reads_with_write_churn,
    realistic_r3_cold_load_fjall,
    realistic_r4_fanout_updates,
    realistic_r5_permission_recursive,
    realistic_r6_permission_write_heavy,
    realistic_r7_hotspot_history
);
criterion_main!(benches);
