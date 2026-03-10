use std::env;
use std::time::Instant;

use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::types::{ColumnType, Schema, SchemaBuilder, TableSchema, Value};
use jazz_tools::runtime_core::{NoopScheduler, RuntimeCore, VecSyncSender};
use jazz_tools::schema_manager::{AppId, SchemaManager};
use jazz_tools::storage::{FjallStorage, Storage, SurrealKvStorage};
use jazz_tools::sync_manager::SyncManager;
use serde::Serialize;

type ProbeRuntime<S> = RuntimeCore<S, NoopScheduler, VecSyncSender>;

#[derive(Clone, Copy)]
enum Backend {
    SurrealKv,
    Fjall,
}

impl Backend {
    fn as_str(self) -> &'static str {
        match self {
            Self::SurrealKv => "surrealkv",
            Self::Fjall => "fjall",
        }
    }

    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "surrealkv" => Some(Self::SurrealKv),
            "fjall" => Some(Self::Fjall),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
enum Operation {
    SeedFirstTaskCreate,
    UpdateTaskStatus,
}

impl Operation {
    fn as_str(self) -> &'static str {
        match self {
            Self::SeedFirstTaskCreate => "seed_first_task_create",
            Self::UpdateTaskStatus => "update_task_status",
        }
    }

    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "seed_first_task_create" => Some(Self::SeedFirstTaskCreate),
            "update_task_status" => Some(Self::UpdateTaskStatus),
            _ => None,
        }
    }
}

#[derive(Serialize)]
struct ProbeResult {
    backend: &'static str,
    operation: &'static str,
    elapsed_ms: f64,
}

struct SeededBoard {
    users: [ObjectId; 2],
    project_id: ObjectId,
    task_id: Option<ObjectId>,
    next_timestamp: u64,
}

fn main() {
    let mut backend = None;
    let mut operation = None;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--backend" => backend = args.next().and_then(|value| Backend::parse(&value)),
            "--operation" => operation = args.next().and_then(|value| Operation::parse(&value)),
            _ => {}
        }
    }

    let backend = backend.unwrap_or_else(|| {
        eprintln!("missing or invalid --backend (surrealkv|fjall)");
        std::process::exit(2);
    });
    let operation = operation.unwrap_or_else(|| {
        eprintln!("missing or invalid --operation (seed_first_task_create|update_task_status)");
        std::process::exit(2);
    });

    let elapsed_ms = match backend {
        Backend::SurrealKv => {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let storage =
                SurrealKvStorage::open(tempdir.path().join("probe.surrealkv"), 64 * 1024 * 1024)
                    .expect("open surrealkv");
            run_with_storage(storage, operation)
        }
        Backend::Fjall => {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let storage = FjallStorage::open(tempdir.path().join("probe.fjall"), 64 * 1024 * 1024)
                .expect("open fjall");
            run_with_storage(storage, operation)
        }
    };

    let result = ProbeResult {
        backend: backend.as_str(),
        operation: operation.as_str(),
        elapsed_ms,
    };
    println!("{}", serde_json::to_string(&result).expect("encode json"));
}

fn run_with_storage<S: Storage>(storage: S, operation: Operation) -> f64 {
    let mut runtime = create_runtime(storage);
    let started = Instant::now();
    match operation {
        Operation::SeedFirstTaskCreate => {
            let seeded = seed_board(&mut runtime, false);
            insert_task(&mut runtime, &seeded);
            runtime.flush_storage();
        }
        Operation::UpdateTaskStatus => {
            let seeded = seed_board(&mut runtime, true);
            let task_id = seeded.task_id.expect("task id");
            runtime
                .update(
                    task_id,
                    vec![
                        ("status".to_string(), Value::Text("done".to_string())),
                        ("priority".to_string(), Value::Integer(2)),
                        ("assignee_id".to_string(), Value::Uuid(seeded.users[1])),
                        (
                            "updated_at".to_string(),
                            Value::Timestamp(seeded.next_timestamp + 1),
                        ),
                    ],
                    None,
                )
                .expect("update task");
            runtime.flush_storage();
        }
    }
    started.elapsed().as_secs_f64() * 1000.0
}

fn create_runtime<S: Storage>(storage: S) -> ProbeRuntime<S> {
    let schema_manager = SchemaManager::new(
        SyncManager::new(),
        project_board_schema(),
        AppId::from_name("storage-backend-probe"),
        "dev",
        "main",
    )
    .expect("create schema manager");
    RuntimeCore::new(schema_manager, storage, NoopScheduler, VecSyncSender::new())
}

fn seed_board<S: Storage>(runtime: &mut ProbeRuntime<S>, include_task: bool) -> SeededBoard {
    let mut next_timestamp = 1_770_000_000_000_000u64;
    let mut bump_timestamp = || {
        next_timestamp += 1;
        next_timestamp
    };

    let (user_a, _) = runtime
        .insert(
            "users",
            vec![
                Value::Text("User A".to_string()),
                Value::Text("a@bench.local".to_string()),
            ],
            None,
        )
        .expect("insert user a");
    let (user_b, _) = runtime
        .insert(
            "users",
            vec![
                Value::Text("User B".to_string()),
                Value::Text("b@bench.local".to_string()),
            ],
            None,
        )
        .expect("insert user b");

    let (org_id, _) = runtime
        .insert(
            "organizations",
            vec![
                Value::Text("Org".to_string()),
                Value::Timestamp(bump_timestamp()),
            ],
            None,
        )
        .expect("insert org");

    for (user_id, role) in [(user_a, "owner"), (user_b, "editor")] {
        runtime
            .insert(
                "memberships",
                vec![
                    Value::Uuid(org_id),
                    Value::Uuid(user_id),
                    Value::Text(role.to_string()),
                ],
                None,
            )
            .expect("insert membership");
    }

    let (project_id, _) = runtime
        .insert(
            "projects",
            vec![
                Value::Uuid(org_id),
                Value::Text("Project".to_string()),
                Value::Boolean(false),
                Value::Timestamp(bump_timestamp()),
            ],
            None,
        )
        .expect("insert project");

    let task_id = include_task.then(|| {
        insert_task(
            runtime,
            &SeededBoard {
                users: [user_a, user_b],
                project_id,
                task_id: None,
                next_timestamp,
            },
        )
    });

    SeededBoard {
        users: [user_a, user_b],
        project_id,
        task_id,
        next_timestamp,
    }
}

fn insert_task<S: Storage>(runtime: &mut ProbeRuntime<S>, seeded: &SeededBoard) -> ObjectId {
    runtime
        .insert(
            "tasks",
            vec![
                Value::Uuid(seeded.project_id),
                Value::Text("Task 0".to_string()),
                Value::Text("todo".to_string()),
                Value::Integer(1),
                Value::Uuid(seeded.users[0]),
                Value::Timestamp(seeded.next_timestamp + 1),
                Value::Null,
            ],
            None,
        )
        .expect("insert task")
        .0
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
        .build()
}
