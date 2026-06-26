//! Small active direct-core realistic benchmark slice.
//!
//! This intentionally exercises `jazz::db::Db<MemoryStorage>` directly, without
//! the legacy `RuntimeCore`, `SchemaManager`, or `SyncManager` stack.

#![allow(clippy::single_element_loop)]

use std::collections::BTreeMap;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, SeededRowIdSource, SubscriptionEvent, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::query::{Query, all_of, col, eq, lit};
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::DurabilityTier;

type BenchDb = Db<MemoryStorage>;

const AUTHOR: AuthorId = AuthorId(uuid::uuid!("00000000-0000-0000-0000-0000000000a1"));

#[derive(Debug, Clone, Copy)]
struct SmallProfile {
    users: usize,
    organizations: usize,
    projects: usize,
    tasks: usize,
    comments: usize,
    watchers_per_task: usize,
    activity_events: usize,
}

const CI_S_PROFILE: SmallProfile = SmallProfile {
    users: 4,
    organizations: 2,
    projects: 8,
    tasks: 120,
    comments: 360,
    watchers_per_task: 1,
    activity_events: 240,
};

fn schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new(
            "users",
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("handle", ColumnType::String),
            ],
        )
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "organizations",
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("created_at", ColumnType::U64),
            ],
        )
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "memberships",
            [
                ColumnSchema::new("organization", ColumnType::Uuid),
                ColumnSchema::new("user", ColumnType::Uuid),
                ColumnSchema::new("role", ColumnType::String),
            ],
        )
        .with_reference("organization", "organizations")
        .with_reference("user", "users")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "projects",
            [
                ColumnSchema::new("organization", ColumnType::Uuid),
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("slug", ColumnType::String),
                ColumnSchema::new("owner", ColumnType::Uuid),
            ],
        )
        .with_reference("organization", "organizations")
        .with_reference("owner", "users")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "tasks",
            [
                ColumnSchema::new("project", ColumnType::Uuid),
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("status", ColumnType::String),
                ColumnSchema::new("priority", ColumnType::U64),
                ColumnSchema::new("assignee", ColumnType::Uuid),
                ColumnSchema::new("updated_at", ColumnType::U64),
            ],
        )
        .with_reference("project", "projects")
        .with_reference("assignee", "users")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "comments",
            [
                ColumnSchema::new("task", ColumnType::Uuid),
                ColumnSchema::new("author", ColumnType::Uuid),
                ColumnSchema::new("body", ColumnType::String),
                ColumnSchema::new("created_at", ColumnType::U64),
            ],
        )
        .with_reference("task", "tasks")
        .with_reference("author", "users")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "watchers",
            [
                ColumnSchema::new("task", ColumnType::Uuid),
                ColumnSchema::new("user", ColumnType::Uuid),
            ],
        )
        .with_reference("task", "tasks")
        .with_reference("user", "users")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "activity",
            [
                ColumnSchema::new("project", ColumnType::Uuid),
                ColumnSchema::new("task", ColumnType::Uuid),
                ColumnSchema::new("actor", ColumnType::Uuid),
                ColumnSchema::new("kind", ColumnType::String),
                ColumnSchema::new("created_at", ColumnType::U64),
            ],
        )
        .with_reference("project", "projects")
        .with_reference("task", "tasks")
        .with_reference("actor", "users")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ])
}

fn open_db(seed: u64) -> BenchDb {
    let schema = schema();
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();

    block_on(Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([seed as u8; 16]),
                author: AUTHOR,
            },
        )
        .with_id_source(SeededRowIdSource::new(seed)),
    ))
    .expect("open direct realistic benchmark db")
}

fn row_uuid(tag: u8, index: usize) -> RowUuid {
    let mut bytes = [tag; 16];
    bytes[8..16].copy_from_slice(&(index as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn wait_local(write: jazz::db::WriteHandle<MemoryStorage>) {
    block_on(write.wait(DurabilityTier::Local)).expect("write should be local");
}

fn user_cells(index: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("name".to_owned(), Value::String(format!("User {index}"))),
        ("handle".to_owned(), Value::String(format!("user-{index}"))),
    ])
}

fn organization_cells(index: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "name".to_owned(),
            Value::String(format!("Organization {index}")),
        ),
        ("created_at".to_owned(), Value::U64(index as u64)),
    ])
}

fn membership_cells(
    index: usize,
    organizations: &[RowUuid],
    users: &[RowUuid],
) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "organization".to_owned(),
            Value::Uuid(organizations[index % organizations.len()].0),
        ),
        ("user".to_owned(), Value::Uuid(users[index % users.len()].0)),
        (
            "role".to_owned(),
            Value::String(if index % 5 == 0 { "admin" } else { "member" }.to_owned()),
        ),
    ])
}

fn project_cells(
    index: usize,
    organizations: &[RowUuid],
    users: &[RowUuid],
) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "organization".to_owned(),
            Value::Uuid(organizations[index % organizations.len()].0),
        ),
        ("name".to_owned(), Value::String(format!("Project {index}"))),
        ("slug".to_owned(), Value::String(format!("project-{index}"))),
        (
            "owner".to_owned(),
            Value::Uuid(users[index % users.len()].0),
        ),
    ])
}

fn task_cells(index: usize, projects: &[RowUuid], users: &[RowUuid]) -> BTreeMap<String, Value> {
    let status = match index % 4 {
        0 => "todo",
        1 => "doing",
        2 => "review",
        _ => "done",
    };
    BTreeMap::from([
        (
            "project".to_owned(),
            Value::Uuid(projects[index % projects.len()].0),
        ),
        ("title".to_owned(), Value::String(format!("Task {index}"))),
        ("status".to_owned(), Value::String(status.to_owned())),
        ("priority".to_owned(), Value::U64((index % 5) as u64)),
        (
            "assignee".to_owned(),
            Value::Uuid(users[index % users.len()].0),
        ),
        ("updated_at".to_owned(), Value::U64(index as u64)),
    ])
}

fn comment_cells(index: usize, tasks: &[RowUuid], users: &[RowUuid]) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("task".to_owned(), Value::Uuid(tasks[index % tasks.len()].0)),
        (
            "author".to_owned(),
            Value::Uuid(users[(index * 3) % users.len()].0),
        ),
        (
            "body".to_owned(),
            Value::String(format!("Comment {index} on project-board work")),
        ),
        ("created_at".to_owned(), Value::U64(index as u64)),
    ])
}

fn watcher_cells(task: RowUuid, user: RowUuid) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("task".to_owned(), Value::Uuid(task.0)),
        ("user".to_owned(), Value::Uuid(user.0)),
    ])
}

fn activity_cells(
    index: usize,
    projects: &[RowUuid],
    tasks: &[RowUuid],
    users: &[RowUuid],
) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "project".to_owned(),
            Value::Uuid(projects[index % projects.len()].0),
        ),
        ("task".to_owned(), Value::Uuid(tasks[index % tasks.len()].0)),
        (
            "actor".to_owned(),
            Value::Uuid(users[(index * 5) % users.len()].0),
        ),
        (
            "kind".to_owned(),
            Value::String(
                if index % 2 == 0 {
                    "updated"
                } else {
                    "commented"
                }
                .to_owned(),
            ),
        ),
        ("created_at".to_owned(), Value::U64(index as u64)),
    ])
}

#[derive(Debug)]
struct Fixture {
    users: Vec<RowUuid>,
    projects: Vec<RowUuid>,
    tasks: Vec<RowUuid>,
}

fn seed_fixture(db: &BenchDb, profile: SmallProfile) -> Fixture {
    let users = (0..profile.users)
        .map(|index| {
            let row = row_uuid(0x11, index);
            wait_local(
                db.insert_with_id("users", row, user_cells(index))
                    .expect("seed user"),
            );
            row
        })
        .collect::<Vec<_>>();

    let organizations = (0..profile.organizations)
        .map(|index| {
            let row = row_uuid(0x1f, index);
            wait_local(
                db.insert_with_id("organizations", row, organization_cells(index))
                    .expect("seed organization"),
            );
            row
        })
        .collect::<Vec<_>>();

    for index in 0..(profile.organizations * profile.users) {
        wait_local(
            db.insert(
                "memberships",
                membership_cells(index, &organizations, &users),
            )
            .expect("seed membership"),
        );
    }

    let projects = (0..profile.projects)
        .map(|index| {
            let row = row_uuid(0x22, index);
            wait_local(
                db.insert_with_id(
                    "projects",
                    row,
                    project_cells(index, &organizations, &users),
                )
                .expect("seed project"),
            );
            row
        })
        .collect::<Vec<_>>();

    let tasks = (0..profile.tasks)
        .map(|index| {
            let row = row_uuid(0x33, index);
            wait_local(
                db.insert_with_id("tasks", row, task_cells(index, &projects, &users))
                    .expect("seed task"),
            );
            row
        })
        .collect::<Vec<_>>();

    for index in 0..profile.comments {
        wait_local(
            db.insert("comments", comment_cells(index, &tasks, &users))
                .expect("seed comment"),
        );
    }

    for (task_index, task) in tasks.iter().enumerate() {
        for watcher_offset in 0..profile.watchers_per_task {
            let user = users[(task_index + watcher_offset) % users.len()];
            wait_local(
                db.insert("watchers", watcher_cells(*task, user))
                    .expect("seed watcher"),
            );
        }
    }

    for index in 0..profile.activity_events {
        wait_local(
            db.insert("activity", activity_cells(index, &projects, &tasks, &users))
                .expect("seed activity"),
        );
    }

    Fixture {
        users,
        projects,
        tasks,
    }
}

fn project_board_query(db: &BenchDb, project: RowUuid) -> jazz::db::PreparedQuery {
    db.prepare_query(&Query::from("tasks").filter(eq(col("project"), lit(project.0))))
        .expect("prepare project board query")
}

fn my_work_query(db: &BenchDb, user: RowUuid) -> jazz::db::PreparedQuery {
    db.prepare_query(&Query::from("tasks").filter(all_of([
        eq(col("assignee"), lit(user.0)),
        eq(col("status"), lit("doing")),
    ])))
    .expect("prepare my work query")
}

fn task_comments_query(db: &BenchDb, task: RowUuid) -> jazz::db::PreparedQuery {
    db.prepare_query(&Query::from("comments").filter(eq(col("task"), lit(task.0))))
        .expect("prepare task comments query")
}

fn activity_feed_query(db: &BenchDb, project: RowUuid) -> jazz::db::PreparedQuery {
    db.prepare_query(&Query::from("activity").filter(eq(col("project"), lit(project.0))))
        .expect("prepare activity feed query")
}

fn drain_opened(event: Option<SubscriptionEvent>, name: &str) -> usize {
    match event {
        Some(SubscriptionEvent::Opened { current, .. }) => current.len(),
        other => panic!("expected opened {name} subscription event, got {other:?}"),
    }
}

fn drain_delta(event: Option<SubscriptionEvent>, name: &str) -> usize {
    match event {
        Some(SubscriptionEvent::Delta { added, updated, .. }) => added.len() + updated.len(),
        other => panic!("expected {name} subscription delta event, got {other:?}"),
    }
}

fn r1_crud(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic_phase1_direct/r1_crud");

    for profile in [CI_S_PROFILE] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("project_board_s", profile.tasks),
            &profile,
            |b, &profile| {
                let db = open_db(1);
                let fixture = seed_fixture(&db, profile);
                let mut next_task = profile.tasks;
                let mut update_index = 0usize;

                b.iter(|| {
                    let inserted = db
                        .insert(
                            "tasks",
                            task_cells(next_task, &fixture.projects, &fixture.users),
                        )
                        .expect("insert task");
                    let inserted_row = inserted.row_uuid();
                    wait_local(inserted);
                    next_task += 1;

                    let update_row = fixture.tasks[update_index % fixture.tasks.len()];
                    wait_local(
                        db.update(
                            "tasks",
                            update_row,
                            BTreeMap::from([
                                ("status".to_owned(), Value::String("review".to_owned())),
                                ("updated_at".to_owned(), Value::U64(next_task as u64)),
                            ]),
                        )
                        .expect("update task"),
                    );
                    update_index += 1;

                    wait_local(db.delete("tasks", inserted_row).expect("delete task"));
                });
            },
        );
    }

    group.finish();
}

fn r2_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic_phase1_direct/r2_reads");

    for profile in [CI_S_PROFILE] {
        group.throughput(Throughput::Elements(profile.tasks as u64));
        group.bench_with_input(
            BenchmarkId::new("project_board_s", profile.tasks),
            &profile,
            |b, &profile| {
                let db = open_db(2);
                let fixture = seed_fixture(&db, profile);
                let queries = [
                    project_board_query(&db, fixture.projects[0]),
                    my_work_query(&db, fixture.users[0]),
                    task_comments_query(&db, fixture.tasks[0]),
                    activity_feed_query(&db, fixture.projects[0]),
                ];
                let mut query_index = 0usize;

                b.iter(|| {
                    let rows = db
                        .read(&queries[query_index % queries.len()])
                        .expect("read realistic query");
                    query_index += 1;
                    black_box(rows.len())
                });
            },
        );
    }

    group.finish();
}

fn r4_hot_task_history(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic_phase1_direct/r4_hot_task_history");

    for profile in [CI_S_PROFILE] {
        group.throughput(Throughput::Elements(3));
        group.bench_with_input(
            BenchmarkId::new("project_board_s", profile.tasks),
            &profile,
            |b, &profile| {
                let db = open_db(4);
                let fixture = seed_fixture(&db, profile);
                let hot_task = fixture.tasks[0];
                let hot_project = fixture.projects[0];
                let mut project_board = block_on(
                    db.subscribe(&project_board_query(&db, hot_project), ReadOpts::default()),
                )
                .expect("subscribe project board");
                let mut task_comments = block_on(
                    db.subscribe(&task_comments_query(&db, hot_task), ReadOpts::default()),
                )
                .expect("subscribe task comments");
                let mut activity_feed = block_on(
                    db.subscribe(&activity_feed_query(&db, hot_project), ReadOpts::default()),
                )
                .expect("subscribe activity feed");

                black_box(drain_opened(
                    block_on(project_board.next_event()),
                    "project board",
                ));
                black_box(drain_opened(
                    block_on(task_comments.next_event()),
                    "task comments",
                ));
                black_box(drain_opened(
                    block_on(activity_feed.next_event()),
                    "activity feed",
                ));

                let mut event_index = profile.activity_events;
                b.iter(|| {
                    wait_local(
                        db.update(
                            "tasks",
                            hot_task,
                            BTreeMap::from([
                                (
                                    "status".to_owned(),
                                    Value::String(
                                        if event_index % 2 == 0 {
                                            "doing"
                                        } else {
                                            "review"
                                        }
                                        .to_owned(),
                                    ),
                                ),
                                ("updated_at".to_owned(), Value::U64(event_index as u64)),
                            ]),
                        )
                        .expect("hot task update"),
                    );
                    wait_local(
                        db.insert(
                            "comments",
                            comment_cells(event_index, &[hot_task], &fixture.users),
                        )
                        .expect("hot task comment"),
                    );
                    wait_local(
                        db.insert(
                            "activity",
                            activity_cells(
                                event_index,
                                &[hot_project],
                                &[hot_task],
                                &fixture.users,
                            ),
                        )
                        .expect("hot task activity"),
                    );
                    event_index += 1;

                    let delivered =
                        drain_delta(block_on(project_board.next_event()), "project board")
                            + drain_delta(block_on(task_comments.next_event()), "task comments")
                            + drain_delta(block_on(activity_feed.next_event()), "activity feed");
                    black_box(delivered)
                });
            },
        );
    }

    group.finish();
}

fn r9_subscribed_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic_phase1_direct/r9_subscribed_write");

    for profile in [CI_S_PROFILE] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("project_board_s", profile.tasks),
            &profile,
            |b, &profile| {
                let db = open_db(3);
                let fixture = seed_fixture(&db, profile);
                let query = project_board_query(&db, fixture.projects[0]);
                let mut subscription =
                    block_on(db.subscribe(&query, ReadOpts::default())).expect("subscribe board");
                match block_on(subscription.next_event()) {
                    Some(SubscriptionEvent::Opened { current, .. }) => {
                        assert!(!current.is_empty());
                    }
                    other => panic!("expected opened subscription event, got {other:?}"),
                }

                let mut task_index = 0usize;
                b.iter(|| {
                    let row = fixture.tasks[task_index % fixture.tasks.len()];
                    task_index += profile.projects;
                    wait_local(
                        db.update(
                            "tasks",
                            row,
                            BTreeMap::from([
                                ("status".to_owned(), Value::String("doing".to_owned())),
                                ("updated_at".to_owned(), Value::U64(task_index as u64)),
                            ]),
                        )
                        .expect("subscribed task update"),
                    );
                    match block_on(subscription.next_event()) {
                        Some(SubscriptionEvent::Delta { updated, .. }) => black_box(updated.len()),
                        other => panic!("expected subscription delta event, got {other:?}"),
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = r1_crud, r2_reads, r4_hot_task_history, r9_subscribed_write
}
criterion_main!(benches);
