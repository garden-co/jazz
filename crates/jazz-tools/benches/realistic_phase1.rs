//! Small active core realistic benchmark slice.
//!
//! This intentionally exercises `jazz::db::Db<MemoryStorage>` directly, without
//! the legacy `RuntimeCore`, `SchemaManager`, or `SyncManager` stack.

#![allow(clippy::single_element_loop, dead_code)]

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
#[cfg(feature = "rocksdb")]
use std::path::Path;
use std::rc::Rc;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use jazz::db::{
    Db, DbConfig, DbIdentity, LocalUpdates, Propagation, ReadOpts, SeededRowIdSource,
    SubscriptionEvent, WireTransportAdapter, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
#[cfg(feature = "rocksdb")]
use jazz::groove::storage::RocksDbStorage;
use jazz::groove::storage::{MemoryStorage, OrderedKvStorage};
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::query::{Query, all_of, claim, col, eq, lit};
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::DurabilityTier;
use jazz::wire::{
    FEATURE_SESSION_FRAME, FEATURE_STRUCTURED_ERRORS, FEATURE_SYNC_MESSAGE_PAYLOAD, TransportError,
    WIRE_PROTOCOL_VERSION, WireSession, WireTransport,
};
#[cfg(feature = "rocksdb")]
use tempfile::TempDir;

type BenchDb = Db<MemoryStorage>;
#[cfg(feature = "rocksdb")]
type RocksBenchDb = Db<RocksDbStorage>;

const AUTHOR: AuthorId = AuthorId(uuid::uuid!("00000000-0000-0000-0000-0000000000a1"));
const READER_AUTHOR: AuthorId = AuthorId(uuid::uuid!("00000000-0000-0000-0000-0000000000b2"));

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

fn recursive_permissions_schema() -> JazzSchema {
    let recursive_policy = Policy::shape(Query::from("docs").reachable_via(
        "doc_access",
        "doc",
        "team",
        claim("sub"),
        "team_edges",
        "member",
        "parent",
        [],
    ));

    JazzSchema::new([
        TableSchema::new(
            "docs",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("kind", ColumnType::String),
            ],
        )
        .with_read_policy(recursive_policy),
        TableSchema::new("teams", [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "doc_access",
            [
                ColumnSchema::new("doc", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
            ],
        )
        .with_reference("doc", "docs")
        .with_reference("team", "teams")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "team_edges",
            [
                ColumnSchema::new("member", ColumnType::Uuid),
                ColumnSchema::new("parent", ColumnType::Uuid),
            ],
        )
        .with_reference("member", "teams")
        .with_reference("parent", "teams")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ])
}

fn open_db(seed: u64) -> BenchDb {
    open_db_with_author(seed, AUTHOR, false)
}

fn open_core_db(seed: u64) -> BenchDb {
    open_db_with_author(seed, AuthorId::SYSTEM, true)
}

fn open_db_with_author(seed: u64, author: AuthorId, history_complete: bool) -> BenchDb {
    open_db_with_schema(seed, author, history_complete, schema())
}

fn open_db_with_schema(
    seed: u64,
    author: AuthorId,
    history_complete: bool,
    schema: JazzSchema,
) -> BenchDb {
    open_db_with_storage(
        seed,
        author,
        history_complete,
        schema,
        MemoryStorage::new,
        "open core realistic benchmark db",
    )
}

#[cfg(feature = "rocksdb")]
fn open_rocks_db_with_author(
    seed: u64,
    author: AuthorId,
    history_complete: bool,
    path: &Path,
) -> RocksBenchDb {
    open_db_with_storage(
        seed,
        author,
        history_complete,
        schema(),
        |refs| RocksDbStorage::open(path, refs).expect("open realistic RocksDB storage"),
        "open core realistic RocksDB benchmark db",
    )
}

fn open_db_with_storage<S>(
    seed: u64,
    author: AuthorId,
    history_complete: bool,
    schema: JazzSchema,
    storage: impl FnOnce(&[&str]) -> S,
    context: &str,
) -> Db<S>
where
    S: OrderedKvStorage + jazz::groove::storage::ReopenableStorage + 'static,
{
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();

    let config = DbConfig::new(
        schema,
        storage(&refs),
        DbIdentity {
            node: NodeUuid::from_bytes([seed as u8; 16]),
            author,
        },
    )
    .with_id_source(SeededRowIdSource::new(seed));

    let opened = if history_complete {
        block_on(Db::open_history_complete(config))
    } else {
        block_on(Db::open(config))
    };
    opened.expect(context)
}

struct ByteDuplexTransport {
    outbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    inbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
}

impl WireTransport for ByteDuplexTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.outbound.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.inbound.borrow_mut().pop_front()
    }
}

fn byte_duplex() -> (Box<dyn jazz::db::Transport>, Box<dyn jazz::db::Transport>) {
    let left = Rc::new(RefCell::new(VecDeque::new()));
    let right = Rc::new(RefCell::new(VecDeque::new()));
    let left_transport = ByteDuplexTransport {
        outbound: Rc::clone(&left),
        inbound: Rc::clone(&right),
    };
    let right_transport = ByteDuplexTransport {
        outbound: right,
        inbound: left,
    };
    (
        Box::new(WireTransportAdapter::current(left_transport)),
        Box::new(WireTransportAdapter::current(right_transport)),
    )
}

fn byte_duplex_with_session(
    identity: AuthorId,
    epoch: u64,
) -> (Box<dyn jazz::db::Transport>, Box<dyn jazz::db::Transport>) {
    let left = Rc::new(RefCell::new(VecDeque::new()));
    let right = Rc::new(RefCell::new(VecDeque::new()));
    let left_transport = ByteDuplexTransport {
        outbound: Rc::clone(&left),
        inbound: Rc::clone(&right),
    };
    let right_transport = ByteDuplexTransport {
        outbound: right,
        inbound: left,
    };
    let session = WireSession {
        session_id: "realistic-phase1-direct-resume".to_owned(),
        epoch,
        identity: Some(identity),
    };
    let features = FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_SESSION_FRAME | FEATURE_STRUCTURED_ERRORS;
    (
        Box::new(WireTransportAdapter::new(
            left_transport,
            WIRE_PROTOCOL_VERSION,
            features,
            Some(session.clone()),
        )),
        Box::new(WireTransportAdapter::new(
            right_transport,
            WIRE_PROTOCOL_VERSION,
            features,
            Some(session),
        )),
    )
}

fn global_subscribe_opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Global,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::Full,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

fn row_uuid(tag: u8, index: usize) -> RowUuid {
    let mut bytes = [tag; 16];
    bytes[8..16].copy_from_slice(&(index as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn wait_local<S>(write: jazz::db::WriteHandle<S>)
where
    S: OrderedKvStorage,
{
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
            Value::String(
                if index.is_multiple_of(5) {
                    "admin"
                } else {
                    "member"
                }
                .to_owned(),
            ),
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
                if index.is_multiple_of(2) {
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

fn seed_fixture<S>(db: &Db<S>, profile: SmallProfile) -> Fixture
where
    S: OrderedKvStorage + jazz::groove::storage::ReopenableStorage + 'static,
{
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

fn seed_resume_fixture<S>(db: &Db<S>, profile: SmallProfile) -> Fixture
where
    S: OrderedKvStorage + jazz::groove::storage::ReopenableStorage + 'static,
{
    let users = (0..profile.users)
        .map(|index| {
            let row = row_uuid(0x41, index);
            wait_local(
                db.insert_with_id("users", row, user_cells(index))
                    .expect("seed resume user"),
            );
            row
        })
        .collect::<Vec<_>>();

    let organizations = (0..profile.organizations)
        .map(|index| {
            let row = row_uuid(0x42, index);
            wait_local(
                db.insert_with_id("organizations", row, organization_cells(index))
                    .expect("seed resume organization"),
            );
            row
        })
        .collect::<Vec<_>>();

    let projects = (0..profile.projects)
        .map(|index| {
            let row = row_uuid(0x43, index);
            wait_local(
                db.insert_with_id(
                    "projects",
                    row,
                    project_cells(index, &organizations, &users),
                )
                .expect("seed resume project"),
            );
            row
        })
        .collect::<Vec<_>>();

    let tasks = (0..profile.tasks)
        .map(|index| {
            let row = row_uuid(0x44, index);
            wait_local(
                db.insert_with_id("tasks", row, task_cells(index, &projects, &users))
                    .expect("seed resume task"),
            );
            row
        })
        .collect::<Vec<_>>();

    Fixture {
        users,
        projects,
        tasks,
    }
}

fn project_board_query<S>(db: &Db<S>, project: RowUuid) -> jazz::db::PreparedQuery
where
    S: OrderedKvStorage + jazz::groove::storage::ReopenableStorage + 'static,
{
    db.prepare_query(&Query::from("tasks").filter(eq(col("project"), lit(project.0))))
        .expect("prepare project board query")
}

fn my_work_query<S>(db: &Db<S>, user: RowUuid) -> jazz::db::PreparedQuery
where
    S: OrderedKvStorage + jazz::groove::storage::ReopenableStorage + 'static,
{
    db.prepare_query(&Query::from("tasks").filter(all_of([
        eq(col("assignee"), lit(user.0)),
        eq(col("status"), lit("doing")),
    ])))
    .expect("prepare my work query")
}

fn task_comments_query<S>(db: &Db<S>, task: RowUuid) -> jazz::db::PreparedQuery
where
    S: OrderedKvStorage + jazz::groove::storage::ReopenableStorage + 'static,
{
    db.prepare_query(&Query::from("comments").filter(eq(col("task"), lit(task.0))))
        .expect("prepare task comments query")
}

fn activity_feed_query<S>(db: &Db<S>, project: RowUuid) -> jazz::db::PreparedQuery
where
    S: OrderedKvStorage + jazz::groove::storage::ReopenableStorage + 'static,
{
    db.prepare_query(&Query::from("activity").filter(eq(col("project"), lit(project.0))))
        .expect("prepare activity feed query")
}

const RECURSIVE_DOC_DIRECT: RowUuid = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000001"));
const RECURSIVE_DOC_CLOSURE: RowUuid = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000002"));
const RECURSIVE_DOC_HIDDEN: RowUuid = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000003"));
const RESUME_DOC_DIRECT: RowUuid = RowUuid(uuid::uuid!("13000000-0000-0000-0000-000000000001"));
const RESUME_DOC_REVOKED: RowUuid = RowUuid(uuid::uuid!("13000000-0000-0000-0000-000000000002"));
const RESUME_DOC_GRANTED: RowUuid = RowUuid(uuid::uuid!("13000000-0000-0000-0000-000000000003"));
const RESUME_DOC_NEVER: RowUuid = RowUuid(uuid::uuid!("13000000-0000-0000-0000-000000000004"));
const RECURSIVE_READER_TEAM: RowUuid = RowUuid(uuid::uuid!("00000000-0000-0000-0000-0000000000b2"));
const RECURSIVE_PARENT_TEAM: RowUuid = RowUuid(uuid::uuid!("20000000-0000-0000-0000-000000000002"));
const RECURSIVE_HIDDEN_TEAM: RowUuid = RowUuid(uuid::uuid!("20000000-0000-0000-0000-000000000003"));
const RESUME_ACCESS_DIRECT: RowUuid = RowUuid(uuid::uuid!("13000000-0000-0000-0000-000000000101"));
const RESUME_ACCESS_REVOKED: RowUuid = RowUuid(uuid::uuid!("13000000-0000-0000-0000-000000000102"));
const RESUME_ACCESS_GRANTED: RowUuid = RowUuid(uuid::uuid!("13000000-0000-0000-0000-000000000103"));
const RESUME_ACCESS_NEVER: RowUuid = RowUuid(uuid::uuid!("13000000-0000-0000-0000-000000000104"));
const RESUME_EDGE_READER_PARENT: RowUuid =
    RowUuid(uuid::uuid!("13000000-0000-0000-0000-000000000201"));

fn recursive_doc_cells(title: &str, kind: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("kind".to_owned(), Value::String(kind.to_owned())),
    ])
}

fn recursive_team_cells(name: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))])
}

fn recursive_doc_access_cells(doc: RowUuid, team: RowUuid) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("doc".to_owned(), Value::Uuid(doc.0)),
        ("team".to_owned(), Value::Uuid(team.0)),
    ])
}

fn recursive_team_edge_cells(member: RowUuid, parent: RowUuid) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("member".to_owned(), Value::Uuid(member.0)),
        ("parent".to_owned(), Value::Uuid(parent.0)),
    ])
}

fn open_recursive_permissions_db(seed: u64) -> BenchDb {
    open_db_with_schema(
        seed,
        AuthorId::SYSTEM,
        false,
        recursive_permissions_schema(),
    )
}

fn open_recursive_permissions_db_with_author(
    seed: u64,
    author: AuthorId,
    history_complete: bool,
) -> BenchDb {
    open_db_with_schema(
        seed,
        author,
        history_complete,
        recursive_permissions_schema(),
    )
}

fn seed_recursive_permissions_fixture(db: &BenchDb) {
    for (team, name) in [
        (RECURSIVE_READER_TEAM, "reader"),
        (RECURSIVE_PARENT_TEAM, "parent"),
        (RECURSIVE_HIDDEN_TEAM, "hidden"),
    ] {
        wait_local(
            db.insert_with_id("teams", team, recursive_team_cells(name))
                .expect("seed recursive team"),
        );
    }

    for (doc, title, kind) in [
        (RECURSIVE_DOC_DIRECT, "direct", "visible"),
        (RECURSIVE_DOC_CLOSURE, "closure", "visible"),
        (RECURSIVE_DOC_HIDDEN, "hidden", "hidden"),
    ] {
        wait_local(
            db.insert_with_id("docs", doc, recursive_doc_cells(title, kind))
                .expect("seed recursive doc"),
        );
    }

    for (doc, team) in [
        (RECURSIVE_DOC_DIRECT, RECURSIVE_READER_TEAM),
        (RECURSIVE_DOC_CLOSURE, RECURSIVE_PARENT_TEAM),
        (RECURSIVE_DOC_HIDDEN, RECURSIVE_HIDDEN_TEAM),
    ] {
        wait_local(
            db.insert("doc_access", recursive_doc_access_cells(doc, team))
                .expect("seed recursive doc access"),
        );
    }

    wait_local(
        db.insert(
            "team_edges",
            recursive_team_edge_cells(RECURSIVE_READER_TEAM, RECURSIVE_PARENT_TEAM),
        )
        .expect("seed recursive team edge"),
    );
}

fn seed_permission_resume_fixture(db: &BenchDb) {
    for (team, name) in [
        (RECURSIVE_READER_TEAM, "reader"),
        (RECURSIVE_PARENT_TEAM, "parent"),
        (RECURSIVE_HIDDEN_TEAM, "hidden"),
    ] {
        wait_local(
            db.insert_with_id("teams", team, recursive_team_cells(name))
                .expect("seed resume permission team"),
        );
    }

    wait_local(
        db.insert_with_id(
            "team_edges",
            RESUME_EDGE_READER_PARENT,
            recursive_team_edge_cells(RECURSIVE_READER_TEAM, RECURSIVE_PARENT_TEAM),
        )
        .expect("seed resume permission team edge"),
    );

    for (doc, title, kind) in [
        (RESUME_DOC_DIRECT, "direct", "visible"),
        (RESUME_DOC_REVOKED, "revoked", "visible-then-revoked"),
        (RESUME_DOC_GRANTED, "granted", "hidden-then-granted"),
        (RESUME_DOC_NEVER, "never", "never-visible"),
    ] {
        wait_local(
            db.insert_with_id("docs", doc, recursive_doc_cells(title, kind))
                .expect("seed resume permission doc"),
        );
    }

    for (access, doc, team) in [
        (
            RESUME_ACCESS_DIRECT,
            RESUME_DOC_DIRECT,
            RECURSIVE_READER_TEAM,
        ),
        (
            RESUME_ACCESS_REVOKED,
            RESUME_DOC_REVOKED,
            RECURSIVE_PARENT_TEAM,
        ),
        (RESUME_ACCESS_NEVER, RESUME_DOC_NEVER, RECURSIVE_HIDDEN_TEAM),
    ] {
        wait_local(
            db.insert_with_id("doc_access", access, recursive_doc_access_cells(doc, team))
                .expect("seed resume permission access"),
        );
    }
}

fn recursive_docs_query<S>(db: &Db<S>) -> jazz::db::PreparedQuery
where
    S: OrderedKvStorage + jazz::groove::storage::ReopenableStorage + 'static,
{
    db.prepare_query(&Query::from("docs"))
        .expect("prepare recursive docs query")
}

fn assert_recursive_docs_visible(rows: &[jazz::node::CurrentRow]) {
    assert!(
        rows.iter()
            .any(|row| row.row_uuid() == RECURSIVE_DOC_DIRECT)
    );
    assert!(
        rows.iter()
            .any(|row| row.row_uuid() == RECURSIVE_DOC_CLOSURE)
    );
    assert!(
        !rows
            .iter()
            .any(|row| row.row_uuid() == RECURSIVE_DOC_HIDDEN)
    );
    assert_eq!(rows.len(), 2);
}

fn assert_permission_resume_docs(rows: &[jazz::node::CurrentRow], visible: &[RowUuid]) {
    for doc in [
        RESUME_DOC_DIRECT,
        RESUME_DOC_REVOKED,
        RESUME_DOC_GRANTED,
        RESUME_DOC_NEVER,
    ] {
        let expected = visible.contains(&doc);
        assert_eq!(
            rows.iter().any(|row| row.row_uuid() == doc),
            expected,
            "unexpected visibility for {doc:?}"
        );
    }
    assert_eq!(rows.len(), visible.len());
}

fn drain_permission_resume_delta(event: Option<SubscriptionEvent>) -> (usize, usize, usize) {
    match event {
        Some(SubscriptionEvent::Delta {
            added,
            updated,
            removed,
            ..
        }) => {
            for row in added.iter().chain(updated.iter()) {
                assert_ne!(row.row_uuid(), RESUME_DOC_REVOKED);
                assert_ne!(row.row_uuid(), RESUME_DOC_NEVER);
            }
            assert!(removed.iter().any(|row| row.row_uuid == RESUME_DOC_REVOKED));
            assert!(!removed.iter().any(|row| row.row_uuid == RESUME_DOC_NEVER));
            (added.len(), updated.len(), removed.len())
        }
        None => (0, 0, 0),
        other => panic!("expected permission-filtered resume delta event, got {other:?}"),
    }
}

fn drain_optional_permission_rows(event: Option<SubscriptionEvent>) -> usize {
    match event {
        Some(SubscriptionEvent::Delta { added, updated, .. }) => added.len() + updated.len(),
        None => 0,
        other => panic!("unexpected permission snapshot subscription event {other:?}"),
    }
}

fn drain_opened(event: Option<SubscriptionEvent>, name: &str) -> usize {
    match event {
        Some(SubscriptionEvent::Delta {
            reset: true,
            added,
            updated,
            ..
        }) => added.len() + updated.len(),
        other => panic!("expected reset {name} subscription event, got {other:?}"),
    }
}

fn drain_delta(event: Option<SubscriptionEvent>, name: &str) -> usize {
    match event {
        Some(SubscriptionEvent::Delta { added, updated, .. }) => added.len() + updated.len(),
        other => panic!("expected {name} subscription delta event, got {other:?}"),
    }
}

fn r1_crud(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic_phase1/r1_crud");

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
    let mut group = c.benchmark_group("realistic_phase1/r2_reads");

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

#[cfg(feature = "rocksdb")]
fn r3_rocksdb_cold_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic_phase1/r3_rocksdb_cold_load");

    for profile in [CI_S_PROFILE] {
        group.throughput(Throughput::Elements(profile.tasks as u64));
        group.bench_with_input(
            BenchmarkId::new("project_board_s", profile.tasks),
            &profile,
            |b, &profile| {
                let tempdir = TempDir::new().expect("create tempdir for RocksDB cold-load bench");
                let db_path = tempdir.path().join("realistic_phase1.rocksdb");
                let project = {
                    let db = open_rocks_db_with_author(30, AUTHOR, false, &db_path);
                    let fixture = seed_fixture(&db, profile);
                    fixture.projects[0]
                };

                b.iter(|| {
                    let db = open_rocks_db_with_author(31, AUTHOR, false, &db_path);
                    let query = project_board_query(&db, project);
                    let rows = db.read(&query).expect("read cold project board");
                    assert!(!rows.is_empty());
                    black_box(rows.len())
                });
            },
        );
    }

    group.finish();
}

#[cfg(not(feature = "rocksdb"))]
fn r3_rocksdb_cold_load(_c: &mut Criterion) {}

fn r4_hot_task_history(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic_phase1/r4_hot_task_history");

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
                                        if event_index.is_multiple_of(2) {
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
    let mut group = c.benchmark_group("realistic_phase1/r9_subscribed_write");

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
                    Some(SubscriptionEvent::Delta {
                        reset: true,
                        added,
                        updated,
                        ..
                    }) => {
                        assert!(!added.is_empty() || !updated.is_empty());
                    }
                    other => panic!("expected reset subscription event, got {other:?}"),
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

fn r10_sync_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic_phase1/r10_sync_fanout");

    for profile in [CI_S_PROFILE] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("project_board_s", profile.tasks),
            &profile,
            |b, &profile| {
                let writer = open_db(10);
                let server = open_core_db(11);
                let reader = open_db_with_author(12, READER_AUTHOR, false);

                let fixture = seed_fixture(&writer, profile);
                let project = fixture.projects[0];
                let subscribed_row = fixture.tasks[0];

                let (writer_transport, server_writer_transport) = byte_duplex();
                let _writer_upstream = writer.connect_upstream(writer_transport);
                let _writer_subscriber = server.accept_subscriber(server_writer_transport, AUTHOR);

                let (reader_transport, server_reader_transport) = byte_duplex();
                let _reader_upstream = reader.connect_upstream(reader_transport);
                let _reader_subscriber =
                    server.accept_subscriber(server_reader_transport, READER_AUTHOR);

                let query = project_board_query(&reader, project);
                let mut subscription = block_on(reader.subscribe(&query, global_subscribe_opts()))
                    .expect("subscribe reader project board");
                assert!(drain_opened(block_on(subscription.next_event()), "reader board") == 0);

                writer.tick().expect("ship seeded writer rows");
                server.tick().expect("ingest seeded writer rows");
                reader.tick().expect("announce reader subscription");
                server.tick().expect("serve reader subscription");
                reader.tick().expect("apply reader subscription snapshot");
                assert!(
                    drain_delta(block_on(subscription.next_event()), "reader board seeded") > 0
                );

                let mut update_index = 0usize;
                b.iter(|| {
                    wait_local(
                        writer
                            .update(
                                "tasks",
                                subscribed_row,
                                BTreeMap::from([
                                    (
                                        "status".to_owned(),
                                        Value::String(
                                            if update_index.is_multiple_of(2) {
                                                "doing"
                                            } else {
                                                "review"
                                            }
                                            .to_owned(),
                                        ),
                                    ),
                                    (
                                        "updated_at".to_owned(),
                                        Value::U64((profile.tasks + update_index) as u64),
                                    ),
                                ]),
                            )
                            .expect("writer project-board update"),
                    );
                    update_index += 1;

                    writer.tick().expect("ship writer update");
                    server.tick().expect("fan out writer update");
                    reader.tick().expect("apply reader update");

                    let delivered =
                        drain_delta(block_on(subscription.next_event()), "reader board update");
                    assert!(delivered > 0);
                    black_box(delivered)
                });
            },
        );
    }

    group.finish();
}

fn r11_byte_wire_resume(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic_phase1/r11_byte_wire_resume");

    for profile in [CI_S_PROFILE] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("tasks_s", profile.tasks),
            &profile,
            |b, &profile| {
                b.iter(|| {
                    let writer = open_db(110);
                    let server = open_core_db(111);
                    let client = open_db_with_author(112, READER_AUTHOR, false);
                    let fixture = seed_resume_fixture(&writer, profile);
                    let subscribed_row = fixture.tasks[0];
                    let prepared = client
                        .prepare_query(&Query::from("tasks"))
                        .expect("prepare resumed tasks query");

                    let (writer_transport, server_writer_transport) =
                        byte_duplex_with_session(AUTHOR, 1);
                    let writer_upstream = writer.connect_upstream(writer_transport);
                    let writer_subscriber =
                        server.accept_subscriber(server_writer_transport, AUTHOR);
                    writer.tick().expect("ship resume seed rows");
                    server.tick().expect("ingest resume seed rows");
                    assert!(writer.detach_connection(&writer_upstream));
                    assert!(server.detach_connection(&writer_subscriber));

                    let (client_transport, server_transport) =
                        byte_duplex_with_session(READER_AUTHOR, 2);
                    let upstream = client.connect_upstream(client_transport);
                    let subscriber = server.accept_subscriber(server_transport, READER_AUTHOR);

                    let mut subscription =
                        block_on(client.subscribe(&prepared, global_subscribe_opts()))
                            .expect("subscribe client tasks");

                    assert_eq!(
                        drain_opened(block_on(subscription.next_event()), "client tasks"),
                        0
                    );

                    client.tick().expect("announce client tasks subscription");
                    server.tick().expect("serve full task snapshot");
                    let full_bytes = subscriber
                        .borrow()
                        .last_resume_bytes()
                        .expect("full current-row bytes");
                    client.tick().expect("apply full task snapshot");
                    client.tick().expect("materialize full task snapshot event");

                    let current_rows =
                        drain_delta(subscription.try_next_event(), "client tasks seeded");
                    assert_eq!(current_rows, profile.tasks);
                    assert!(full_bytes > 0);

                    server.tick().expect("refresh served current rows");
                    client.tick().expect("apply served cursor state");

                    let cursor = subscriber
                        .borrow_mut()
                        .take_resume_cursor()
                        .expect("take subscriber resume cursor");
                    assert!(client.detach_connection(&upstream));
                    assert!(server.detach_connection(&subscriber));

                    let changed_status = "resume-canary";
                    wait_local(
                        writer
                            .update(
                                "tasks",
                                subscribed_row,
                                BTreeMap::from([
                                    (
                                        "status".to_owned(),
                                        Value::String(changed_status.to_owned()),
                                    ),
                                    ("updated_at".to_owned(), Value::U64(9_001)),
                                ]),
                            )
                            .expect("writer disconnected task update"),
                    );
                    let (writer_transport, server_writer_transport) =
                        byte_duplex_with_session(AUTHOR, 3);
                    let writer_upstream = writer.connect_upstream(writer_transport);
                    let writer_subscriber =
                        server.accept_subscriber(server_writer_transport, AUTHOR);
                    writer.tick().expect("ship disconnected task update");
                    server.tick().expect("ingest disconnected task update");
                    assert!(writer.detach_connection(&writer_upstream));
                    assert!(server.detach_connection(&writer_subscriber));

                    let (client_transport, server_transport) =
                        byte_duplex_with_session(READER_AUTHOR, 4);
                    let _resumed_upstream = client.connect_upstream(client_transport);
                    let resumed =
                        server.accept_subscriber_with_resume(server_transport, READER_AUTHOR, cursor);

                    client.tick().expect("announce resumed tasks subscription");
                    server.tick().expect("serve task resume catch-up");
                    client.tick().expect("apply task resume catch-up");
                    client.tick().expect("materialize task resume event");

                    let resume_bytes = resumed
                        .borrow()
                        .last_resume_bytes()
                        .expect("resume catch-up bytes");
                    assert!(resume_bytes > 0);
                    assert!(
                        resume_bytes < full_bytes,
                        "resume catch-up ({resume_bytes}) should be smaller than full send ({full_bytes})"
                    );

                    let delivered =
                        drain_delta(block_on(subscription.next_event()), "client tasks resumed");
                    assert!(delivered > 0);
                    let rows = client.read(&prepared).expect("read resumed task rows");
                    let changed = rows
                        .iter()
                        .find(|row| row.row_uuid() == subscribed_row)
                        .expect("changed task visible on client");
                    assert_eq!(
                        changed.cell_at(2),
                        Some(Value::String(changed_status.to_owned()))
                    );
                    black_box(resume_bytes)
                });
            },
        );
    }

    group.finish();
}

fn r12_recursive_permissions(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic_phase1/r12_recursive_permissions");
    group.throughput(Throughput::Elements(2));

    group.bench_function("docs_recursive_read_s", |b| {
        let db = open_recursive_permissions_db(120);
        seed_recursive_permissions_fixture(&db);
        let query = recursive_docs_query(&db);
        let read_opts = ReadOpts::default();

        b.iter(|| {
            let rows = block_on(db.all_for_identity(&query, read_opts.clone(), READER_AUTHOR))
                .expect("read recursive docs for reader");
            assert_recursive_docs_visible(&rows);

            let mut subscription =
                block_on(db.subscribe_for_identity(&query, read_opts.clone(), READER_AUTHOR))
                    .expect("subscribe recursive docs for reader");
            match block_on(subscription.next_event()) {
                Some(SubscriptionEvent::Delta {
                    reset: true,
                    added,
                    updated,
                    ..
                }) => {
                    let mut rows = added;
                    rows.extend(updated);
                    assert_recursive_docs_visible(&rows);
                }
                other => panic!("expected recursive docs reset event, got {other:?}"),
            }

            black_box(rows.len())
        });
    });

    group.finish();
}

fn r13_permission_filtered_resume(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic_phase1/r13_permission_filtered_resume");
    group.throughput(Throughput::Elements(1));

    group.bench_function("docs_recursive_resume_s", |b| {
        b.iter(|| {
            let writer = open_recursive_permissions_db_with_author(130, AuthorId::SYSTEM, false);
            let server = open_recursive_permissions_db_with_author(131, AuthorId::SYSTEM, true);
            let client = open_recursive_permissions_db_with_author(132, READER_AUTHOR, false);
            seed_permission_resume_fixture(&writer);
            let prepared = client
                .prepare_query(&Query::from("docs"))
                .expect("prepare permission-filtered docs query");

            let (writer_transport, server_writer_transport) =
                byte_duplex_with_session(AuthorId::SYSTEM, 13_001);
            let writer_upstream = writer.connect_upstream(writer_transport);
            let writer_subscriber =
                server.accept_subscriber(server_writer_transport, AuthorId::SYSTEM);
            writer.tick().expect("ship permission seed rows");
            server.tick().expect("ingest permission seed rows");
            assert!(writer.detach_connection(&writer_upstream));
            assert!(server.detach_connection(&writer_subscriber));

            let (client_transport, server_transport) =
                byte_duplex_with_session(READER_AUTHOR, 13_002);
            let upstream = client.connect_upstream(client_transport);
            let subscriber = server.accept_subscriber(server_transport, READER_AUTHOR);
            let mut subscription = block_on(client.subscribe(&prepared, global_subscribe_opts()))
                .expect("subscribe permission-filtered docs");
            assert_eq!(
                drain_opened(block_on(subscription.next_event()), "permission docs"),
                0
            );

            client
                .tick()
                .expect("announce permission docs subscription");
            server.tick().expect("serve full permission docs snapshot");
            let full_bytes = subscriber
                .borrow()
                .last_resume_bytes()
                .expect("full permission current-row bytes");
            client.tick().expect("apply full permission docs snapshot");
            client
                .tick()
                .expect("materialize full permission docs snapshot event");
            let seeded = drain_optional_permission_rows(subscription.try_next_event());
            assert!(full_bytes > 0);
            let rows = client
                .read(&prepared)
                .expect("read initial permission-filtered docs");
            assert_permission_resume_docs(&rows, &[RESUME_DOC_DIRECT, RESUME_DOC_REVOKED]);
            if seeded > 0 {
                assert_eq!(seeded, rows.len());
            }

            server.tick().expect("refresh permission docs cursor");
            client.tick().expect("apply permission docs cursor state");
            let cursor = subscriber
                .borrow_mut()
                .take_resume_cursor()
                .expect("take permission subscriber resume cursor");
            assert!(client.detach_connection(&upstream));
            assert!(server.detach_connection(&subscriber));

            wait_local(
                writer
                    .update(
                        "doc_access",
                        RESUME_ACCESS_REVOKED,
                        recursive_doc_access_cells(RESUME_DOC_REVOKED, RECURSIVE_HIDDEN_TEAM),
                    )
                    .expect("hide disconnected doc access before revoke"),
            );
            wait_local(
                writer
                    .delete("doc_access", RESUME_ACCESS_REVOKED)
                    .expect("revoke disconnected doc access"),
            );
            wait_local(
                writer
                    .insert_with_id(
                        "doc_access",
                        RESUME_ACCESS_GRANTED,
                        recursive_doc_access_cells(RESUME_DOC_GRANTED, RECURSIVE_PARENT_TEAM),
                    )
                    .expect("grant disconnected doc access"),
            );

            let (writer_transport, server_writer_transport) =
                byte_duplex_with_session(AuthorId::SYSTEM, 13_003);
            let writer_upstream = writer.connect_upstream(writer_transport);
            let writer_subscriber =
                server.accept_subscriber(server_writer_transport, AuthorId::SYSTEM);
            writer.tick().expect("ship disconnected permission changes");
            server
                .tick()
                .expect("ingest disconnected permission changes");
            writer
                .tick()
                .expect("ship settled disconnected permission changes");
            server
                .tick()
                .expect("ingest settled disconnected permission changes");
            assert!(writer.detach_connection(&writer_upstream));
            assert!(server.detach_connection(&writer_subscriber));

            let (client_transport, server_transport) =
                byte_duplex_with_session(READER_AUTHOR, 13_004);
            let _resumed_upstream = client.connect_upstream(client_transport);
            let resumed =
                server.accept_subscriber_with_resume(server_transport, READER_AUTHOR, cursor);

            client
                .tick()
                .expect("announce resumed permission docs subscription");
            server.tick().expect("serve permission resume catch-up");
            client.tick().expect("apply permission resume catch-up");
            client.tick().expect("materialize permission resume event");
            server
                .tick()
                .expect("serve settled permission resume state");
            client
                .tick()
                .expect("apply settled permission resume state");
            client
                .tick()
                .expect("materialize settled permission resume state");

            let resume_bytes = resumed
                .borrow()
                .last_resume_bytes()
                .expect("permission resume catch-up bytes");
            assert!(resume_bytes > 0);

            let (added, updated, removed) =
                drain_permission_resume_delta(subscription.try_next_event());
            if added + updated + removed > 0 {
                assert_eq!(added + updated, 1);
                assert_eq!(removed, 1);
            }
            let rows = client
                .read(&prepared)
                .expect("read final permission-filtered docs");
            assert_permission_resume_docs(&rows, &[RESUME_DOC_DIRECT, RESUME_DOC_GRANTED]);

            black_box((resume_bytes, full_bytes, added, updated, removed))
        });
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = r1_crud, r2_reads, r3_rocksdb_cold_load, r4_hot_task_history, r9_subscribed_write, r10_sync_fanout, r11_byte_wire_resume, r12_recursive_permissions, r13_permission_filtered_resume
}
criterion_main!(benches);
