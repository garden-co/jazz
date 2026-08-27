//! Small active core realistic benchmark slice.
//!
//! This intentionally exercises `jazz::db::Db<MemoryStorage>` directly, without
//! the legacy `RuntimeCore`, `SchemaManager`, or `SyncManager` stack.

#![recursion_limit = "256"]
#![allow(clippy::single_element_loop, dead_code)]

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::env;
#[cfg(target_os = "linux")]
use std::fs;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::path::Path;
use std::rc::Rc;
use std::time::{Duration, Instant};

mod schema_fixture;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use jazz::db::{
    Db, DbConfig, DbIdentity, LocalUpdates, Propagation, ReadOpts, SeededRowIdSource,
    SubscriptionEvent, WireTransportAdapter, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::storage::{MemoryStorage, OrderedKvStorage};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, all_of, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{CmpOp, RelValueRef};
use jazz::tools::{
    ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder, Value as PublicValue,
};
use jazz::tx::DurabilityTier;
use jazz::wire::{
    FEATURE_SESSION_FRAME, FEATURE_STRUCTURED_ERRORS, FEATURE_SYNC_MESSAGE_PAYLOAD, TransportError,
    WIRE_PROTOCOL_VERSION, WireSession, WireTransport,
};
use jazz_storage_rocksdb::RocksDbStorage;
use tempfile::TempDir;

type BenchDb = Db<MemoryStorage>;
type RocksBenchDb = Db<RocksDbStorage>;

fn author() -> AuthorSubject {
    AuthorSubject::for_test_uuid(uuid::uuid!("00000000-0000-0000-0000-0000000000a1"))
}

fn reader_author() -> AuthorSubject {
    AuthorSubject::for_test_uuid(uuid::uuid!("00000000-0000-0000-0000-0000000000b2"))
}
const R3_REOPEN_SEED: u64 = 31;

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

const S_PROFILE: SmallProfile = SmallProfile {
    users: 10,
    organizations: 3,
    projects: 30,
    tasks: 3_000,
    comments: 12_000,
    watchers_per_task: 1,
    activity_events: 9_000,
};

const M_PROFILE: SmallProfile = SmallProfile {
    users: 100,
    organizations: 20,
    projects: 500,
    tasks: 100_000,
    comments: 400_000,
    watchers_per_task: 2,
    activity_events: 250_000,
};

fn schema() -> JazzSchema {
    schema_fixture::compile(
        SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("users")
                    .column("name", ColumnType::Text)
                    .column("handle", ColumnType::Text),
            )
            .table(
                TableSchemaBuilder::new("organizations")
                    .column("name", ColumnType::Text)
                    .column("created_at", ColumnType::Timestamp),
            )
            .table(
                TableSchemaBuilder::new("memberships")
                    .fk_column("organization", "organizations")
                    .fk_column("user", "users")
                    .column("role", ColumnType::Text),
            )
            .table(
                TableSchemaBuilder::new("projects")
                    .fk_column("organization", "organizations")
                    .column("name", ColumnType::Text)
                    .column("slug", ColumnType::Text)
                    .fk_column("owner", "users"),
            )
            .table(
                TableSchemaBuilder::new("tasks")
                    .fk_column("project", "projects")
                    .column("title", ColumnType::Text)
                    .column("status", ColumnType::Text)
                    .column("priority", ColumnType::Timestamp)
                    .fk_column("assignee", "users")
                    .column("updated_at", ColumnType::Timestamp),
            )
            .table(
                TableSchemaBuilder::new("comments")
                    .fk_column("task", "tasks")
                    .fk_column("author", "users")
                    .column("body", ColumnType::Text)
                    .column("created_at", ColumnType::Timestamp),
            )
            .table(
                TableSchemaBuilder::new("watchers")
                    .fk_column("task", "tasks")
                    .fk_column("user", "users"),
            )
            .table(
                TableSchemaBuilder::new("activity")
                    .fk_column("project", "projects")
                    .fk_column("task", "tasks")
                    .fk_column("actor", "users")
                    .column("kind", ColumnType::Text)
                    .column("created_at", ColumnType::Timestamp),
            ),
    )
}

fn recursive_permissions_schema() -> JazzSchema {
    let recursive_policy = schema_fixture::reachable_access(
        "doc_access",
        "doc",
        "team",
        "teams",
        "team_edges",
        "member",
        "parent",
        RelValueRef::SessionRef(vec!["user".to_owned()]),
    );

    schema_fixture::compile(
        SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("title", ColumnType::Text)
                    .column("kind", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(recursive_policy)),
            )
            .table(TableSchemaBuilder::new("teams").column("name", ColumnType::Text))
            .table(
                TableSchemaBuilder::new("doc_access")
                    .fk_column("doc", "docs")
                    .fk_column("team", "teams"),
            )
            .table(
                TableSchemaBuilder::new("team_edges")
                    .fk_column("member", "teams")
                    .fk_column("parent", "teams"),
            ),
    )
}

fn claim_resume_schema() -> JazzSchema {
    schema_fixture::compile(
        SchemaBuilder::new().table(
            TableSchemaBuilder::new("claim_docs")
                .column("title", ColumnType::Text)
                .policies(TablePolicies::new().with_select(PolicyExpr::SessionCmp {
                    path: vec!["access".to_owned()],
                    op: CmpOp::Eq,
                    value: PublicValue::Text("allowed".to_owned()),
                })),
        ),
    )
}

fn open_db(seed: u64) -> BenchDb {
    open_db_with_author(seed, author(), false)
}

fn open_core_db(seed: u64) -> BenchDb {
    open_db_with_author(seed, AuthorSubject::SYSTEM, true)
}

fn open_db_with_author(seed: u64, author: AuthorSubject, history_complete: bool) -> BenchDb {
    open_db_with_schema(seed, author, history_complete, schema())
}

fn open_db_with_schema(
    seed: u64,
    author: AuthorSubject,
    history_complete: bool,
    schema: JazzSchema,
) -> BenchDb {
    open_db_with_storage(
        seed,
        author,
        history_complete,
        schema,
        |refs| MemoryStorage::new(refs).expect("valid memory storage families"),
        "open core realistic benchmark db",
    )
}

fn open_rocks_db_with_author(
    seed: u64,
    author: AuthorSubject,
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
    author: AuthorSubject,
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
    identity: AuthorSubject,
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
                db.insert(
                    "users",
                    user_cells(index),
                    jazz::db::InsertOptions {
                        row_id: Some(row),
                        ..Default::default()
                    },
                )
                .expect("seed user"),
            );
            row
        })
        .collect::<Vec<_>>();

    let organizations = (0..profile.organizations)
        .map(|index| {
            let row = row_uuid(0x1f, index);
            wait_local(
                db.insert(
                    "organizations",
                    organization_cells(index),
                    jazz::db::InsertOptions {
                        row_id: Some(row),
                        ..Default::default()
                    },
                )
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
                Default::default(),
            )
            .expect("seed membership"),
        );
    }

    let projects = (0..profile.projects)
        .map(|index| {
            let row = row_uuid(0x22, index);
            wait_local(
                db.insert(
                    "projects",
                    project_cells(index, &organizations, &users),
                    jazz::db::InsertOptions {
                        row_id: Some(row),
                        ..Default::default()
                    },
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
                db.insert(
                    "tasks",
                    task_cells(index, &projects, &users),
                    jazz::db::InsertOptions {
                        row_id: Some(row),
                        ..Default::default()
                    },
                )
                .expect("seed task"),
            );
            row
        })
        .collect::<Vec<_>>();

    for index in 0..profile.comments {
        wait_local(
            db.insert(
                "comments",
                comment_cells(index, &tasks, &users),
                Default::default(),
            )
            .expect("seed comment"),
        );
    }

    for (task_index, task) in tasks.iter().enumerate() {
        for watcher_offset in 0..profile.watchers_per_task {
            let user = users[(task_index + watcher_offset) % users.len()];
            wait_local(
                db.insert("watchers", watcher_cells(*task, user), Default::default())
                    .expect("seed watcher"),
            );
        }
    }

    for index in 0..profile.activity_events {
        wait_local(
            db.insert(
                "activity",
                activity_cells(index, &projects, &tasks, &users),
                Default::default(),
            )
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
                db.insert(
                    "users",
                    user_cells(index),
                    jazz::db::InsertOptions {
                        row_id: Some(row),
                        ..Default::default()
                    },
                )
                .expect("seed resume user"),
            );
            row
        })
        .collect::<Vec<_>>();

    let organizations = (0..profile.organizations)
        .map(|index| {
            let row = row_uuid(0x42, index);
            wait_local(
                db.insert(
                    "organizations",
                    organization_cells(index),
                    jazz::db::InsertOptions {
                        row_id: Some(row),
                        ..Default::default()
                    },
                )
                .expect("seed resume organization"),
            );
            row
        })
        .collect::<Vec<_>>();

    let projects = (0..profile.projects)
        .map(|index| {
            let row = row_uuid(0x43, index);
            wait_local(
                db.insert(
                    "projects",
                    project_cells(index, &organizations, &users),
                    jazz::db::InsertOptions {
                        row_id: Some(row),
                        ..Default::default()
                    },
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
                db.insert(
                    "tasks",
                    task_cells(index, &projects, &users),
                    jazz::db::InsertOptions {
                        row_id: Some(row),
                        ..Default::default()
                    },
                )
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
const CLAIM_RESUME_DOC_A: RowUuid = RowUuid(uuid::uuid!("13000000-0000-0000-0000-000000000301"));
const CLAIM_RESUME_DOC_B: RowUuid = RowUuid(uuid::uuid!("13000000-0000-0000-0000-000000000302"));

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
        AuthorSubject::SYSTEM,
        false,
        recursive_permissions_schema(),
    )
}

fn open_recursive_permissions_db_with_author(
    seed: u64,
    author: AuthorSubject,
    history_complete: bool,
) -> BenchDb {
    open_db_with_schema(
        seed,
        author,
        history_complete,
        recursive_permissions_schema(),
    )
}

fn open_claim_resume_db(seed: u64, author: AuthorSubject, history_complete: bool) -> BenchDb {
    open_db_with_schema(seed, author, history_complete, claim_resume_schema())
}

fn seed_recursive_permissions_fixture(db: &BenchDb) {
    for (team, name) in [
        (RECURSIVE_READER_TEAM, "reader"),
        (RECURSIVE_PARENT_TEAM, "parent"),
        (RECURSIVE_HIDDEN_TEAM, "hidden"),
    ] {
        wait_local(
            db.insert(
                "teams",
                recursive_team_cells(name),
                jazz::db::InsertOptions {
                    row_id: Some(team),
                    ..Default::default()
                },
            )
            .expect("seed recursive team"),
        );
    }

    for (doc, title, kind) in [
        (RECURSIVE_DOC_DIRECT, "direct", "visible"),
        (RECURSIVE_DOC_CLOSURE, "closure", "visible"),
        (RECURSIVE_DOC_HIDDEN, "hidden", "hidden"),
    ] {
        wait_local(
            db.insert(
                "docs",
                recursive_doc_cells(title, kind),
                jazz::db::InsertOptions {
                    row_id: Some(doc),
                    ..Default::default()
                },
            )
            .expect("seed recursive doc"),
        );
    }

    for (doc, team) in [
        (RECURSIVE_DOC_DIRECT, RECURSIVE_READER_TEAM),
        (RECURSIVE_DOC_CLOSURE, RECURSIVE_PARENT_TEAM),
        (RECURSIVE_DOC_HIDDEN, RECURSIVE_HIDDEN_TEAM),
    ] {
        wait_local(
            db.insert(
                "doc_access",
                recursive_doc_access_cells(doc, team),
                Default::default(),
            )
            .expect("seed recursive doc access"),
        );
    }

    wait_local(
        db.insert(
            "team_edges",
            recursive_team_edge_cells(RECURSIVE_READER_TEAM, RECURSIVE_PARENT_TEAM),
            Default::default(),
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
            db.insert(
                "teams",
                recursive_team_cells(name),
                jazz::db::InsertOptions {
                    row_id: Some(team),
                    ..Default::default()
                },
            )
            .expect("seed resume permission team"),
        );
    }

    wait_local(
        db.insert(
            "team_edges",
            recursive_team_edge_cells(RECURSIVE_READER_TEAM, RECURSIVE_PARENT_TEAM),
            jazz::db::InsertOptions {
                row_id: Some(RESUME_EDGE_READER_PARENT),
                ..Default::default()
            },
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
            db.insert(
                "docs",
                recursive_doc_cells(title, kind),
                jazz::db::InsertOptions {
                    row_id: Some(doc),
                    ..Default::default()
                },
            )
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
            db.insert(
                "doc_access",
                recursive_doc_access_cells(doc, team),
                jazz::db::InsertOptions {
                    row_id: Some(access),
                    ..Default::default()
                },
            )
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

#[derive(Clone, Copy, Debug)]
enum PermissionResumeChurn {
    Unchanged,
    Grant,
    Revoke,
    GrantAndRevoke,
}

#[derive(Clone, Copy, Debug)]
enum ClaimResumeChurn {
    Revoke,
    Restore,
}

impl ClaimResumeChurn {
    fn name(self) -> &'static str {
        match self {
            Self::Revoke => "claim_revoke",
            Self::Restore => "claim_restore",
        }
    }

    fn initial_access(self) -> &'static str {
        match self {
            Self::Revoke => "allowed",
            Self::Restore => "denied",
        }
    }

    fn resumed_access(self) -> &'static str {
        match self {
            Self::Revoke => "denied",
            Self::Restore => "allowed",
        }
    }
}

impl PermissionResumeChurn {
    fn name(self) -> &'static str {
        match self {
            Self::Unchanged => "unchanged",
            Self::Grant => "grant_only",
            Self::Revoke => "revoke_only",
            Self::GrantAndRevoke => "grant_and_revoke",
        }
    }

    fn grants(self) -> bool {
        matches!(self, Self::Grant | Self::GrantAndRevoke)
    }

    fn revokes(self) -> bool {
        matches!(self, Self::Revoke | Self::GrantAndRevoke)
    }
}

fn assert_resume_delta(
    event: Option<SubscriptionEvent>,
    expected_added: &[RowUuid],
    expected_removed: &[RowUuid],
) -> (usize, usize, usize) {
    let (added, updated, removed) = match event {
        Some(SubscriptionEvent::Delta {
            added,
            updated,
            removed,
            ..
        }) => (added, updated, removed),
        None if expected_added.is_empty() && expected_removed.is_empty() => {
            return (0, 0, 0);
        }
        None => panic!("permission-filtered resume emitted no delta event"),
        other => panic!("expected permission-filtered resume delta event, got {other:?}"),
    };
    let actual_added = added
        .iter()
        .chain(updated.iter())
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    let actual_removed = removed
        .iter()
        .map(|row| row.row_uuid)
        .collect::<BTreeSet<_>>();
    assert_eq!(actual_added, expected_added.iter().copied().collect());
    assert_eq!(actual_removed, expected_removed.iter().copied().collect());
    assert_eq!(added.len() + updated.len(), expected_added.len());
    assert_eq!(removed.len(), expected_removed.len());
    (added.len(), updated.len(), removed.len())
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
                            Default::default(),
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
                            Default::default(),
                        )
                        .expect("update task"),
                    );
                    update_index += 1;

                    wait_local(
                        db.delete("tasks", inserted_row, Default::default())
                            .expect("delete task"),
                    );
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

#[derive(Clone, Copy)]
struct R3Profile {
    id: &'static str,
    profile: SmallProfile,
}

fn r3_profiles() -> Vec<R3Profile> {
    let requested = env::var("JAZZ_R3_PROFILES").unwrap_or_else(|_| "ci".to_owned());
    requested
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| match value {
            "ci" => R3Profile {
                id: "ci",
                profile: CI_S_PROFILE,
            },
            "s" => R3Profile {
                id: "s",
                profile: S_PROFILE,
            },
            "m" => R3Profile {
                id: "m",
                profile: M_PROFILE,
            },
            other => panic!(
                "unknown JAZZ_R3_PROFILES entry {other:?}; expected a comma-separated subset of ci,s,m"
            ),
        })
        .collect()
}

#[derive(Debug)]
struct R3PhaseSample {
    storage_open: Duration,
    jazz_open: Duration,
    open_breakdown: Option<R3OpenBreakdown>,
    prepare: Duration,
    first_read: Duration,
    first_read_resolve_view: Duration,
    first_read_compile_program: Duration,
    first_read_select_plan: Duration,
    first_read_execute_plan: Duration,
    first_read_decode_materialize: Duration,
    first_read_finish_rows: Duration,
    first_read_apply_projection: Duration,
    first_read_unattributed: Duration,
    rows: usize,
}

#[derive(Debug)]
struct R3OpenBreakdown {
    catalogue_open: Duration,
    database_open: Duration,
    state_init: Duration,
    recover_storage: Duration,
    recover_catalogue_state: Duration,
    validate_current_rows: Duration,
    recover_global_times: Duration,
    recover_pending_and_rejected: Duration,
    recover_unclean_close: Duration,
    recover_known_state: Duration,
    rebuild_ahead_current: Duration,
    finalize_catalogue: Duration,
    validated_current_rows: usize,
    accepted_global_times: usize,
    global_time_records_scanned: usize,
    ahead_current_entries: usize,
}

#[derive(Clone, Copy)]
enum R3CacheMode {
    Warm,
    Evicted,
}

#[derive(Clone, Copy)]
enum R3CloseMode {
    Clean,
    Unclean,
}

impl R3CloseMode {
    fn id(self) -> &'static str {
        match self {
            Self::Clean => "db_close",
            Self::Unclean => "drop_without_close",
        }
    }
}

fn r3_close_modes() -> Vec<R3CloseMode> {
    let requested = env::var("JAZZ_R3_CLOSE_MODES").unwrap_or_else(|_| "unclean".to_owned());
    requested
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| match value {
            "clean" => R3CloseMode::Clean,
            "unclean" => R3CloseMode::Unclean,
            other => panic!(
                "unknown JAZZ_R3_CLOSE_MODES entry {other:?}; expected a comma-separated subset of clean,unclean"
            ),
        })
        .collect()
}

impl R3CacheMode {
    fn id(self) -> &'static str {
        match self {
            Self::Warm => "os_page_cache_uncontrolled_after_seed",
            Self::Evicted => "linux_posix_fadvise_dontneed",
        }
    }

    fn phase(self) -> &'static str {
        match self {
            Self::Warm => "reopen_warm_cache",
            Self::Evicted => "reopen_evicted_cache",
        }
    }
}

fn r3_cache_modes() -> Vec<R3CacheMode> {
    let requested = env::var("JAZZ_R3_CACHE_MODES").unwrap_or_else(|_| "warm".to_owned());
    requested
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| match value {
            "warm" => R3CacheMode::Warm,
            "evicted" => R3CacheMode::Evicted,
            other => panic!(
                "unknown JAZZ_R3_CACHE_MODES entry {other:?}; expected a comma-separated subset of warm,evicted"
            ),
        })
        .collect()
}

#[cfg(target_os = "linux")]
fn evict_path_from_linux_page_cache(path: &Path) {
    for entry in fs::read_dir(path).expect("read R3 RocksDB directory for cache eviction") {
        let entry = entry.expect("read R3 RocksDB cache eviction entry");
        let file_type = entry
            .file_type()
            .expect("read R3 RocksDB cache eviction file type");
        if file_type.is_dir() {
            evict_path_from_linux_page_cache(&entry.path());
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        let file = fs::File::open(entry.path()).expect("open R3 RocksDB file for cache eviction");
        file.sync_all()
            .expect("flush R3 RocksDB file before cache eviction");
        let result =
            unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
        assert_eq!(
            result,
            0,
            "posix_fadvise(DONTNEED) failed for {:?}: errno {result}",
            entry.path()
        );
    }
}

#[cfg(not(target_os = "linux"))]
fn evict_path_from_linux_page_cache(_path: &Path) {
    panic!("JAZZ_R3_CACHE_MODES=evicted is currently supported only on Linux");
}

fn open_rocks_db_with_phases(
    seed: u64,
    author: AuthorSubject,
    path: &Path,
) -> (RocksBenchDb, Duration, Duration, Option<R3OpenBreakdown>) {
    let schema = schema();
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();

    let storage_started = Instant::now();
    let storage =
        RocksDbStorage::open(path, &refs).expect("open realistic RocksDB phase receipt storage");
    let storage_open = storage_started.elapsed();

    let config = DbConfig::new(
        schema,
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([seed as u8; 16]),
            author,
        },
    )
    .with_id_source(SeededRowIdSource::new(seed));

    let jazz_started = Instant::now();
    #[cfg(feature = "r3-open-attribution")]
    let (db, open_breakdown) = {
        let (db, receipt) = block_on(Db::open_with_receipt_for_test(config))
            .expect("open attributed core realistic RocksDB phase receipt db");
        (
            db,
            Some(R3OpenBreakdown {
                catalogue_open: receipt.catalogue_open,
                database_open: receipt.database_open,
                state_init: receipt.state_init,
                recover_storage: receipt.recover_storage,
                recover_catalogue_state: receipt.recover_catalogue_state,
                validate_current_rows: receipt.validate_current_rows,
                recover_global_times: receipt.recover_global_times,
                recover_pending_and_rejected: receipt.recover_pending_and_rejected,
                recover_unclean_close: receipt.recover_unclean_close,
                recover_known_state: receipt.recover_known_state,
                rebuild_ahead_current: receipt.rebuild_ahead_current,
                finalize_catalogue: receipt.finalize_catalogue,
                validated_current_rows: receipt.validated_current_rows,
                accepted_global_times: receipt.accepted_global_times,
                global_time_records_scanned: receipt.global_time_records_scanned,
                ahead_current_entries: receipt.ahead_current_entries,
            }),
        )
    };
    #[cfg(not(feature = "r3-open-attribution"))]
    let (db, open_breakdown) = (
        block_on(Db::open(config)).expect("open core realistic RocksDB phase receipt db"),
        None,
    );
    let jazz_open = jazz_started.elapsed();

    (db, storage_open, jazz_open, open_breakdown)
}

fn measure_r3_phase_sample(
    path: &Path,
    project: RowUuid,
    expected_rows: usize,
    _sample: usize,
    cache_mode: R3CacheMode,
    close_mode: R3CloseMode,
) -> R3PhaseSample {
    if matches!(cache_mode, R3CacheMode::Evicted) {
        evict_path_from_linux_page_cache(path);
    }
    let (db, storage_open, jazz_open, open_breakdown) =
        open_rocks_db_with_phases(R3_REOPEN_SEED, author(), path);

    let prepare_started = Instant::now();
    let query = project_board_query(&db, project);
    let prepare = prepare_started.elapsed();

    let read_started = Instant::now();
    let (rows, read_profile) = db
        .read_profiled(&query)
        .expect("read warm-cache project board");
    let first_read = read_started.elapsed();
    assert_eq!(
        rows.len(),
        expected_rows,
        "R3 project-board result count changed"
    );
    if matches!(close_mode, R3CloseMode::Clean) {
        block_on(db.close()).expect("close R3 phase receipt db after measured read");
    }

    R3PhaseSample {
        storage_open,
        jazz_open,
        open_breakdown,
        prepare,
        first_read,
        first_read_resolve_view: read_profile.resolve_view,
        first_read_compile_program: read_profile.compile_program,
        first_read_select_plan: read_profile.select_plan,
        first_read_execute_plan: read_profile.execute_plan,
        first_read_decode_materialize: read_profile.decode_materialize,
        first_read_finish_rows: read_profile.finish_rows,
        first_read_apply_projection: read_profile.apply_projection,
        first_read_unattributed: first_read.saturating_sub(read_profile.total),
        rows: rows.len(),
    }
}

fn establish_r3_close_mode(path: &Path, close_mode: R3CloseMode) {
    let db = open_rocks_db_with_author(R3_REOPEN_SEED, author(), false, path);
    if matches!(close_mode, R3CloseMode::Clean) {
        block_on(db.close()).expect("establish clean-close marker before R3 phase samples");
    }
}

fn median_open_us(
    samples: &[R3PhaseSample],
    phase: impl Fn(&R3OpenBreakdown) -> Duration,
) -> Option<u64> {
    let mut values = samples
        .iter()
        .filter_map(|sample| sample.open_breakdown.as_ref())
        .map(|receipt| phase(receipt).as_micros().min(u64::MAX as u128) as u64)
        .collect::<Vec<_>>();
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    Some(values[values.len() / 2])
}

fn median_us(samples: &[R3PhaseSample], phase: impl Fn(&R3PhaseSample) -> Duration) -> u64 {
    let mut values = samples
        .iter()
        .map(|sample| phase(sample).as_micros().min(u64::MAX as u128) as u64)
        .collect::<Vec<_>>();
    values.sort_unstable();
    values[values.len() / 2]
}

fn emit_r3_phase_receipts(path: &Path, project: RowUuid, selected: R3Profile) {
    let sample_count = env::var("JAZZ_R3_PHASE_SAMPLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(3)
        .max(1);
    let expected_rows = selected.profile.tasks.div_ceil(selected.profile.projects);
    for close_mode in r3_close_modes() {
        establish_r3_close_mode(path, close_mode);
        for cache_mode in r3_cache_modes() {
            let samples = (0..sample_count)
                .map(|sample| {
                    measure_r3_phase_sample(
                        path,
                        project,
                        expected_rows,
                        sample,
                        cache_mode,
                        close_mode,
                    )
                })
                .collect::<Vec<_>>();

            println!(
                "{}",
                serde_json::json!({
                    "scenario": "r3_rocksdb_cold_load",
                    "phase": cache_mode.phase(),
                    "cache_mode": cache_mode.id(),
                    "close_mode": close_mode.id(),
                    "profile": selected.id,
                    "users": selected.profile.users,
                    "organizations": selected.profile.organizations,
                    "tasks": selected.profile.tasks,
                    "projects": selected.profile.projects,
                    "comments": selected.profile.comments,
                    "watchers_per_task": selected.profile.watchers_per_task,
                    "activity_events": selected.profile.activity_events,
                    "result_rows": samples[0].rows,
                    "samples": sample_count,
                    "durability": "wal_no_sync",
                    "total_p50_us": median_us(&samples, |sample| {
                        sample.storage_open + sample.jazz_open + sample.prepare + sample.first_read
                    }),
                    "storage_open_p50_us": median_us(&samples, |sample| sample.storage_open),
                    "jazz_open_p50_us": median_us(&samples, |sample| sample.jazz_open),
                    "catalogue_open_p50_us": median_open_us(&samples, |receipt| receipt.catalogue_open),
                    "database_open_p50_us": median_open_us(&samples, |receipt| receipt.database_open),
                    "state_init_p50_us": median_open_us(&samples, |receipt| receipt.state_init),
                    "recover_storage_p50_us": median_open_us(&samples, |receipt| receipt.recover_storage),
                    "recover_catalogue_state_p50_us": median_open_us(
                        &samples,
                        |receipt| receipt.recover_catalogue_state,
                    ),
                    "validate_current_rows_p50_us": median_open_us(
                        &samples,
                        |receipt| receipt.validate_current_rows,
                    ),
                    "recover_global_times_p50_us": median_open_us(
                        &samples,
                        |receipt| receipt.recover_global_times,
                    ),
                    "recover_pending_and_rejected_p50_us": median_open_us(
                        &samples,
                        |receipt| receipt.recover_pending_and_rejected,
                    ),
                    "recover_unclean_close_p50_us": median_open_us(
                        &samples,
                        |receipt| receipt.recover_unclean_close,
                    ),
                    "recover_known_state_p50_us": median_open_us(
                        &samples,
                        |receipt| receipt.recover_known_state,
                    ),
                    "rebuild_ahead_current_p50_us": median_open_us(
                        &samples,
                        |receipt| receipt.rebuild_ahead_current,
                    ),
                    "finalize_catalogue_p50_us": median_open_us(
                        &samples,
                        |receipt| receipt.finalize_catalogue,
                    ),
                    "validated_current_rows": samples[0]
                        .open_breakdown
                        .as_ref()
                        .map(|receipt| receipt.validated_current_rows),
                    "accepted_global_times": samples[0]
                        .open_breakdown
                        .as_ref()
                        .map(|receipt| receipt.accepted_global_times),
                    "global_time_records_scanned": samples[0]
                        .open_breakdown
                        .as_ref()
                        .map(|receipt| receipt.global_time_records_scanned),
                    "ahead_current_entries": samples[0]
                        .open_breakdown
                        .as_ref()
                        .map(|receipt| receipt.ahead_current_entries),
                    "prepare_p50_us": median_us(&samples, |sample| sample.prepare),
                    "first_read_p50_us": median_us(&samples, |sample| sample.first_read),
                    "first_read_resolve_view_p50_us": median_us(&samples, |sample| sample.first_read_resolve_view),
                    "first_read_compile_program_p50_us": median_us(&samples, |sample| sample.first_read_compile_program),
                    "first_read_select_plan_p50_us": median_us(&samples, |sample| sample.first_read_select_plan),
                    "first_read_execute_plan_p50_us": median_us(&samples, |sample| sample.first_read_execute_plan),
                    "first_read_decode_materialize_p50_us": median_us(&samples, |sample| sample.first_read_decode_materialize),
                    "first_read_finish_rows_p50_us": median_us(&samples, |sample| sample.first_read_finish_rows),
                    "first_read_apply_projection_p50_us": median_us(&samples, |sample| sample.first_read_apply_projection),
                    "first_read_unattributed_p50_us": median_us(&samples, |sample| sample.first_read_unattributed),
                })
            );
        }
    }
}

fn r3_rocksdb_cold_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic_phase1/r3_rocksdb_cold_load");

    for selected in r3_profiles() {
        let profile = selected.profile;
        let tempdir = TempDir::new().expect("create tempdir for RocksDB cold-load bench");
        let db_path = tempdir.path().join("realistic_phase1.rocksdb");
        let project = {
            let db = open_rocks_db_with_author(30, author(), false, &db_path);
            let fixture = seed_fixture(&db, profile);
            fixture.projects[0]
        };
        let expected_rows = profile.tasks.div_ceil(profile.projects);
        emit_r3_phase_receipts(&db_path, project, selected);

        if env::var_os("JAZZ_R3_PHASE_ONLY").is_some() {
            continue;
        }

        group.throughput(Throughput::Elements(profile.tasks as u64));
        group.bench_with_input(
            BenchmarkId::new("project_board_s", profile.tasks),
            &profile,
            |b, &_profile| {
                b.iter(|| {
                    let db = open_rocks_db_with_author(31, author(), false, &db_path);
                    let query = project_board_query(&db, project);
                    let rows = db.read(&query).expect("read cold project board");
                    assert_eq!(
                        rows.len(),
                        expected_rows,
                        "R3 project-board result count changed"
                    );
                    black_box(rows.len())
                });
            },
        );
    }

    group.finish();
}

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
                            Default::default(),
                        )
                        .expect("hot task update"),
                    );
                    wait_local(
                        db.insert(
                            "comments",
                            comment_cells(event_index, &[hot_task], &fixture.users),
                            Default::default(),
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
                            Default::default(),
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
                            Default::default(),
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
                let reader = open_db_with_author(12, reader_author(), false);

                let fixture = seed_fixture(&writer, profile);
                let project = fixture.projects[0];
                let subscribed_row = fixture.tasks[0];

                let (writer_transport, server_writer_transport) = byte_duplex();
                let _writer_upstream = block_on(writer.connect_upstream(writer_transport));
                let _writer_subscriber =
                    server.accept_subscriber(server_writer_transport, author());

                let (reader_transport, server_reader_transport) = byte_duplex();
                let _reader_upstream = block_on(reader.connect_upstream(reader_transport));
                let _reader_subscriber =
                    server.accept_subscriber(server_reader_transport, reader_author());

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
                                Default::default(),
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
                    let client = open_db_with_author(112, reader_author(), false);
                    let fixture = seed_resume_fixture(&writer, profile);
                    let subscribed_row = fixture.tasks[0];
                    let prepared = client
                        .prepare_query(&Query::from("tasks"))
                        .expect("prepare resumed tasks query");

                    let (writer_transport, server_writer_transport) =
                        byte_duplex_with_session(author(), 1);
                    let writer_upstream = block_on(writer.connect_upstream(writer_transport));
                    let writer_subscriber =
                        server.accept_subscriber(server_writer_transport, author());
                    writer.tick().expect("ship resume seed rows");
                    server.tick().expect("ingest resume seed rows");
                    assert!(writer.detach_connection(&writer_upstream));
                    assert!(server.detach_connection(&writer_subscriber));

                    let (client_transport, server_transport) =
                        byte_duplex_with_session(reader_author(), 2);
                    let upstream = block_on(client.connect_upstream(client_transport));
                    let subscriber = server.accept_subscriber(server_transport, reader_author());

                    let mut subscription =
                        block_on(client.subscribe(&prepared, global_subscribe_opts()))
                            .expect("subscribe client tasks");

                    assert_eq!(
                        drain_opened(block_on(subscription.next_event()), "client tasks"),
                        0
                    );

                    client.tick().expect("announce client tasks subscription");
                    server.tick().expect("serve full task snapshot");
                    let full_bytes = block_on(subscriber.lock())
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

                    let cursor = block_on(subscriber.lock())
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
                                ]), Default::default()
                            )
                            .expect("writer disconnected task update"),
                    );
                    let (writer_transport, server_writer_transport) =
                        byte_duplex_with_session(author(), 3);
                    let writer_upstream = block_on(writer.connect_upstream(writer_transport));
                    let writer_subscriber =
                        server.accept_subscriber(server_writer_transport, author());
                    writer.tick().expect("ship disconnected task update");
                    server.tick().expect("ingest disconnected task update");
                    assert!(writer.detach_connection(&writer_upstream));
                    assert!(server.detach_connection(&writer_subscriber));

                    let (client_transport, server_transport) =
                        byte_duplex_with_session(reader_author(), 4);
                    let _resumed_upstream = block_on(client.connect_upstream(client_transport));
                    let resumed =
                        server.accept_subscriber_with_resume(server_transport, reader_author(), cursor);

                    client.tick().expect("announce resumed tasks subscription");
                    server.tick().expect("serve task resume catch-up");
                    client.tick().expect("apply task resume catch-up");
                    client.tick().expect("materialize task resume event");

                    let resume_bytes = block_on(resumed.lock())
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
            let rows = block_on(db.all_for_identity(&query, read_opts.clone(), reader_author()))
                .expect("read recursive docs for reader");
            assert_recursive_docs_visible(&rows);

            let mut subscription =
                block_on(db.subscribe_for_identity(&query, read_opts.clone(), reader_author()))
                    .expect("subscribe recursive docs for reader");
            match block_on(subscription.next_event()) {
                Some(SubscriptionEvent::Delta {
                    reset: true,
                    added,
                    updated,
                    ..
                }) => {
                    let mut rows = added
                        .into_iter()
                        .map(|output| output.row)
                        .collect::<Vec<_>>();
                    rows.extend(updated.into_iter().map(|output| output.row));
                    assert_recursive_docs_visible(&rows);
                }
                other => panic!("expected recursive docs reset event, got {other:?}"),
            }

            black_box(rows.len())
        });
    });

    group.finish();
}

fn run_permission_filtered_resume(
    churn: PermissionResumeChurn,
) -> (Duration, usize, usize, usize, usize, usize) {
    let writer = open_recursive_permissions_db_with_author(130, AuthorSubject::SYSTEM, false);
    let server = open_recursive_permissions_db_with_author(131, AuthorSubject::SYSTEM, true);
    let client = open_recursive_permissions_db_with_author(132, reader_author(), false);
    seed_permission_resume_fixture(&writer);
    let prepared = client
        .prepare_query(&Query::from("docs"))
        .expect("prepare permission-filtered docs query");

    let (writer_transport, server_writer_transport) =
        byte_duplex_with_session(AuthorSubject::SYSTEM, 13_001);
    let writer_upstream = block_on(writer.connect_upstream(writer_transport));
    let writer_subscriber =
        server.accept_subscriber(server_writer_transport, AuthorSubject::SYSTEM);
    writer.tick().expect("ship permission seed rows");
    server.tick().expect("ingest permission seed rows");
    assert!(writer.detach_connection(&writer_upstream));
    assert!(server.detach_connection(&writer_subscriber));

    let (client_transport, server_transport) = byte_duplex_with_session(reader_author(), 13_002);
    let upstream = block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, reader_author());
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
    let full_bytes = block_on(subscriber.lock())
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
    let cursor = block_on(subscriber.lock())
        .take_resume_cursor()
        .expect("take permission subscriber resume cursor");
    assert!(client.detach_connection(&upstream));
    assert!(server.detach_connection(&subscriber));

    if churn.revokes() {
        wait_local(
            writer
                .update(
                    "doc_access",
                    RESUME_ACCESS_REVOKED,
                    recursive_doc_access_cells(RESUME_DOC_REVOKED, RECURSIVE_HIDDEN_TEAM),
                    Default::default(),
                )
                .expect("hide disconnected doc access before revoke"),
        );
        wait_local(
            writer
                .delete("doc_access", RESUME_ACCESS_REVOKED, Default::default())
                .expect("revoke disconnected doc access"),
        );
    }
    if churn.grants() {
        wait_local(
            writer
                .insert(
                    "doc_access",
                    recursive_doc_access_cells(RESUME_DOC_GRANTED, RECURSIVE_PARENT_TEAM),
                    jazz::db::InsertOptions {
                        row_id: Some(RESUME_ACCESS_GRANTED),
                        ..Default::default()
                    },
                )
                .expect("grant disconnected doc access"),
        );
    }

    if churn.grants() || churn.revokes() {
        let (writer_transport, server_writer_transport) =
            byte_duplex_with_session(AuthorSubject::SYSTEM, 13_003);
        let writer_upstream = block_on(writer.connect_upstream(writer_transport));
        let writer_subscriber =
            server.accept_subscriber(server_writer_transport, AuthorSubject::SYSTEM);
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
    }

    let server_query = recursive_docs_query(&server);
    let server_rows =
        block_on(server.all_for_identity(&server_query, ReadOpts::default(), reader_author()))
            .expect("read disconnected permission state on server");
    let mut expected_server_rows = vec![RESUME_DOC_DIRECT];
    if !churn.revokes() {
        expected_server_rows.push(RESUME_DOC_REVOKED);
    }
    if churn.grants() {
        expected_server_rows.push(RESUME_DOC_GRANTED);
    }
    assert_permission_resume_docs(&server_rows, &expected_server_rows);

    let (client_transport, server_transport) = byte_duplex_with_session(reader_author(), 13_004);
    let _resumed_upstream = block_on(client.connect_upstream(client_transport));
    let resumed = server.accept_subscriber_with_resume(server_transport, reader_author(), cursor);

    let resume_started = Instant::now();
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
    let resume_elapsed = resume_started.elapsed();

    let resume_bytes = block_on(resumed.lock())
        .last_resume_bytes()
        .expect("permission resume catch-up bytes");
    assert!(resume_bytes > 0);

    let unchanged_members = [RESUME_DOC_DIRECT, RESUME_DOC_REVOKED];
    let granted_member = [RESUME_DOC_GRANTED];
    let expected_added = match churn {
        PermissionResumeChurn::Unchanged => unchanged_members.as_slice(),
        PermissionResumeChurn::Grant | PermissionResumeChurn::GrantAndRevoke => {
            granted_member.as_slice()
        }
        PermissionResumeChurn::Revoke => &[],
    };
    let expected_removed = churn.revokes().then_some(RESUME_DOC_REVOKED);
    let (added, updated, removed) = assert_resume_delta(
        subscription.try_next_event(),
        expected_added,
        expected_removed.as_slice(),
    );
    assert!(subscription.try_next_event().is_none());
    // `Db::read` is intentionally a local-preview read; it may still
    // see retained row bodies after upstream membership is revoked.
    // The authoritative reconnect contract is the subscription delta
    // asserted above.

    (
        resume_elapsed,
        resume_bytes,
        full_bytes,
        added,
        updated,
        removed,
    )
}

fn run_claim_filtered_resume(
    churn: ClaimResumeChurn,
) -> (Duration, usize, usize, usize, usize, usize) {
    let writer = open_claim_resume_db(133, AuthorSubject::SYSTEM, false);
    let server = open_claim_resume_db(134, AuthorSubject::SYSTEM, true);
    let client = open_claim_resume_db(135, reader_author(), false);
    for (row, title) in [
        (CLAIM_RESUME_DOC_A, "claim-a"),
        (CLAIM_RESUME_DOC_B, "claim-b"),
    ] {
        wait_local(
            writer
                .insert(
                    "claim_docs",
                    BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]),
                    jazz::db::InsertOptions {
                        row_id: Some(row),
                        ..Default::default()
                    },
                )
                .expect("seed claim-resume doc"),
        );
    }
    let prepared = client
        .prepare_query(&Query::from("claim_docs"))
        .expect("prepare claim-filtered docs query");

    let (writer_transport, server_writer_transport) =
        byte_duplex_with_session(AuthorSubject::SYSTEM, 13_101);
    let writer_upstream = block_on(writer.connect_upstream(writer_transport));
    let writer_subscriber =
        server.accept_subscriber(server_writer_transport, AuthorSubject::SYSTEM);
    writer.tick().expect("ship claim-resume seed rows");
    server.tick().expect("ingest claim-resume seed rows");
    assert!(writer.detach_connection(&writer_upstream));
    assert!(server.detach_connection(&writer_subscriber));

    server.set_identity_claims(
        reader_author(),
        BTreeMap::from([(
            "access".to_owned(),
            Value::String(churn.initial_access().to_owned()),
        )]),
    );
    let (client_transport, server_transport) = byte_duplex_with_session(reader_author(), 13_102);
    let upstream = block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, reader_author());
    let mut subscription = block_on(client.subscribe(&prepared, global_subscribe_opts()))
        .expect("subscribe claim-filtered docs");
    assert_eq!(
        drain_opened(block_on(subscription.next_event()), "claim-filtered docs"),
        0
    );
    client.tick().expect("announce claim-filtered subscription");
    server.tick().expect("serve full claim-filtered snapshot");
    let full_bytes = block_on(subscriber.lock())
        .last_resume_bytes()
        .expect("full claim-filtered snapshot bytes");
    client.tick().expect("apply claim-filtered snapshot");
    client.tick().expect("materialize claim-filtered snapshot");
    let visible_claim_docs = [CLAIM_RESUME_DOC_A, CLAIM_RESUME_DOC_B];
    let expected_initial = match churn {
        ClaimResumeChurn::Revoke => visible_claim_docs.as_slice(),
        ClaimResumeChurn::Restore => &[],
    };
    assert_resume_delta(subscription.try_next_event(), expected_initial, &[]);

    server.tick().expect("refresh claim-filtered cursor");
    client.tick().expect("apply claim-filtered cursor state");
    let cursor = block_on(subscriber.lock())
        .take_resume_cursor()
        .expect("take claim-filtered resume cursor");
    assert!(client.detach_connection(&upstream));
    assert!(server.detach_connection(&subscriber));

    server.set_identity_claims(
        reader_author(),
        BTreeMap::from([(
            "access".to_owned(),
            Value::String(churn.resumed_access().to_owned()),
        )]),
    );
    let (client_transport, server_transport) = byte_duplex_with_session(reader_author(), 13_103);
    let _resumed_upstream = block_on(client.connect_upstream(client_transport));
    let resumed = server.accept_subscriber_with_resume(server_transport, reader_author(), cursor);

    let resume_started = Instant::now();
    client.tick().expect("announce resumed claim subscription");
    server.tick().expect("serve claim resume catch-up");
    client.tick().expect("apply claim resume catch-up");
    client.tick().expect("materialize claim resume event");
    server.tick().expect("serve settled claim resume state");
    client.tick().expect("apply settled claim resume state");
    client
        .tick()
        .expect("materialize settled claim resume state");
    let resume_elapsed = resume_started.elapsed();
    let resume_bytes = block_on(resumed.lock())
        .last_resume_bytes()
        .expect("claim resume catch-up bytes");
    assert!(resume_bytes > 0);

    let (expected_added, expected_removed) = match churn {
        ClaimResumeChurn::Revoke => (&[][..], visible_claim_docs.as_slice()),
        ClaimResumeChurn::Restore => (visible_claim_docs.as_slice(), &[][..]),
    };
    let (added, updated, removed) = assert_resume_delta(
        subscription.try_next_event(),
        expected_added,
        expected_removed,
    );
    assert!(subscription.try_next_event().is_none());
    (
        resume_elapsed,
        resume_bytes,
        full_bytes,
        added,
        updated,
        removed,
    )
}

fn r13_permission_filtered_resume(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic_phase1/r13_permission_filtered_resume");
    group.throughput(Throughput::Elements(1));

    for churn in [
        PermissionResumeChurn::Unchanged,
        PermissionResumeChurn::Grant,
        PermissionResumeChurn::Revoke,
        PermissionResumeChurn::GrantAndRevoke,
    ] {
        let (elapsed, resume_bytes, full_bytes, added, updated, removed) =
            run_permission_filtered_resume(churn);
        eprintln!(
            "{{\"scenario\":\"r13_permission_filtered_resume\",\"case\":\"{}\",\"resume_us\":{},\"resume_bytes\":{},\"full_bytes\":{},\"added\":{},\"updated\":{},\"removed\":{}}}",
            churn.name(),
            elapsed.as_micros(),
            resume_bytes,
            full_bytes,
            added,
            updated,
            removed,
        );
        group.bench_function(churn.name(), |b| {
            b.iter_custom(|iterations| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    let (resume_elapsed, resume_bytes, full_bytes, added, updated, removed) =
                        run_permission_filtered_resume(churn);
                    black_box((resume_bytes, full_bytes, added, updated, removed));
                    elapsed += resume_elapsed;
                }
                elapsed
            });
        });
    }
    for churn in [ClaimResumeChurn::Revoke, ClaimResumeChurn::Restore] {
        let (elapsed, resume_bytes, full_bytes, added, updated, removed) =
            run_claim_filtered_resume(churn);
        eprintln!(
            "{{\"scenario\":\"r13_permission_filtered_resume\",\"case\":\"{}\",\"resume_us\":{},\"resume_bytes\":{},\"full_bytes\":{},\"added\":{},\"updated\":{},\"removed\":{}}}",
            churn.name(),
            elapsed.as_micros(),
            resume_bytes,
            full_bytes,
            added,
            updated,
            removed,
        );
        group.bench_function(churn.name(), |b| {
            b.iter_custom(|iterations| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    let (resume_elapsed, resume_bytes, full_bytes, added, updated, removed) =
                        run_claim_filtered_resume(churn);
                    black_box((resume_bytes, full_bytes, added, updated, removed));
                    elapsed += resume_elapsed;
                }
                elapsed
            });
        });
    }

    group.finish();
}

fn guarded_benches(c: &mut Criterion) {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    r1_crud(c);
    r2_reads(c);
    r3_rocksdb_cold_load(c);
    r4_hot_task_history(c);
    r9_subscribed_write(c);
    r10_sync_fanout(c);
    r11_byte_wire_resume(c);
    r12_recursive_permissions(c);
    r13_permission_filtered_resume(c);
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = guarded_benches
}
criterion_main!(benches);
mod support;

use support::BenchFutureExt as _;
