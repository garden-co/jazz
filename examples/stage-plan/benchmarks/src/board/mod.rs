//! StagePlan's show board: the former W1 project-board workload, reframed as a
//! crew preparing shows. Users are crew members, projects are shows, tasks are
//! stage-prep tasks, and comments/activity are each task's discussion and log.
//! Fixture shapes, query shapes and timing boundaries are unchanged from W1.

pub mod crew_dashboard;

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, DeleteOptions, FileOptions, InsertOptions, PreparedQuery, ReadOpts,
    ReadTier, RestoreOptions, SubscriberId, SubscriptionEvent, SubscriptionStream, UpdateOptions,
    Upstream, WireTransportAdapter, WriteIdentity, block_on,
};
use jazz::db::{TransportError, WireTransport};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    CmpOp, ColumnType, PolicyExpr, PolicyValue, SchemaBuilder, TablePolicies, TableSchemaBuilder,
    Value as PublicValue,
};
use jazz::tx::DurabilityTier;
use tempfile::TempDir;

const USERS: usize = 10;
const PROJECTS: usize = 30;

fn policy_bench_identity() -> AuthorSubject {
    // Durable session writes need an account-bound principal. This fixture
    // preserves the existing Session identity and policy configuration.
    AuthorSubject::for_test_bytes([0x76; 16])
}

/// Seeded W1 read fixture. Setup is deliberately outside measured closures.
pub struct Fixture {
    db: Db,
    board: PreparedQuery,
    comments: PreparedQuery,
    activity: PreparedQuery,
    bounded_activity_page: PreparedQuery,
    maintained_activity: PreparedQuery,
    point_activity: PreparedQuery,
    activity_transition_row: RowUuid,
    task_transition_row: RowUuid,
    activity_transition_matching: bool,
    activity_update_identity: WriteIdentity,
}

pub struct MaintainedActivityFixture {
    fixture: Fixture,
    subscription: SubscriptionStream,
}

/// Prepared byte-wire reconnect with one task update made at the authority
/// while the client was away.
pub struct ResumeFixture {
    server: Db,
    client: Db,
    subscription: SubscriptionStream,
    fresh_post_update_bytes: usize,
    task_schema: jazz::schema::TableSchema,
    rows: ResumeRows,
    expected_rows: ResumeRows,
    resume_events: Vec<SubscriptionEvent>,
    /// The current link, kept alive while the client is connected.
    _link: Option<(Upstream, SubscriberId)>,
}

type ResumeRows = BTreeMap<RowUuid, BTreeMap<String, Option<Value>>>;

/// One direction of an in-memory byte link; the server-to-client side counts
/// the bytes it sends.
struct ByteDuplexTransport {
    outbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    inbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    sent: Option<Rc<RefCell<usize>>>,
}

impl WireTransport for ByteDuplexTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        if let Some(sent) = &self.sent {
            *sent.borrow_mut() += frame.len();
        }
        self.outbound.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.inbound.borrow_mut().pop_front()
    }
}

/// Client side, server side, and the bytes the server side has sent.
type ByteLink = (
    Box<dyn jazz::db::Transport>,
    Box<dyn jazz::db::Transport>,
    Rc<RefCell<usize>>,
);

fn byte_duplex() -> ByteLink {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let sent = Rc::new(RefCell::new(0));
    let client = ByteDuplexTransport {
        outbound: Rc::clone(&up),
        inbound: Rc::clone(&down),
        sent: None,
    };
    let server = ByteDuplexTransport {
        outbound: down,
        inbound: up,
        sent: Some(Rc::clone(&sent)),
    };
    (
        Box::new(WireTransportAdapter::current(client)),
        Box::new(WireTransportAdapter::current(server)),
        sent,
    )
}

/// Connect `client` to `server` over a fresh counted byte link.
fn connect(client: &Db, server: &Db) -> ((Upstream, SubscriberId), Rc<RefCell<usize>>) {
    let (client_side, server_side, sent) = byte_duplex();
    let upstream = block_on(client.connect_upstream(client_side));
    let subscriber = server
        .accept_subscriber(server_side, AuthorSubject::SYSTEM)
        .unwrap();
    ((upstream, subscriber), sent)
}

fn pump(client: &Db, server: &Db) {
    block_on(client.tick()).expect("announce W1 subscription");
    block_on(server.tick()).expect("serve W1 subscription");
    block_on(client.tick()).expect("apply W1 answer");
    block_on(client.tick()).expect("materialize W1 answer");
}

fn task_schema() -> jazz::schema::TableSchema {
    schema(false)
        .tables()
        .iter()
        .find(|table| table.name == "tasks")
        .unwrap()
        .clone()
}

fn subscribe_tasks(client: &Db) -> SubscriptionStream {
    let prepared = client
        .prepare_query(&Query::from("tasks"))
        .expect("prepare W1 tasks query");
    block_on(client.subscribe(
        &prepared,
        ReadOpts {
            tier: ReadTier::Remote,
            ..ReadOpts::default()
        },
    ))
    .expect("subscribe W1 tasks")
}

fn update_target_task(fixture: &Fixture) {
    let write = block_on(fixture.db.update(
        "tasks",
        fixture.task_transition_row,
        BTreeMap::from([(
            "status".to_owned(),
            Value::String("resume-canary".to_owned()),
        )]),
        UpdateOptions::default(),
    ))
    .expect("write W1 task update");
    block_on(write.wait(DurabilityTier::Local)).expect("settle W1 task update");
}

impl ResumeFixture {
    /// The authority is the seeded board itself; the client syncs every task,
    /// disconnects, and the authority updates one task meanwhile.
    pub fn memory(tasks: usize, comments: usize, activity_events: usize) -> Self {
        let board = Fixture::memory(tasks, comments, activity_events);
        let task_schema = task_schema();
        let client = open_memory_node(schema(false), 0x73, false);
        let (link, _) = connect(&client, &board.db);
        let mut subscription = subscribe_tasks(&client);
        let mut rows = ResumeRows::new();
        for _ in 0..512 {
            pump(&client, &board.db);
            while let Some(event) = subscription.try_next_event() {
                apply_resume_event(&mut rows, &task_schema, event);
            }
            if rows.len() == tasks {
                break;
            }
        }
        assert_eq!(rows.len(), tasks);
        let mut expected_rows = rows.clone();
        let target = expected_rows
            .get_mut(&board.task_transition_row)
            .expect("updated task was in the initial snapshot");
        let expected_status = Some(Value::String("resume-canary".to_owned()));
        assert_ne!(target.get("status"), Some(&expected_status));
        target.insert("status".to_owned(), expected_status);
        assert!(client.detach_connection(&link.0));
        board.db.disconnect_subscriber(link.1);

        update_target_task(&board);

        // Compare with a fresh client's full subscription at the same
        // post-update state, on an isolated control authority.
        let fresh_post_update_bytes = fresh_task_snapshot_bytes(tasks, comments, activity_events);

        Self {
            server: board.db,
            client,
            subscription,
            fresh_post_update_bytes,
            task_schema,
            rows,
            expected_rows,
            resume_events: Vec::new(),
            _link: None,
        }
    }

    /// The timed operation: reconnect and apply the catch-up. Returns the
    /// bytes the authority sent.
    pub fn resume_once(&mut self) -> usize {
        assert!(self._link.is_none(), "W1 resume fixture is single-use");
        let (link, sent) = connect(&self.client, &self.server);
        self._link = Some(link);
        pump(&self.client, &self.server);
        while let Some(event) = self.subscription.try_next_event() {
            self.resume_events.push(event);
        }
        *sent.borrow()
    }

    /// Correctness-only validation, deliberately outside the timed closure.
    /// The crate tests exercise the same benchmark sizes and resume path.
    pub fn assert_resume_receipt(&mut self, resume_bytes: usize) {
        assert!(!self.resume_events.is_empty());
        for event in self.resume_events.drain(..) {
            apply_resume_event(&mut self.rows, &self.task_schema, event);
        }
        // Identical membership and every column unchanged except the one
        // expected status.
        assert_eq!(self.rows, self.expected_rows);
        assert!(resume_bytes > 0);
        assert!(
            resume_bytes <= self.fresh_post_update_bytes,
            "resume must not exceed the equivalent fresh post-update response: resume={resume_bytes}, fresh={}",
            self.fresh_post_update_bytes
        );
    }
}

fn apply_resume_event(
    rows: &mut ResumeRows,
    task_schema: &jazz::schema::TableSchema,
    event: SubscriptionEvent,
) {
    let SubscriptionEvent::Delta {
        reset,
        added,
        updated,
        removed,
        ..
    } = event
    else {
        panic!("W1 subscription must remain open and authorized: {event:?}");
    };
    if reset {
        rows.clear();
        assert!(updated.is_empty(), "reset must supply complete rows");
    } else {
        for row in removed {
            assert_eq!(row.table, "tasks");
            assert!(rows.remove(&row.row_uuid).is_some());
        }
    }
    for (is_update, output) in added
        .into_iter()
        .map(|row| (false, row))
        .chain(updated.into_iter().map(|row| (true, row)))
    {
        assert_eq!(output.row.table(), "tasks");
        let cells = task_schema
            .columns
            .iter()
            .map(|column| (column.name().to_owned(), output.row.cell(column.name())))
            .collect();
        let previous = rows.insert(output.row.row_uuid(), cells);
        assert_eq!(
            previous.is_some(),
            is_update,
            "duplicate add or unknown update"
        );
    }
}

fn fresh_task_snapshot_bytes(tasks: usize, comments: usize, activity_events: usize) -> usize {
    let board = Fixture::memory(tasks, comments, activity_events);
    update_target_task(&board);
    let client = open_memory_node(schema(false), 0x75, false);
    let (_link, sent) = connect(&client, &board.db);
    let mut subscription = subscribe_tasks(&client);
    let mut rows = 0;
    let mut updated_rows = 0;
    for _ in 0..512 {
        pump(&client, &board.db);
        while let Some(event) = subscription.try_next_event() {
            if let SubscriptionEvent::Delta { added, .. } = &event {
                updated_rows += added
                    .iter()
                    .filter(|row| {
                        row.row.cell("status") == Some(Value::String("resume-canary".to_owned()))
                    })
                    .count();
            }
            rows += event_row_count(event);
        }
        if rows == tasks {
            break;
        }
    }
    assert_eq!(rows, tasks);
    assert_eq!(
        updated_rows, 1,
        "fresh control must observe the post-update frontier"
    );
    *sent.borrow()
}

impl Fixture {
    pub fn memory(tasks: usize, comments: usize, activity_events: usize) -> Self {
        Self::new(
            tasks,
            comments,
            activity_events,
            open_memory_node(schema(false), 0x71, true),
            WriteIdentity::Database,
        )
    }

    pub fn memory_profile_s() -> Self {
        Self::memory(3_000, 12_000, 9_000)
    }

    pub fn memory_profile_s_policy_update() -> Self {
        Self::memory_policy_update(3_000, 12_000, 9_000)
    }

    pub fn memory_policy_update(tasks: usize, comments: usize, activity_events: usize) -> Self {
        Self::new(
            tasks,
            comments,
            activity_events,
            open_memory_node(schema(true), 0x71, true),
            WriteIdentity::Session(policy_bench_identity()),
        )
    }

    /// A file-backed board: SQLite with WAL and `synchronous=NORMAL`.
    pub fn file(tasks: usize, comments: usize, activity_events: usize) -> (TempDir, Self) {
        let dir = tempfile::tempdir().expect("create W1 benchmark directory");
        let db = block_on(Db::open_at(
            DbConfig::new(schema(false), board_identity(0x71)),
            &dir.path().join("board.sqlite"),
            FileOptions::default(),
        ))
        .expect("open W1 benchmark database");
        let fixture = Self::new(
            tasks,
            comments,
            activity_events,
            db,
            WriteIdentity::Database,
        );
        (dir, fixture)
    }

    pub fn file_profile_s() -> (TempDir, Self) {
        Self::file(3_000, 12_000, 9_000)
    }
}

impl Fixture {
    fn new(
        tasks: usize,
        comments: usize,
        activity_events: usize,
        db: Db,
        activity_update_identity: WriteIdentity,
    ) -> Self {
        assert!(tasks > 0 && comments > 0 && activity_events > 0);
        let users = (0..USERS).map(|i| row_id(1, i)).collect::<Vec<_>>();
        let projects = (0..PROJECTS).map(|i| row_id(2, i)).collect::<Vec<_>>();
        let task_ids = (0..tasks).map(|i| row_id(3, i)).collect::<Vec<_>>();

        block_on(db.transaction(async |tx| {
            for (index, id) in users.iter().copied().enumerate() {
                tx.insert(
                    "users",
                    BTreeMap::from([("name".to_owned(), Value::String(format!("User {index}")))]),
                    InsertOptions {
                        row_id: Some(id),
                        ..Default::default()
                    },
                )
                .await?;
            }
            for (index, id) in projects.iter().copied().enumerate() {
                tx.insert(
                    "projects",
                    BTreeMap::from([(
                        "name".to_owned(),
                        Value::String(format!("Project {index}")),
                    )]),
                    InsertOptions {
                        row_id: Some(id),
                        ..Default::default()
                    },
                )
                .await?;
            }
            for (index, id) in task_ids.iter().copied().enumerate() {
                tx.insert(
                    "tasks",
                    BTreeMap::from([
                        (
                            "project".to_owned(),
                            Value::Uuid(projects[index % PROJECTS].0),
                        ),
                        ("title".to_owned(), Value::String(format!("Task {index}"))),
                        (
                            "status".to_owned(),
                            Value::String(["todo", "doing", "review", "done"][index % 4].into()),
                        ),
                        ("assignee".to_owned(), Value::Uuid(users[index % USERS].0)),
                        ("updated_at".to_owned(), Value::U64(index as u64)),
                    ]),
                    InsertOptions {
                        row_id: Some(id),
                        ..Default::default()
                    },
                )
                .await?;
            }
            for index in 0..comments {
                tx.insert(
                    "comments",
                    BTreeMap::from([
                        ("task".to_owned(), Value::Uuid(task_ids[index % tasks].0)),
                        (
                            "author".to_owned(),
                            Value::Uuid(users[(index * 3) % USERS].0),
                        ),
                        (
                            "body".to_owned(),
                            Value::String(format!("Comment {index} on project-board work")),
                        ),
                        ("created_at".to_owned(), Value::U64(index as u64)),
                    ]),
                    InsertOptions::default(),
                )
                .await?;
            }
            for index in 0..activity_events {
                tx.insert(
                    "activity",
                    BTreeMap::from([
                        (
                            "project".to_owned(),
                            Value::Uuid(projects[index % PROJECTS].0),
                        ),
                        ("task".to_owned(), Value::Uuid(task_ids[index % tasks].0)),
                        (
                            "actor".to_owned(),
                            Value::Uuid(users[(index * 5) % USERS].0),
                        ),
                        (
                            "kind".to_owned(),
                            Value::String(
                                if (index / PROJECTS).is_multiple_of(2) {
                                    "updated"
                                } else {
                                    "commented"
                                }
                                .into(),
                            ),
                        ),
                        ("created_at".to_owned(), Value::U64(index as u64)),
                    ]),
                    InsertOptions {
                        row_id: Some(row_id(4, index)),
                        ..Default::default()
                    },
                )
                .await?;
            }
            Ok(())
        }))
        .expect("seed W1 fixture transaction");

        let board = prepare_page(&db, "tasks", "project", projects[0], "updated_at");
        let comments_query = prepare_page(&db, "comments", "task", task_ids[0], "created_at");
        let activity_query = prepare_page(&db, "activity", "task", task_ids[0], "created_at");
        let bounded_activity_page = db
            .prepare_query(
                &Query::from("activity")
                    .filter(eq(col("project"), lit(projects[0].0)))
                    .filter(eq(col("kind"), lit("updated")))
                    .limit(50),
            )
            .expect("prepare W1 two-equality activity page");
        let maintained_activity = db
            .prepare_query(
                &Query::from("activity")
                    .filter(eq(col("project"), lit(projects[0].0)))
                    .filter(eq(col("kind"), lit("updated"))),
            )
            .expect("prepare W1 maintained two-equality activity query");
        let point_activity = db
            .prepare_query(
                &Query::from("activity")
                    .filter(eq(col("id"), lit(row_id(4, 0).0)))
                    .limit(1),
            )
            .expect("prepare W1 point activity query");
        let fixture = Self {
            db,
            board,
            comments: comments_query,
            activity: activity_query,
            bounded_activity_page,
            maintained_activity,
            point_activity,
            activity_transition_row: row_id(4, PROJECTS),
            task_transition_row: task_ids[0],
            activity_transition_matching: false,
            activity_update_identity,
        };
        assert_eq!(fixture.board_count(), tasks.div_ceil(PROJECTS).min(200));
        assert_eq!(fixture.comments_count(), comments.div_ceil(tasks).min(200));
        assert_eq!(
            fixture.activity_count(),
            activity_events.div_ceil(tasks).min(200)
        );
        assert_eq!(
            fixture.bounded_activity_page_count(),
            expected_bounded_activity_count(activity_events)
        );
        fixture
    }

    pub fn board_count(&self) -> usize {
        self.read_count(&self.board)
    }

    pub fn comments_count(&self) -> usize {
        self.read_count(&self.comments)
    }

    pub fn activity_count(&self) -> usize {
        self.read_count(&self.activity)
    }

    pub fn task_detail_count(&self) -> usize {
        self.comments_count() + self.activity_count()
    }

    pub fn bounded_activity_page_count(&self) -> usize {
        self.read_count(&self.bounded_activity_page)
    }

    pub fn into_maintained_activity(self) -> MaintainedActivityFixture {
        let subscription = block_on(self.db.subscribe(
            &self.maintained_activity,
            ReadOpts {
                tier: ReadTier::LocalOnly,
                ..ReadOpts::default()
            },
        ))
        .expect("install W1 maintained activity delta subscription");
        let mut fixture = MaintainedActivityFixture {
            fixture: self,
            subscription,
        };
        while fixture.subscription.try_next_event().is_some() {}
        fixture
    }

    pub fn subscribe_point_activity_once(&self) -> usize {
        let mut subscription = block_on(self.db.subscribe(
            &self.point_activity,
            ReadOpts {
                tier: ReadTier::LocalOnly,
                ..ReadOpts::default()
            },
        ))
        .expect("install W1 maintained point subscription");
        let mut rows = 0;
        while let Some(event) = subscription.try_next_event() {
            rows += event_row_count(event);
        }
        rows
    }

    fn read_count(&self, query: &PreparedQuery) -> usize {
        self.db
            .read(query)
            .expect("W1 benchmark read succeeds")
            .len()
    }

    pub fn toggle_activity_indexed_predicate(&mut self) {
        let next_kind = if self.activity_transition_matching {
            "commented"
        } else {
            "updated"
        };
        let write = block_on(self.db.update(
            "activity",
            self.activity_transition_row,
            BTreeMap::from([("kind".to_owned(), Value::String(next_kind.to_owned()))]),
            UpdateOptions {
                identity: self.activity_update_identity,
                ..Default::default()
            },
        ))
        .expect("toggle W1 indexed predicate");
        block_on(write.wait(DurabilityTier::Local)).expect("settle W1 indexed predicate toggle");
        self.activity_transition_matching = !self.activity_transition_matching;
    }

    pub fn delete_target_task(&self) {
        let write = block_on(self.db.delete(
            "tasks",
            self.task_transition_row,
            DeleteOptions::default(),
        ))
        .expect("delete W1 target task");
        block_on(write.wait(DurabilityTier::Local)).expect("settle W1 target deletion");
    }

    pub fn restore_target_task(&self) {
        let write = block_on(self.db.restore(
            "tasks",
            self.task_transition_row,
            None,
            RestoreOptions::default(),
        ))
        .expect("restore W1 target task");
        block_on(write.wait(DurabilityTier::Local)).expect("settle W1 target restore");
    }
}

impl MaintainedActivityFixture {
    pub fn toggle_indexed_predicate(&mut self) -> usize {
        self.fixture.toggle_activity_indexed_predicate();
        let mut changed = 0;
        while let Some(event) = self.subscription.try_next_event() {
            changed += event_row_count(event);
        }
        changed
    }
}

fn event_row_count(event: SubscriptionEvent) -> usize {
    match event {
        SubscriptionEvent::Delta {
            added,
            updated,
            removed,
            ..
        } => added.len() + updated.len() + removed.len(),
        SubscriptionEvent::Rejected { reason } => panic!("W1 subscription rejected: {reason:?}"),
        SubscriptionEvent::Closed => panic!("W1 subscription closed"),
    }
}

fn schema(policy_activity_updates: bool) -> JazzSchema {
    let activity_policy = PolicyExpr::Or(
        ["updated", "commented"]
            .into_iter()
            .map(|kind| PolicyExpr::Cmp {
                column: "kind".to_owned(),
                op: CmpOp::Eq,
                value: PolicyValue::Literal(PublicValue::Text(kind.to_owned())),
            })
            .collect(),
    );
    let activity = TableSchemaBuilder::new("activity")
        .fk_column("project", "projects")
        .fk_column("task", "tasks")
        .fk_column("actor", "users")
        .column("kind", ColumnType::Text)
        .column("created_at", ColumnType::Timestamp)
        .composite_index(["task", "created_at"]);
    let activity = if policy_activity_updates {
        activity.policies(
            TablePolicies::new()
                .with_select(activity_policy.clone())
                .with_update(Some(activity_policy.clone()), activity_policy),
        )
    } else {
        activity
    };
    let public = SchemaBuilder::new()
        .table(TableSchemaBuilder::new("users").column("name", ColumnType::Text))
        .table(TableSchemaBuilder::new("projects").column("name", ColumnType::Text))
        .table(
            TableSchemaBuilder::new("tasks")
                .fk_column("project", "projects")
                .column("title", ColumnType::Text)
                .column("status", ColumnType::Text)
                .fk_column("assignee", "users")
                .column("updated_at", ColumnType::Timestamp)
                .composite_index(["project", "updated_at"]),
        )
        .table(
            TableSchemaBuilder::new("comments")
                .fk_column("task", "tasks")
                .fk_column("author", "users")
                .column("body", ColumnType::Text)
                .column("created_at", ColumnType::Timestamp)
                .composite_index(["task", "created_at"]),
        )
        .table(activity)
        .build();
    JazzSchema::new(&public).expect("W1 public schema compiles")
}

fn board_identity(node: u8) -> DbIdentity {
    DbIdentity {
        node: NodeUuid::from_bytes([node; 16]),
        author: AuthorSubject::SYSTEM,
    }
}

fn open_memory_node(schema: JazzSchema, node: u8, history_complete: bool) -> Db {
    let config = DbConfig::new(schema, board_identity(node));
    if history_complete {
        block_on(Db::open_history_complete(config))
    } else {
        block_on(Db::open(config))
    }
    .expect("open W1 database")
}

fn prepare_page(
    db: &Db,
    table: &str,
    filter_column: &str,
    filter_value: RowUuid,
    order_column: &str,
) -> PreparedQuery {
    db.prepare_query(
        &Query::from(table)
            .filter(eq(col(filter_column), lit(filter_value.0)))
            .order_by(order_column, OrderDirection::Desc)
            .limit(200),
    )
    .expect("prepare W1 page query")
}

fn row_id(kind: u8, index: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[0] = kind;
    bytes[8..].copy_from_slice(&(index as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn expected_bounded_activity_count(activity_events: usize) -> usize {
    expected_maintained_activity_count(activity_events).min(50)
}

fn expected_maintained_activity_count(activity_events: usize) -> usize {
    (0..activity_events)
        .filter(|index| index % PROJECTS == 0 && (index / PROJECTS).is_multiple_of(2))
        .count()
}

#[cfg(test)]
mod resume_tests {
    use super::*;

    /// Exercises the benchmark's exact public-result assertion outside timing.
    /// Bob syncs, disconnects, the authority updates one task, then Bob
    /// resumes. This reuses the byte-wire benchmark harness so CI checks the
    /// very same reset/delta consumer; it is not a second transport model.
    #[test]
    fn resumed_task_update_preserves_every_other_row_and_column() {
        for (tasks, comments, activity) in [(300, 1_200, 900), (500, 2_000, 1_500)] {
            let mut fixture = ResumeFixture::memory(tasks, comments, activity);
            let resume_bytes = fixture.resume_once();
            fixture.assert_resume_receipt(resume_bytes);
        }
    }
}
