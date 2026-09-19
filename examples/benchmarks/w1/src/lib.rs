//! W1 compatibility fixture derived from the realistic project-board workload.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use jazz::db::{
    CommitUnitTrust, Db, DbConfig, DbIdentity, DeleteOptions, InsertOptions, LocalUpdates,
    MergeableTxOps, PeerIoPump, PreparedQuery, Propagation, ReadOpts, RestoreOptions, ResumeCursor,
    SubscriptionEvent, SubscriptionStream, UpdateOptions, WireTransportAdapter, WriteIdentity,
    block_on,
};
use jazz::groove::records::Value;
use jazz::groove::storage::{MemoryStorage, OrderedKvStorage, ReopenableStorage};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::tools::{
    CmpOp, ColumnType, PolicyExpr, PolicyValue, SchemaBuilder, TablePolicies, TableSchemaBuilder,
    Value as PublicValue,
};
use jazz::tx::DurabilityTier;
use jazz::wire::{
    FEATURE_MESSAGE_FRAGMENTATION, FEATURE_SESSION_FRAME, FEATURE_STRUCTURED_ERRORS,
    FEATURE_SYNC_MESSAGE_PAYLOAD, TransportError, WIRE_PROTOCOL_VERSION, WireSession,
    WireTransport,
};
use jazz_storage_rocksdb::{Durability, RocksDbStorage};
use tempfile::TempDir;

const USERS: usize = 10;
const PROJECTS: usize = 30;

fn policy_bench_identity() -> AuthorSubject {
    // Durable session writes need an account-bound principal. This fixture
    // preserves the existing Session identity and policy configuration.
    AuthorSubject::for_test_bytes([0x76; 16])
}

/// Seeded W1 read fixture. Setup is deliberately outside measured closures.
pub struct Fixture<S: OrderedKvStorage> {
    db: Db<S>,
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

pub struct MaintainedActivityFixture<S: OrderedKvStorage> {
    fixture: Fixture<S>,
    subscription: SubscriptionStream,
}

/// Prepared byte-wire reconnect with one disconnected task update pending.
pub struct ResumeFixture {
    server: Db<MemoryStorage>,
    client: Db<MemoryStorage>,
    subscription: SubscriptionStream,
    cursor: Option<ResumeCursor>,
    fresh_post_update_bytes: usize,
    task_schema: jazz::schema::TableSchema,
    rows: ResumeRows,
    expected_rows: ResumeRows,
    resume_events: Vec<SubscriptionEvent>,
}

type ResumeRows = BTreeMap<RowUuid, BTreeMap<String, Option<Value>>>;

struct ByteDuplexTransport {
    outbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    inbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
}

fn pump_aux(left: &PeerIoPump, right: &PeerIoPump) {
    loop {
        let mut progressed = false;
        while let Some(frame) = left
            .take_outbound_wire_frame()
            .expect("drain W1 left wire pump")
        {
            assert!(
                block_on(right.route_incoming_wire_frame(frame))
                    .expect("route W1 left wire frame")
                    .is_none()
            );
            progressed = true;
        }
        while let Some(frame) = right
            .take_outbound_wire_frame()
            .expect("drain W1 right wire pump")
        {
            assert!(
                block_on(left.route_incoming_wire_frame(frame))
                    .expect("route W1 right wire frame")
                    .is_none()
            );
            progressed = true;
        }
        if !progressed {
            break;
        }
    }
}

// Empty carrier queues do not establish remote settlement: adapters can still
// own accepted messages waiting for credits. Keep driving both real wire pumps
// until the public write wait proves the authority has applied the frontier.
fn pump_until_settled(
    writer: &Db<MemoryStorage>,
    server: &Db<MemoryStorage>,
    writer_pump: &PeerIoPump,
    server_pump: &PeerIoPump,
    max_turns: usize,
) {
    let mut settled = std::pin::pin!(writer.wait_for_pending_writes(DurabilityTier::Global));
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..max_turns {
        block_on(writer.tick()).expect("ship W1 pending writes");
        block_on(server.tick()).expect("ingest W1 pending writes");
        pump_aux(writer_pump, server_pump);
        if let Poll::Ready(result) = settled.as_mut().poll(&mut context) {
            result.expect("settle W1 writes at the authority");
            return;
        }
    }
    panic!("W1 writes must reach global settlement within bounded pump turns");
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

impl Fixture<MemoryStorage> {
    pub fn memory(tasks: usize, comments: usize, activity_events: usize) -> Self {
        let schema = schema(false);
        let families = schema.column_families();
        let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        Self::new(
            tasks,
            comments,
            activity_events,
            schema,
            MemoryStorage::new(&family_refs).expect("valid memory storage families"),
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
        let schema = schema(true);
        let families = schema.column_families();
        let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        Self::new(
            tasks,
            comments,
            activity_events,
            schema,
            MemoryStorage::new(&family_refs).expect("valid memory storage families"),
            WriteIdentity::Session(policy_bench_identity()),
        )
    }
}

impl ResumeFixture {
    pub fn memory(tasks: usize, comments: usize, activity_events: usize) -> Self {
        let writer = Fixture::<MemoryStorage>::memory(tasks, comments, activity_events);
        let task_schema = schema(false)
            .tables()
            .iter()
            .find(|table| table.name == "tasks")
            .unwrap()
            .clone();
        let server = open_memory_node(schema(false), 0x72, true);
        let client = open_memory_node(schema(false), 0x73, false);

        let (writer_transport, server_writer_transport) = byte_duplex(1);
        let writer_upstream = block_on(writer.db.connect_upstream(writer_transport));
        // These fixture writes are database-authored SYSTEM work, admitted by
        // the synthetic host as a backend rather than an ordinary user session.
        let writer_subscriber = server.accept_subscriber_with_claims_and_trust(
            server_writer_transport,
            AuthorSubject::SYSTEM,
            BTreeMap::new(),
            CommitUnitTrust::TrustedBackend,
        );
        let writer_pump = block_on(writer_upstream.lock()).io_pump();
        let server_writer_pump = block_on(writer_subscriber.lock()).io_pump();
        pump_until_settled(
            &writer.db,
            &server,
            &writer_pump,
            &server_writer_pump,
            10_000,
        );
        assert!(writer.db.detach_connection(&writer_upstream));
        assert!(server.detach_connection(&writer_subscriber));
        let (client_transport, server_transport) = byte_duplex(2);
        let upstream = block_on(client.connect_upstream(client_transport));
        let subscriber = server.accept_subscriber(server_transport, AuthorSubject::SYSTEM);
        let client_pump = block_on(upstream.lock()).io_pump();
        let server_pump = block_on(subscriber.lock()).io_pump();
        let prepared = client
            .prepare_query(&Query::from("tasks"))
            .expect("prepare W1 resumed tasks query");
        let mut subscription = block_on(client.subscribe(
            &prepared,
            ReadOpts {
                tier: DurabilityTier::Global,
                local_updates: LocalUpdates::Deferred,
                propagation: Propagation::Full,
                ..ReadOpts::default()
            },
        ))
        .expect("subscribe W1 resumed tasks");
        let mut rows = ResumeRows::new();
        for _ in 0..512 {
            block_on(client.tick()).expect("announce W1 resumed tasks subscription");
            block_on(server.tick()).expect("serve W1 full task snapshot");
            block_on(client.tick()).expect("apply W1 full task snapshot");
            block_on(client.tick()).expect("materialize W1 full task snapshot");
            pump_aux(&client_pump, &server_pump);
            while let Some(event) = subscription.try_next_event() {
                apply_resume_event(&mut rows, &task_schema, event);
            }
            if rows.len() == tasks {
                break;
            }
        }
        assert_eq!(rows.len(), tasks);
        let initial_snapshot_bytes = block_on(subscriber.lock())
            .last_resume_bytes()
            .expect("W1 full snapshot bytes");
        assert!(initial_snapshot_bytes > 0);
        block_on(server.tick()).expect("refresh W1 served cursor state");
        block_on(client.tick()).expect("apply W1 served cursor state");
        while let Some(event) = subscription.try_next_event() {
            apply_resume_event(&mut rows, &task_schema, event);
        }
        assert_eq!(rows.len(), tasks);
        let mut expected_rows = rows.clone();
        let target = expected_rows
            .get_mut(&writer.task_transition_row)
            .expect("updated task was in the initial snapshot");
        let expected_status = Some(Value::String("resume-canary".to_owned()));
        assert_ne!(target.get("status"), Some(&expected_status));
        target.insert("status".to_owned(), expected_status);
        let cursor = block_on(subscriber.lock())
            .take_resume_cursor()
            .expect("take W1 subscriber resume cursor");
        assert!(client.detach_connection(&upstream));
        assert!(server.detach_connection(&subscriber));

        let write = block_on(writer.db.update(
            "tasks",
            writer.task_transition_row,
            BTreeMap::from([(
                "status".to_owned(),
                Value::String("resume-canary".to_owned()),
            )]),
            UpdateOptions::default(),
        ))
        .expect("write disconnected W1 task update");
        block_on(write.wait(DurabilityTier::Local)).expect("settle disconnected W1 task update");
        let (writer_transport, server_writer_transport) = byte_duplex(3);
        let writer_upstream = block_on(writer.db.connect_upstream(writer_transport));
        let writer_subscriber = server.accept_subscriber_with_claims_and_trust(
            server_writer_transport,
            AuthorSubject::SYSTEM,
            BTreeMap::new(),
            CommitUnitTrust::TrustedBackend,
        );
        let writer_pump = block_on(writer_upstream.lock()).io_pump();
        let server_writer_pump = block_on(writer_subscriber.lock()).io_pump();
        pump_until_settled(
            &writer.db,
            &server,
            &writer_pump,
            &server_writer_pump,
            1_000,
        );
        assert!(writer.db.detach_connection(&writer_upstream));
        assert!(server.detach_connection(&writer_subscriber));

        // Compare the same post-update frontier. A new usage site rebuilds its
        // complete CoveredInput closure; delta-sized reconnect is not promised
        // for this reference-bearing query (performance follow-up: #2372).
        // Use isolated control nodes so setup does not reconcile or warm the
        // measured server's retained evaluator before resume_once.
        let fresh_post_update_bytes = fresh_task_snapshot_bytes(tasks, comments, activity_events);

        Self {
            server,
            client,
            subscription,
            cursor: Some(cursor),
            fresh_post_update_bytes,
            task_schema,
            rows,
            expected_rows,
            resume_events: Vec::new(),
        }
    }

    pub fn resume_once(&mut self) -> usize {
        let cursor = self.cursor.take().expect("W1 resume fixture is single-use");
        let (client_transport, server_transport) = byte_duplex(4);
        let _upstream = block_on(self.client.connect_upstream(client_transport));
        let resumed = self.server.accept_subscriber_with_resume(
            server_transport,
            AuthorSubject::SYSTEM,
            cursor,
        );
        let client_pump = block_on(_upstream.lock()).io_pump();
        let server_pump = block_on(resumed.lock()).io_pump();
        block_on(self.client.tick()).expect("announce resumed W1 subscription");
        block_on(self.server.tick()).expect("serve W1 resume catch-up");
        pump_aux(&client_pump, &server_pump);
        block_on(self.client.tick()).expect("apply W1 resume catch-up");
        block_on(self.client.tick()).expect("materialize W1 resume event");
        let resume_bytes = block_on(resumed.lock())
            .last_resume_bytes()
            .expect("W1 resume catch-up bytes");
        let event =
            block_on(self.subscription.next_event()).expect("W1 resume stream remains open");
        self.resume_events.push(event);
        while let Some(event) = self.subscription.try_next_event() {
            self.resume_events.push(event);
        }
        resume_bytes
    }

    /// Correctness-only validation, deliberately outside the timed closure.
    /// The crate tests exercise the same benchmark sizes and resume path.
    pub fn assert_resume_receipt(&mut self, resume_bytes: usize) {
        assert!(!self.resume_events.is_empty());
        for event in self.resume_events.drain(..) {
            apply_resume_event(&mut self.rows, &self.task_schema, event);
        }
        // A reconnect may publish a complete reset even when IVM computed one
        // terminal change. Assert the actual application result: identical
        // membership and every column unchanged except the one expected status.
        // Full-reset reconnect cost remains measured here (follow-up #2372).
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
        settled,
        ..
    } = event
    else {
        panic!("W1 subscription must remain open and authorized: {event:?}");
    };
    assert!(settled);
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
            .map(|column| {
                (
                    column.name().to_owned(),
                    output.row.cell(task_schema, column.name()),
                )
            })
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
    let fixture = Fixture::<MemoryStorage>::memory(tasks, comments, activity_events);
    let writer = &fixture.db;
    let write = block_on(writer.update(
        "tasks",
        fixture.task_transition_row,
        BTreeMap::from([(
            "status".to_owned(),
            Value::String("resume-canary".to_owned()),
        )]),
        UpdateOptions::default(),
    ))
    .expect("write W1 fresh control update");
    block_on(write.wait(DurabilityTier::Local)).expect("settle W1 fresh control update");
    let control_schema = schema(false);
    let task_schema = control_schema
        .tables()
        .iter()
        .find(|table| table.name == "tasks")
        .unwrap();
    let server = open_memory_node(schema(false), 0x74, true);
    let client = open_memory_node(schema(false), 0x75, false);
    let (transport, server_transport) = byte_duplex(5);
    let upstream = block_on(writer.connect_upstream(transport));
    let subscriber = server.accept_subscriber_with_claims_and_trust(
        server_transport,
        AuthorSubject::SYSTEM,
        BTreeMap::new(),
        CommitUnitTrust::TrustedBackend,
    );
    let writer_pump = block_on(upstream.lock()).io_pump();
    let server_pump = block_on(subscriber.lock()).io_pump();
    pump_until_settled(writer, &server, &writer_pump, &server_pump, 10_000);
    assert!(writer.detach_connection(&upstream));
    assert!(server.detach_connection(&subscriber));

    let (transport, server_transport) = byte_duplex(6);
    let upstream = block_on(client.connect_upstream(transport));
    let subscriber = server.accept_subscriber(server_transport, AuthorSubject::SYSTEM);
    let client_pump = block_on(upstream.lock()).io_pump();
    let server_pump = block_on(subscriber.lock()).io_pump();
    let prepared = client
        .prepare_query(&Query::from("tasks"))
        .expect("prepare W1 fresh control");
    let mut subscription = block_on(client.subscribe(
        &prepared,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::Full,
            ..ReadOpts::default()
        },
    ))
    .expect("subscribe W1 fresh control");
    let mut rows = 0;
    let mut updated_rows = 0;
    for _ in 0..512 {
        block_on(client.tick()).expect("announce W1 fresh control");
        block_on(server.tick()).expect("serve W1 fresh control");
        block_on(client.tick()).expect("apply W1 fresh control");
        block_on(client.tick()).expect("materialize W1 fresh control");
        pump_aux(&client_pump, &server_pump);
        while let Some(event) = subscription.try_next_event() {
            if let SubscriptionEvent::Delta { added, .. } = &event {
                updated_rows += added
                    .iter()
                    .filter(|row| {
                        row.row.cell(task_schema, "status")
                            == Some(Value::String("resume-canary".to_owned()))
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
    block_on(subscriber.lock())
        .last_resume_bytes()
        .expect("W1 fresh post-update bytes")
}

impl Fixture<RocksDbStorage> {
    pub fn rocksdb(tasks: usize, comments: usize, activity_events: usize) -> (TempDir, Self) {
        let schema = schema(false);
        let families = schema.column_families();
        let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        let dir = tempfile::tempdir().expect("create W1 RocksDB benchmark directory");
        let storage =
            RocksDbStorage::open_with_durability(dir.path(), &family_refs, Durability::WalNoSync)
                .expect("open W1 RocksDB benchmark storage");
        (
            dir,
            Self::new(
                tasks,
                comments,
                activity_events,
                schema,
                storage,
                WriteIdentity::Database,
            ),
        )
    }

    pub fn rocksdb_profile_s() -> (TempDir, Self) {
        Self::rocksdb(3_000, 12_000, 9_000)
    }
}

impl<S: OrderedKvStorage + ReopenableStorage + 'static> Fixture<S> {
    fn new(
        tasks: usize,
        comments: usize,
        activity_events: usize,
        schema: JazzSchema,
        storage: S,
        activity_update_identity: WriteIdentity,
    ) -> Self {
        assert!(tasks > 0 && comments > 0 && activity_events > 0);
        let db = open_db(schema, storage);
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

    pub fn into_maintained_activity(self) -> MaintainedActivityFixture<S> {
        let subscription = block_on(self.db.subscribe(
            &self.maintained_activity,
            ReadOpts {
                tier: DurabilityTier::Local,
                local_updates: LocalUpdates::Deferred,
                propagation: Propagation::LocalOnly,
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
                tier: DurabilityTier::Local,
                local_updates: LocalUpdates::Deferred,
                propagation: Propagation::LocalOnly,
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

impl<S: OrderedKvStorage + ReopenableStorage + 'static> MaintainedActivityFixture<S> {
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
        .column("created_at", ColumnType::Timestamp);
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
                .column("updated_at", ColumnType::Timestamp),
        )
        .table(
            TableSchemaBuilder::new("comments")
                .fk_column("task", "tasks")
                .fk_column("author", "users")
                .column("body", ColumnType::Text)
                .column("created_at", ColumnType::Timestamp),
        )
        .table(activity)
        .build();
    JazzSchema::new(&public).expect("W1 public schema compiles")
}

fn open_db<S: OrderedKvStorage + ReopenableStorage + 'static>(
    schema: JazzSchema,
    storage: S,
) -> Db<S> {
    block_on(Db::open(DbConfig::new(
        schema,
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([0x71; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .expect("open W1 benchmark database")
}

fn open_memory_node(schema: JazzSchema, node: u8, history_complete: bool) -> Db<MemoryStorage> {
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let config = DbConfig::new(
        schema,
        MemoryStorage::new(&family_refs).expect("valid memory storage families"),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    );
    if history_complete {
        block_on(Db::open_history_complete(config))
    } else {
        block_on(Db::open(config))
    }
    .expect("open W1 resume database")
}

fn byte_duplex(epoch: u64) -> (Box<dyn jazz::db::Transport>, Box<dyn jazz::db::Transport>) {
    if std::env::var_os("JAZZ_W1_TRACE").is_some() {
        let phase = match epoch {
            1 => "seed writer to authority",
            2 => "initial client snapshot",
            3 => "disconnected writer update",
            4 => "client resume",
            5 => "fresh control writer",
            6 => "fresh control snapshot",
            _ => "unknown",
        };
        eprintln!("W1 byte duplex epoch={epoch} phase={phase}");
    }
    let left = Rc::new(RefCell::new(VecDeque::new()));
    let right = Rc::new(RefCell::new(VecDeque::new()));
    let left_transport = ByteDuplexTransport {
        outbound: Rc::clone(&left),
        inbound: Rc::clone(&right),
    };
    let right_transport = ByteDuplexTransport {
        outbound: Rc::clone(&right),
        inbound: Rc::clone(&left),
    };
    let session = WireSession {
        session_id: "w1-resume-benchmark".to_owned(),
        epoch,
        identity: Some(AuthorSubject::SYSTEM),
    };
    let features = FEATURE_SYNC_MESSAGE_PAYLOAD
        | FEATURE_SESSION_FRAME
        | FEATURE_STRUCTURED_ERRORS
        | FEATURE_MESSAGE_FRAGMENTATION;
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

fn prepare_page<S: OrderedKvStorage + ReopenableStorage + 'static>(
    db: &Db<S>,
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

/// Hot current reads over a retained chain of ahead-current candidates.
pub mod ahead_current {
    use std::collections::BTreeMap;

    use jazz::block_on;
    use jazz::groove::records::Value;
    use jazz::ids::{NodeUuid, RowUuid};
    use jazz::node::{MergeableCommit, NodeState};
    use jazz::schema::JazzSchema;
    use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
    use jazz::tx::{DurabilityTier, Fate, TxId};
    use jazz_storage_rocksdb::{Durability, RocksDbStorage};

    const TABLE: &str = "status";

    /// Pre-seeded candidate history for a single logical current row.
    pub struct AheadCurrentFixture {
        core: NodeState<RocksDbStorage>,
        _directory: tempfile::TempDir,
        depth: usize,
        tier: DurabilityTier,
        newest_tx: TxId,
    }

    impl AheadCurrentFixture {
        pub fn new(depth: usize, tier: DurabilityTier) -> Self {
            assert!(depth > 0, "W1 requires at least one retained candidate");
            assert!(
                matches!(tier, DurabilityTier::Local | DurabilityTier::Edge),
                "W1 only measures Local and Edge candidate visibility"
            );

            let schema = schema();
            let directory = tempfile::tempdir().expect("create W1 fixture directory");
            let families = schema.column_families();
            let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
            let storage = RocksDbStorage::open_with_durability(
                directory.path(),
                &family_refs,
                Durability::WalNoSync,
            )
            .expect("open W1 RocksDB");
            let mut core = block_on(NodeState::new(node(), schema, storage)).expect("open W1 node");

            let mut parent = None;
            let mut newest_tx = None;
            for index in 0..depth {
                let mut commit = MergeableCommit::new(TABLE, row(), 20_000_000 + index as u64)
                    .cells(cells(index));
                if let Some(parent_tx) = parent {
                    commit = commit.parents(vec![parent_tx]);
                }
                let publication =
                    block_on(core.commit_mergeable(commit)).expect("commit W1 candidate");
                let tx_id = publication.tx_id();
                block_on(core.persist_and_settle_transaction(publication))
                    .expect("persist W1 candidate");
                if tier == DurabilityTier::Edge {
                    block_on(core.apply_fate_update(
                        tx_id,
                        Fate::Accepted,
                        None,
                        Some(DurabilityTier::Edge),
                    ))
                    .expect("edge-accept W1 candidate");
                }
                parent = Some(tx_id);
                newest_tx = Some(tx_id);
            }

            Self {
                core,
                _directory: directory,
                depth,
                tier,
                newest_tx: newest_tx.expect("non-empty W1 candidate history"),
            }
        }

        /// Untimed correctness receipt for depth, attribution, and winner identity.
        pub fn assert_receipt(&mut self) {
            self.core.reset_storage_read_metrics();
            let rows = self.current_rows();
            let metrics = self.core.storage_read_metrics();

            assert_eq!(rows.len(), 1, "{:?} W1 winner count", self.tier);
            assert_eq!(rows[0].row_uuid(), row(), "{:?} W1 winner row", self.tier);
            assert_eq!(
                rows[0].cell_at(0),
                Some(Value::String(title(self.depth - 1))),
                "{:?} W1 must expose the newest candidate ({:?})",
                self.tier,
                self.newest_tx,
            );
            assert_eq!(
                metrics.ahead_current_rows.reads, self.depth,
                "{:?} W1 must read exactly its retained candidate depth: {metrics:?}",
                self.tier,
            );
            assert_eq!(
                metrics.ahead_current_rows.ranges, 2,
                "{:?} W1 must scan content and deletion ahead-current ranges: {metrics:?}",
                self.tier,
            );
        }

        /// The timed operation: one current-row read over the prepared fixture.
        pub fn current_rows(&mut self) -> Vec<jazz::node::CurrentRow> {
            block_on(self.core.current_rows(TABLE, self.tier)).expect("read W1 current rows")
        }
    }

    fn schema() -> JazzSchema {
        let source = SchemaBuilder::new()
            .table(TableSchemaBuilder::new(TABLE).column("title", ColumnType::Text))
            .build();
        JazzSchema::new(&source).expect("compile W1 schema")
    }

    fn cells(index: usize) -> BTreeMap<String, Value> {
        BTreeMap::from([("title".to_owned(), Value::String(title(index)))])
    }

    fn title(index: usize) -> String {
        format!("status-{index:08}")
    }

    fn node() -> NodeUuid {
        NodeUuid::from_bytes([0x71; 16])
    }

    fn row() -> RowUuid {
        RowUuid::from_bytes([0x17; 16])
    }
}

pub use ahead_current::AheadCurrentFixture;

#[cfg(test)]
mod ahead_current_tests {
    use super::*;

    #[test]
    fn bounded_receipt_reads_exact_local_and_edge_candidate_depth() {
        for tier in [DurabilityTier::Local, DurabilityTier::Edge] {
            AheadCurrentFixture::new(3, tier).assert_receipt();
        }
    }

    /// Exercises the benchmark's exact public-result assertion outside timing.
    /// Alice seeds, disconnects Bob, updates one task, then Bob resumes. This
    /// intentionally reuses the byte-wire benchmark harness so CI checks the
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
