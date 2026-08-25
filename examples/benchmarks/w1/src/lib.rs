//! W1 compatibility fixture derived from the realistic project-board workload.

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, MergeableTxOps, PreparedQuery, block_on};
use jazz::groove::records::Value;
use jazz::groove::storage::{MemoryStorage, OrderedKvStorage, ReopenableStorage};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz_storage_rocksdb::{Durability, RocksDbStorage};
use tempfile::TempDir;

const USERS: usize = 10;
const PROJECTS: usize = 30;

/// Seeded W1 read fixture. Setup is deliberately outside measured closures.
pub struct Fixture<S: OrderedKvStorage> {
    db: Db<S>,
    board: PreparedQuery,
    comments: PreparedQuery,
    activity: PreparedQuery,
    bounded_activity_page: PreparedQuery,
}

impl Fixture<MemoryStorage> {
    pub fn memory(tasks: usize, comments: usize, activity_events: usize) -> Self {
        let schema = schema();
        let families = schema.column_families();
        let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        Self::new(
            tasks,
            comments,
            activity_events,
            schema,
            MemoryStorage::new(&family_refs),
        )
    }

    pub fn memory_profile_s() -> Self {
        Self::memory(3_000, 12_000, 9_000)
    }
}

impl Fixture<RocksDbStorage> {
    pub fn rocksdb(tasks: usize, comments: usize, activity_events: usize) -> (TempDir, Self) {
        let schema = schema();
        let families = schema.column_families();
        let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        let dir = tempfile::tempdir().expect("create W1 RocksDB benchmark directory");
        let storage =
            RocksDbStorage::open_with_durability(dir.path(), &family_refs, Durability::WalNoSync)
                .expect("open W1 RocksDB benchmark storage");
        (
            dir,
            Self::new(tasks, comments, activity_events, schema, storage),
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
                    InsertOptions::default(),
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
        let fixture = Self {
            db,
            board,
            comments: comments_query,
            activity: activity_query,
            bounded_activity_page,
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

    fn read_count(&self, query: &PreparedQuery) -> usize {
        self.db
            .read(query)
            .expect("W1 benchmark read succeeds")
            .len()
    }
}

fn schema() -> JazzSchema {
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
        .table(
            TableSchemaBuilder::new("activity")
                .fk_column("project", "projects")
                .fk_column("task", "tasks")
                .fk_column("actor", "users")
                .column("kind", ColumnType::Text)
                .column("created_at", ColumnType::Timestamp),
        )
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
    (0..activity_events)
        .filter(|index| index % PROJECTS == 0 && (index / PROJECTS).is_multiple_of(2))
        .count()
        .min(50)
}
