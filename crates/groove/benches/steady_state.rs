//! Steady-state experiment: incremental view maintenance versus re-running
//! one-shot queries after each write and diffing against the previous result.
//!
//! Question under test: once subscriptions are open, is it cheaper to maintain
//! them incrementally (Groove IVM) or to re-run each invalidated query and diff?
//! Each benchmark iteration applies one single-row write and brings every
//! subscriber's cached result up to date. Setup, seeding and initial
//! subscription hydration are outside the timing.
//!
//! Workloads (all results are non-trivial; writes keep table sizes stable):
//! - `tasks`: each subscriber watches its own 300 tasks. A write bumps one
//!   task's `rev`, so it changes exactly one subscriber's result by one row.
//! - `feed`: each subscriber watches `follows ⋈ posts` (600 rows). A write
//!   edits one post's `created` by an author some subscriber follows, changing
//!   the feed of every subscribed follower of that author.
//! - `feed_top20`: the same feed, newest 20 only. The edited post becomes the
//!   newest, so it enters every follower's top 20.
//!
//! Engines (all maintain an identical per-subscriber row multiset):
//! - `ivm`: one prepared Groove shape with a live binding per subscriber;
//!   commit, then apply each subscriber's weighted deltas to its cache.
//! - `sqlite_touched`: in-memory SQLite with covering primary keys; re-run the
//!   query for exactly the subscribers the write can affect (ideal read-set
//!   invalidation) and diff.
//! - `sqlite_all`: the same, but re-run every subscriber (coarse table-level
//!   invalidation: every subscription reads the written table).
//! - `pull_touched`: commit to Groove storage without subscriptions, then
//!   re-run hand-written index-probe plans for touched subscribers and diff.
//! - `snapshot_touched`: the same invalidation, re-run via Groove's own
//!   one-shot `query_graph` (today's hydration machinery) and diff.
//!
//! Subscriber counts are swept (`subs` = 10 and 100) to show fan-out. Before
//! measuring, every engine replays the same write sequence and must reach the
//! same caches as the others (benchmark validity, INV-PERF-2).
//!
//! ```text
//! cargo bench -p groove --bench steady_state
//! ```

use std::cell::RefCell;
use std::collections::BTreeMap;

use futures::executor::block_on;
use groove::db::{Database, GraphBuilder, ProjectField, Subscription};
use groove::ivm::{StaticScanSpec, TopByLimit, TopByOrder};
use groove::records::{RecordDescriptor, Value};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, PrimaryKeyColumn,
    TableSchema,
};
use groove::storage::MemoryStorage;
use rusqlite::{Connection, params};

const USERS: u64 = 1_000;
const POSTS_PER_USER: u64 = 20;
const FOLLOWS_PER_USER: u64 = 30;
const TASK_OWNERS: u64 = 100;
const TASKS_PER_OWNER: u64 = 300;
const TOP_K: usize = 20;
const SUBSCRIBER_COUNTS: [u64; 2] = [10, 100];
const VERIFY_WRITES: u64 = 60;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    if std::env::var_os("GROOVE_STEADY_STATE_PROBE").is_some() {
        probe_ivm_phases();
        return;
    }
    verify_engines_agree();
    divan::main();
}

/// Diagnostic, not a benchmark: split the `ivm` engine's per-write time into
/// storage write, IVM tick, progress driving and subscriber drain, and report
/// processed-record counters, for every workload and subscriber count.
fn probe_ivm_phases() {
    const STEPS: u64 = 400;
    for workload in [Workload::Tasks, Workload::Feed, Workload::FeedTop20] {
        for subs in SUBSCRIBER_COUNTS {
            let mut engine = Engine::new(EngineKind::Ivm, workload, subs);
            let (mut storage, mut tick, mut drive, mut drain, mut records) =
                (0.0, 0.0, 0.0, 0.0, 0usize);
            for _ in 0..STEPS {
                let write = write_at(workload, subs, engine.step);
                engine.step += 1;
                let Store::Ivm { database, .. } = &mut engine.store else {
                    unreachable!()
                };
                groove_write(database, write);
                let metrics = database.last_commit_metrics().expect("commit metrics");
                storage += metrics.storage_write_time.as_secs_f64();
                tick += metrics.ivm_tick_time.as_secs_f64();
                records += metrics.tick.records_processed;
                let started = std::time::Instant::now();
                block_on(database.drive_progress()).expect("drive");
                drive += started.elapsed().as_secs_f64();
                let started = std::time::Instant::now();
                drain_ivm(&mut engine.store, &mut engine.caches);
                drain += started.elapsed().as_secs_f64();
            }
            let per = |total: f64| total / STEPS as f64 * 1e6;
            println!(
                "{workload:?} subs={subs}: storage={:.1}us tick={:.1}us drive={:.1}us drain={:.1}us records_processed/write={:.1}",
                per(storage),
                per(tick),
                per(drive),
                per(drain),
                records as f64 / STEPS as f64,
            );
        }
    }
}

/// One result row: `(owner or author, id, rev or created)`.
type Row = (u64, u64, u64);
type Cache = BTreeMap<Row, i64>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Workload {
    Tasks,
    Feed,
    FeedTop20,
}

#[derive(Clone, Copy, Debug)]
enum Write {
    /// Set one task's `rev`.
    TaskRev { owner: u64, id: u64, rev: u64 },
    /// Set one post's `created`, making it the newest post.
    PostCreated { author: u64, id: u64, created: u64 },
}

fn followee(user: u64, slot: u64) -> u64 {
    (user + 1 + slot * 7) % USERS
}

fn initial_created(author: u64, post: u64) -> u64 {
    // Unique: 7919 is coprime with USERS, so the second term is a bijection.
    post * USERS + (author * 7919) % USERS
}

fn post_title(author: u64, post: u64) -> String {
    format!("post {post} by {author}")
}

/// Deterministic write sequence. Every write changes a value, so it always
/// produces a real delta for every affected subscriber.
fn write_at(workload: Workload, subs: u64, step: u64) -> Write {
    match workload {
        Workload::Tasks => Write::TaskRev {
            owner: step % subs,
            id: (step * 7919) % TASKS_PER_OWNER,
            rev: 1_000_000 + step,
        },
        Workload::Feed | Workload::FeedTop20 => {
            // Always edit a post by someone a subscriber follows, so every
            // write lands in watched data (and reaches that author's other
            // subscribed followers too).
            let author = followee(step % subs, (step / subs) % FOLLOWS_PER_USER);
            Write::PostCreated {
                author,
                id: author * POSTS_PER_USER + step % POSTS_PER_USER,
                created: 1_000_000_000 + step,
            }
        }
    }
}

/// Reverse follow index restricted to subscribers: author -> followers.
struct Fixture {
    subs: u64,
    followers: Vec<Vec<u64>>,
}

impl Fixture {
    fn new(subs: u64) -> Self {
        let mut followers = vec![Vec::new(); USERS as usize];
        for user in 0..subs {
            for slot in 0..FOLLOWS_PER_USER {
                followers[followee(user, slot) as usize].push(user);
            }
        }
        Self { subs, followers }
    }

    /// Subscribers whose result a write can change (exact invalidation).
    fn touched(&self, write: Write) -> Vec<u64> {
        match write {
            Write::TaskRev { owner, .. } => vec![owner],
            Write::PostCreated { author, .. } => self.followers[author as usize].clone(),
        }
    }
}

fn apply_diff(cache: &mut Cache, next: Cache) {
    // Diff first so the cost of computing a change set is paid, as a real
    // publisher must before notifying.
    let mut changed = 0usize;
    for (row, weight) in &next {
        if cache.get(row) != Some(weight) {
            changed += 1;
        }
    }
    for row in cache.keys() {
        if !next.contains_key(row) {
            changed += 1;
        }
    }
    divan::black_box(changed);
    *cache = next;
}

fn top_k(mut rows: Vec<Row>) -> Cache {
    let key = |row: &Row| (std::cmp::Reverse(row.2), row.0, row.1);
    if rows.len() > TOP_K {
        rows.select_nth_unstable_by_key(TOP_K - 1, key);
        rows.truncate(TOP_K);
    }
    let mut cache = Cache::new();
    for row in rows {
        *cache.entry(row).or_default() += 1;
    }
    cache
}

fn u64_at(values: &[Value], index: usize) -> u64 {
    match values[index] {
        Value::U64(value) => value,
        ref other => panic!("expected u64, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Groove storage (shared by `ivm`, `pull_touched`, `snapshot_touched`)
// ---------------------------------------------------------------------------

fn groove_schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "posts",
            [
                ColumnSchema::new("author", ColumnType::U64),
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("created", ColumnType::U64),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::composite([
            PrimaryKeyColumn::integer("author", IntegerKeyType::U64),
            PrimaryKeyColumn::integer("id", IntegerKeyType::U64),
        ])),
        TableSchema::new(
            "follows",
            [
                ColumnSchema::new("follower", ColumnType::U64),
                ColumnSchema::new("followee", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::composite([
            PrimaryKeyColumn::integer("follower", IntegerKeyType::U64),
            PrimaryKeyColumn::integer("followee", IntegerKeyType::U64),
        ])),
        TableSchema::new(
            "tasks",
            [
                ColumnSchema::new("owner", ColumnType::U64),
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("rev", ColumnType::U64),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::composite([
            PrimaryKeyColumn::integer("owner", IntegerKeyType::U64),
            PrimaryKeyColumn::integer("id", IntegerKeyType::U64),
        ])),
    ])
}

fn commit(database: &mut Database, batch: groove::db::DatabaseBatch) {
    block_on(async {
        let applied = database.apply_batch(batch).await.expect("apply");
        let persisted = applied.persist().await;
        database.finish_persistence(persisted).expect("persist");
    });
}

fn seeded_groove(workload: Workload) -> Database {
    let storage = MemoryStorage::new(&["posts", "follows", "tasks", "indices"])
        .expect("valid memory storage");
    let mut database = block_on(Database::new(groove_schema(), storage)).expect("database");
    {
        let mut batch = database.open_batch();
        match workload {
            Workload::Tasks => {
                for owner in 0..TASK_OWNERS {
                    for id in 0..TASKS_PER_OWNER {
                        batch.insert(
                            "tasks",
                            vec![
                                Value::U64(owner),
                                Value::U64(id),
                                Value::U64(0),
                                Value::String(format!("task {id} of {owner}")),
                            ],
                        );
                    }
                }
            }
            Workload::Feed | Workload::FeedTop20 => {
                for author in 0..USERS {
                    for post in 0..POSTS_PER_USER {
                        batch.insert(
                            "posts",
                            vec![
                                Value::U64(author),
                                Value::U64(author * POSTS_PER_USER + post),
                                Value::U64(initial_created(author, post)),
                                Value::String(post_title(author, post)),
                            ],
                        );
                    }
                    for slot in 0..FOLLOWS_PER_USER {
                        batch.insert(
                            "follows",
                            vec![Value::U64(author), Value::U64(followee(author, slot))],
                        );
                    }
                }
            }
        }
        commit(&mut database, batch);
    }
    database
}

fn groove_write(database: &mut Database, write: Write) {
    let mut batch = database.open_batch();
    match write {
        Write::TaskRev { owner, id, rev } => batch.update(
            "tasks",
            vec![
                Value::U64(owner),
                Value::U64(id),
                Value::U64(rev),
                Value::String(format!("task {id} of {owner}")),
            ],
        ),
        Write::PostCreated {
            author,
            id,
            created,
        } => batch.update(
            "posts",
            vec![
                Value::U64(author),
                Value::U64(id),
                Value::U64(created),
                Value::String(post_title(author, id % POSTS_PER_USER)),
            ],
        ),
    }
    commit(database, batch);
}

fn user_prefix(user: u64) -> StaticScanSpec {
    StaticScanSpec::Prefix(vec![Value::U64(user).into()])
}

fn row_fields(prefix: &str, key: &str, value: &str) -> [ProjectField; 3] {
    [
        ProjectField::renamed(format!("{prefix}{key}"), key),
        ProjectField::renamed(format!("{prefix}id"), "id"),
        ProjectField::renamed(format!("{prefix}{value}"), value),
    ]
}

fn feed_join(follows: GraphBuilder, user_field: Option<&str>) -> GraphBuilder {
    let mut fields = Vec::new();
    if let Some(user) = user_field {
        fields.push(ProjectField::renamed(format!("left.{user}"), user));
    }
    fields.extend(row_fields("right.", "author", "created"));
    GraphBuilder::join(
        follows,
        GraphBuilder::table("posts"),
        ["followee"],
        ["author"],
    )
    .project_fields(fields)
}

/// One-shot graph for one subscriber (the `snapshot_touched` engine).
fn snapshot_graph(workload: Workload, user: u64) -> GraphBuilder {
    match workload {
        Workload::Tasks => GraphBuilder::table_scan("tasks", user_prefix(user))
            .project_fields(row_fields("", "owner", "rev")),
        Workload::Feed => feed_join(GraphBuilder::table_scan("follows", user_prefix(user)), None),
        Workload::FeedTop20 => GraphBuilder::top_by(
            feed_join(GraphBuilder::table_scan("follows", user_prefix(user)), None),
            Vec::<String>::new(),
            [TopByOrder::desc("created")],
            ["author", "id"],
            0,
            TopByLimit::Finite(TOP_K as u64),
        ),
    }
}

/// Prepared shape keyed by a `user` binding (the `ivm` engine). Output rows
/// carry the hidden route field `user` first.
fn prepared_graph(workload: Workload) -> GraphBuilder {
    let user = GraphBuilder::binding_source(
        "user",
        RecordDescriptor::new([("user", ColumnType::U64.clone())]),
    );
    match workload {
        Workload::Tasks => {
            let mut fields = vec![ProjectField::renamed("left.user", "user")];
            fields.extend(row_fields("right.", "owner", "rev"));
            GraphBuilder::join(user, GraphBuilder::table("tasks"), ["user"], ["owner"])
                .project_fields(fields)
        }
        Workload::Feed | Workload::FeedTop20 => {
            let follows =
                GraphBuilder::join(user, GraphBuilder::table("follows"), ["user"], ["follower"])
                    .project_fields([
                        ProjectField::renamed("left.user", "user"),
                        ProjectField::renamed("right.followee", "followee"),
                    ]);
            let feed = feed_join(follows, Some("user"));
            if workload == Workload::FeedTop20 {
                GraphBuilder::top_by(
                    feed,
                    ["user"],
                    [TopByOrder::desc("created")],
                    ["author", "id"],
                    0,
                    TopByLimit::Finite(TOP_K as u64),
                )
            } else {
                feed
            }
        }
    }
}

fn pull_rows(database: &Database, workload: Workload, user: u64) -> Cache {
    let scan = |table: &str, key: u64| {
        block_on(database.primary_key_scan(table, &[Value::U64(key)])).expect("scan")
    };
    let mut rows = Vec::new();
    match workload {
        Workload::Tasks => {
            for task in scan("tasks", user) {
                let record = task.record();
                rows.push((user, get_u64(record, 1), get_u64(record, 2)));
            }
        }
        Workload::Feed | Workload::FeedTop20 => {
            for follow in scan("follows", user) {
                let author = get_u64(follow.record(), 1);
                for post in scan("posts", author) {
                    let record = post.record();
                    rows.push((author, get_u64(record, 1), get_u64(record, 2)));
                }
            }
        }
    }
    if workload == Workload::FeedTop20 {
        return top_k(rows);
    }
    let mut cache = Cache::new();
    for row in rows {
        *cache.entry(row).or_default() += 1;
    }
    cache
}

fn get_u64(record: &groove::records::OwnedRecord, index: usize) -> u64 {
    match record.get_idx(index).expect("field") {
        Value::U64(value) => value,
        other => panic!("expected u64, got {other:?}"),
    }
}

fn snapshot_rows(database: &mut Database, workload: Workload, user: u64) -> Cache {
    let deltas = block_on(database.query_graph(snapshot_graph(workload, user))).expect("query");
    let mut cache = Cache::new();
    for (values, weight) in deltas.to_values().expect("decode") {
        let row = (u64_at(&values, 0), u64_at(&values, 1), u64_at(&values, 2));
        *cache.entry(row).or_default() += weight;
    }
    cache
}

// ---------------------------------------------------------------------------
// SQLite
// ---------------------------------------------------------------------------

fn seeded_sqlite(workload: Workload) -> Connection {
    let mut conn = Connection::open_in_memory().expect("sqlite");
    conn.execute_batch(
        "
        CREATE TABLE posts(author INTEGER, id INTEGER, created INTEGER, title TEXT,
                           PRIMARY KEY(author, id)) WITHOUT ROWID;
        CREATE TABLE follows(follower INTEGER, followee INTEGER,
                             PRIMARY KEY(follower, followee)) WITHOUT ROWID;
        CREATE TABLE tasks(owner INTEGER, id INTEGER, rev INTEGER, title TEXT,
                           PRIMARY KEY(owner, id)) WITHOUT ROWID;
        ",
    )
    .expect("schema");
    let tx = conn.transaction().expect("tx");
    match workload {
        Workload::Tasks => {
            let mut insert = tx
                .prepare("INSERT INTO tasks VALUES (?1, ?2, 0, ?3)")
                .expect("prepare");
            for owner in 0..TASK_OWNERS {
                for id in 0..TASKS_PER_OWNER {
                    insert
                        .execute(params![owner, id, format!("task {id} of {owner}")])
                        .expect("insert task");
                }
            }
        }
        Workload::Feed | Workload::FeedTop20 => {
            let mut post = tx
                .prepare("INSERT INTO posts VALUES (?1, ?2, ?3, ?4)")
                .expect("prepare");
            let mut follow = tx
                .prepare("INSERT INTO follows VALUES (?1, ?2)")
                .expect("prepare");
            for author in 0..USERS {
                for p in 0..POSTS_PER_USER {
                    post.execute(params![
                        author,
                        author * POSTS_PER_USER + p,
                        initial_created(author, p),
                        post_title(author, p)
                    ])
                    .expect("insert post");
                }
                for slot in 0..FOLLOWS_PER_USER {
                    follow
                        .execute(params![author, followee(author, slot)])
                        .expect("insert follow");
                }
            }
        }
    }
    tx.commit().expect("commit seed");
    conn
}

fn sqlite_write(conn: &Connection, write: Write) {
    match write {
        Write::TaskRev { owner, id, rev } => conn
            .prepare_cached("UPDATE tasks SET rev = ?3 WHERE owner = ?1 AND id = ?2")
            .expect("prepare")
            .execute(params![owner, id, rev])
            .expect("update task"),
        Write::PostCreated {
            author,
            id,
            created,
        } => conn
            .prepare_cached("UPDATE posts SET created = ?3 WHERE author = ?1 AND id = ?2")
            .expect("prepare")
            .execute(params![author, id, created])
            .expect("update post"),
    };
}

fn sqlite_rows(conn: &Connection, workload: Workload, user: u64) -> Cache {
    let sql = match workload {
        Workload::Tasks => "SELECT owner, id, rev FROM tasks WHERE owner = ?1",
        Workload::Feed => {
            "SELECT p.author, p.id, p.created FROM follows f
             JOIN posts p ON p.author = f.followee WHERE f.follower = ?1"
        }
        Workload::FeedTop20 => {
            "SELECT p.author, p.id, p.created FROM follows f
             JOIN posts p ON p.author = f.followee WHERE f.follower = ?1
             ORDER BY p.created DESC, p.author, p.id LIMIT 20"
        }
    };
    let mut statement = conn.prepare_cached(sql).expect("prepare");
    let mut cache = Cache::new();
    let rows = statement
        .query_map(params![user], |row| {
            Ok((
                row.get::<_, u64>(0)?,
                row.get::<_, u64>(1)?,
                row.get::<_, u64>(2)?,
            ))
        })
        .expect("query");
    for row in rows {
        *cache.entry(row.expect("row")).or_default() += 1;
    }
    cache
}

// ---------------------------------------------------------------------------
// Engines
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum EngineKind {
    Ivm,
    SqliteTouched,
    SqliteAll,
    PullTouched,
    SnapshotTouched,
}

impl EngineKind {
    const ALL: [Self; 5] = [
        Self::Ivm,
        Self::SqliteTouched,
        Self::SqliteAll,
        Self::PullTouched,
        Self::SnapshotTouched,
    ];
}

enum Store {
    Ivm {
        database: Database,
        subscriptions: Vec<Subscription>,
    },
    Sqlite {
        conn: Connection,
        rerun_all: bool,
    },
    Groove {
        database: Database,
        pull: bool,
    },
}

struct Engine {
    workload: Workload,
    fixture: Fixture,
    store: Store,
    caches: Vec<Cache>,
    step: u64,
}

impl Engine {
    fn new(kind: EngineKind, workload: Workload, subs: u64) -> Self {
        let fixture = Fixture::new(subs);
        let mut caches = vec![Cache::new(); subs as usize];
        let store = match kind {
            EngineKind::Ivm => {
                let mut database = seeded_groove(workload);
                let shape = block_on(database.prepare_one_sink(
                    prepared_graph(workload),
                    "user",
                    RecordDescriptor::new([("user", ColumnType::U64.clone())]),
                    ["user"],
                ))
                .expect("prepare");
                let mut subscriptions = Vec::new();
                for user in 0..subs {
                    subscriptions.push(
                        block_on(database.bind_shape_one_sink(shape.id(), &[Value::U64(user)]))
                            .expect("bind"),
                    );
                }
                block_on(database.drive_progress()).expect("drive");
                let mut store = Store::Ivm {
                    database,
                    subscriptions,
                };
                drain_ivm(&mut store, &mut caches);
                store
            }
            EngineKind::SqliteTouched | EngineKind::SqliteAll => {
                let conn = seeded_sqlite(workload);
                for user in 0..subs {
                    caches[user as usize] = sqlite_rows(&conn, workload, user);
                }
                Store::Sqlite {
                    conn,
                    rerun_all: matches!(kind, EngineKind::SqliteAll),
                }
            }
            EngineKind::PullTouched | EngineKind::SnapshotTouched => {
                let database = seeded_groove(workload);
                for user in 0..subs {
                    caches[user as usize] = pull_rows(&database, workload, user);
                }
                Store::Groove {
                    database,
                    pull: matches!(kind, EngineKind::PullTouched),
                }
            }
        };
        Self {
            workload,
            fixture,
            store,
            caches,
            step: 0,
        }
    }

    /// Apply the next write and bring every subscriber's cache up to date.
    fn step(&mut self) {
        let write = write_at(self.workload, self.fixture.subs, self.step);
        self.step += 1;
        let workload = self.workload;
        match &mut self.store {
            Store::Ivm { database, .. } => {
                groove_write(database, write);
                block_on(database.drive_progress()).expect("drive");
                drain_ivm(&mut self.store, &mut self.caches);
            }
            Store::Sqlite { conn, rerun_all } => {
                sqlite_write(conn, write);
                let users = if *rerun_all {
                    (0..self.fixture.subs).collect()
                } else {
                    self.fixture.touched(write)
                };
                for user in users {
                    let next = sqlite_rows(conn, workload, user);
                    apply_diff(&mut self.caches[user as usize], next);
                }
            }
            Store::Groove { database, pull } => {
                groove_write(database, write);
                for user in self.fixture.touched(write) {
                    let next = if *pull {
                        pull_rows(database, workload, user)
                    } else {
                        snapshot_rows(database, workload, user)
                    };
                    apply_diff(&mut self.caches[user as usize], next);
                }
            }
        }
    }
}

fn drain_ivm(store: &mut Store, caches: &mut [Cache]) {
    let Store::Ivm { subscriptions, .. } = store else {
        return;
    };
    for (user, subscription) in subscriptions.iter().enumerate() {
        while let Ok(deltas) = subscription.try_recv() {
            let cache = &mut caches[user];
            for (values, weight) in deltas.to_values().expect("decode") {
                // Skip the hidden route field `user` at index 0.
                let row = (u64_at(&values, 1), u64_at(&values, 2), u64_at(&values, 3));
                let next = cache.get(&row).copied().unwrap_or_default() + weight;
                if next == 0 {
                    cache.remove(&row);
                } else {
                    cache.insert(row, next);
                }
            }
        }
    }
}

fn verify_engines_agree() {
    let subs = SUBSCRIBER_COUNTS[0];
    for workload in [Workload::Tasks, Workload::Feed, Workload::FeedTop20] {
        let mut reference: Option<Vec<Cache>> = None;
        for kind in EngineKind::ALL {
            let mut engine = Engine::new(kind, workload, subs);
            for _ in 0..VERIFY_WRITES {
                engine.step();
            }
            let expected_len = match workload {
                Workload::Tasks => TASKS_PER_OWNER as usize,
                Workload::Feed => (FOLLOWS_PER_USER * POSTS_PER_USER) as usize,
                Workload::FeedTop20 => TOP_K,
            };
            for (user, cache) in engine.caches.iter().enumerate() {
                assert_eq!(
                    cache.values().sum::<i64>() as usize,
                    expected_len,
                    "{workload:?} {kind:?} user {user} result size"
                );
            }
            match &reference {
                None => reference = Some(engine.caches),
                Some(expected) => assert!(
                    *expected == engine.caches,
                    "{workload:?}: {kind:?} diverges from the first engine"
                ),
            }
        }
    }
}

fn bench(bencher: divan::Bencher, kind: EngineKind, workload: Workload, subs: u64) {
    let engine = RefCell::new(Engine::new(kind, workload, subs));
    bencher.bench_local(|| engine.borrow_mut().step());
}

macro_rules! workload_benches {
    ($module:ident, $workload:expr) => {
        mod $module {
            use super::*;

            #[divan::bench(args = SUBSCRIBER_COUNTS)]
            fn ivm(bencher: divan::Bencher, subs: u64) {
                bench(bencher, EngineKind::Ivm, $workload, subs);
            }

            #[divan::bench(args = SUBSCRIBER_COUNTS)]
            fn sqlite_touched(bencher: divan::Bencher, subs: u64) {
                bench(bencher, EngineKind::SqliteTouched, $workload, subs);
            }

            #[divan::bench(args = SUBSCRIBER_COUNTS)]
            fn sqlite_all(bencher: divan::Bencher, subs: u64) {
                bench(bencher, EngineKind::SqliteAll, $workload, subs);
            }

            #[divan::bench(args = SUBSCRIBER_COUNTS)]
            fn pull_touched(bencher: divan::Bencher, subs: u64) {
                bench(bencher, EngineKind::PullTouched, $workload, subs);
            }

            #[divan::bench(args = SUBSCRIBER_COUNTS)]
            fn snapshot_touched(bencher: divan::Bencher, subs: u64) {
                bench(bencher, EngineKind::SnapshotTouched, $workload, subs);
            }
        }
    };
}

workload_benches!(tasks, Workload::Tasks);
workload_benches!(feed, Workload::Feed);
workload_benches!(feed_top20, Workload::FeedTop20);
