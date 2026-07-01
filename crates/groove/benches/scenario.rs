use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::env;
use std::time::{Duration, Instant};

use groove::db::Subscription;
use groove::db::{Database, GraphBuilder, PredicateExpr, PrimaryKeyValue};
use groove::ivm::{ProjectField, RecordDeltas};
use groove::queries::{
    BinaryOp, ColumnRef, Expr, JoinConstraint, JoinKind, Query, Select, SelectItem, TableRef,
};
use groove::records::{RecordDescriptor, Value};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::{Durability, OrderedKvStorage, RocksDbStorage};
use hdrhistogram::Histogram;
use rusqlite::{Connection, params};

fn main() {
    let scenario = env::var("GROOVE_SCENARIO").unwrap_or_else(|_| "social_feed".to_owned());
    let engine = env::var("GROOVE_ENGINE").unwrap_or_else(|_| "groove".to_owned());
    match (scenario.as_str(), engine.as_str()) {
        ("social_feed", "groove") => run_groove_social_feed(),
        ("social_feed", "groove_prepared") => run_groove_social_feed_prepared(),
        ("social_feed", "groove_prepared_sql") => run_groove_social_feed_prepared_sql(),
        ("social_feed", "sqlite_naive") => run_sqlite_social_feed(SqliteMode::Naive),
        ("social_feed", "sqlite_indexed") => run_sqlite_social_feed(SqliteMode::IndexedTouched),
        ("acl", "groove") => run_groove_acl(),
        ("acl", "groove_prepared") => run_groove_acl_prepared(),
        ("acl", "sqlite_naive" | "sqlite_indexed") => run_sqlite_acl(&engine),
        ("oneshot", "groove_query") => run_oneshot_query(),
        ("oneshot", "groove_subscribe") => run_oneshot_subscribe(),
        ("oneshot", "groove_scan") => run_oneshot_scan(),
        _ => {
            eprintln!(
                "expected GROOVE_SCENARIO=social_feed|acl|oneshot and GROOVE_ENGINE=groove|groove_prepared|groove_prepared_sql|sqlite_naive|sqlite_indexed|groove_query|groove_subscribe|groove_scan"
            );
            std::process::exit(2);
        }
    }
}

fn apply_bench_auto_family_override<S: OrderedKvStorage>(db: &mut Database<S>) {
    if env::var_os("GROOVE_BENCH_DISABLE_AUTO_DIRECT_FAMILY").is_some() {
        db.set_auto_direct_family_enabled(false);
    }
}

fn run_groove_social_feed() {
    let config = SocialFeedConfig::from_env();
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let storage = RocksDbStorage::open_with_durability(
        temp_dir.path(),
        &["users", "posts", "follows", "likes", "indices"],
        Durability::WalNoSync,
    )
    .expect("open rocksdb");
    let mut db = Database::new(social_feed_schema(), storage).expect("database");
    apply_bench_auto_family_override(&mut db);
    let mut workload = SocialFeedWorkload::new(config);

    seed_groove(&mut db, &mut workload);

    let subscribe_start = Instant::now();
    let mut subscriptions = Vec::with_capacity(config.subscriptions);
    for user_id in 1..=config.subscriptions as u64 {
        subscriptions.push(
            db.subscribe_one_sink(feed_graph(user_id))
                .expect("subscribe social feed"),
        );
    }
    let subscribe_elapsed = subscribe_start.elapsed();
    let mut caches = BTreeMap::<u64, BTreeMap<FeedRow, i64>>::new();
    for (idx, subscription) in subscriptions.iter().enumerate() {
        let user_id = idx as u64 + 1;
        apply_feed_notification(
            caches.entry(user_id).or_default(),
            subscription.recv().expect("initial notification"),
        );
    }

    let mut metrics = ScenarioMetrics::default();
    for step in 0..config.commits {
        let event = workload.next_event(step);
        let commit_start = Instant::now();
        let mut batch = db.open_batch();
        apply_groove_event(&mut batch, event);
        db.commit_batch(batch).expect("commit");
        metrics.record_commit(commit_start.elapsed());
        let commit = db.last_commit_metrics().expect("commit metrics");
        metrics.record_storage(commit.storage_write_time);
        metrics.record_tick(commit.ivm_tick_time);
        metrics.engine_records_processed += commit.tick.records_processed;
        for (idx, subscription) in subscriptions.iter().enumerate() {
            let user_id = idx as u64 + 1;
            drain_feed_subscription(
                caches.entry(user_id).or_default(),
                subscription,
                &mut metrics,
            );
        }
    }
    for user_id in 1..=config.subscriptions as u64 {
        let expected = expected_feed_rows(&workload, user_id);
        let actual = caches.get(&user_id).cloned().unwrap_or_default();
        assert_eq!(
            actual, expected,
            "groove feed cache mismatch for subscription user {user_id}"
        );
    }

    let stats = db
        .last_tick_metrics()
        .map(|tick| tick.runtime_stats.clone())
        .unwrap_or_default();
    print_json(JsonReport {
        scenario: "social_feed",
        engine: "groove",
        durability: "wal_no_sync",
        config,
        subscribe_elapsed,
        metrics,
        graph_nodes: stats.graph_nodes,
        arrangements: stats.arrangement_count,
        arrangement_rows: stats.arrangement_rows,
        arrangement_bytes: stats.arrangement_encoded_bytes,
        logical_nodes_requested: stats.logical_nodes_requested,
        deduped_graph_nodes: stats.deduped_graph_nodes,
        dedupe_ratio: stats.dedupe_ratio(),
        result_cache_rows: caches.values().map(BTreeMap::len).sum(),
    });
}

fn run_groove_social_feed_prepared() {
    run_groove_social_feed_prepared_with(PreparedFeedMode::Builder);
}

fn run_groove_social_feed_prepared_sql() {
    run_groove_social_feed_prepared_with(PreparedFeedMode::Sql);
}

#[derive(Clone, Copy)]
enum PreparedFeedMode {
    Builder,
    Sql,
}

impl PreparedFeedMode {
    fn name(self) -> &'static str {
        match self {
            Self::Builder => "groove_prepared",
            Self::Sql => "groove_prepared_sql",
        }
    }
}

fn run_groove_social_feed_prepared_with(mode: PreparedFeedMode) {
    let config = SocialFeedConfig::from_env();
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let storage = RocksDbStorage::open_with_durability(
        temp_dir.path(),
        &["users", "posts", "follows", "likes", "indices"],
        Durability::WalNoSync,
    )
    .expect("open rocksdb");
    let mut db = Database::new(social_feed_schema(), storage).expect("database");
    let mut workload = SocialFeedWorkload::new(config);

    seed_groove(&mut db, &mut workload);

    let subscribe_start = Instant::now();
    let mut subscriptions = Vec::with_capacity(config.subscriptions);
    match mode {
        PreparedFeedMode::Builder => {
            let family = db
                .prepare_one_sink(
                    feed_prepared_shape_graph(),
                    "feed_params",
                    RecordDescriptor::new([("follower_id", ColumnType::U64.value_type())]),
                    ["follower_id"],
                )
                .expect("subscribe feed family");
            for user_id in 1..=config.subscriptions as u64 {
                subscriptions.push(
                    db.bind_shape_one_sink(family.id(), &[Value::U64(user_id)])
                        .expect("subscribe feed param"),
                );
            }
        }
        PreparedFeedMode::Sql => {
            let prepared = db
                .prepare_query(feed_prepared_shape_query())
                .expect("prepare feed family query");
            for user_id in 1..=config.subscriptions as u64 {
                subscriptions.push(
                    db.bind(&prepared, &[("follower_id", Value::U64(user_id))])
                        .expect("subscribe prepared feed"),
                );
            }
        }
    }
    let subscribe_elapsed = subscribe_start.elapsed();
    let mut caches = BTreeMap::<u64, BTreeMap<FeedRow, i64>>::new();
    for (idx, subscription) in subscriptions.iter().enumerate() {
        let user_id = idx as u64 + 1;
        apply_param_feed_notification(
            caches.entry(user_id).or_default(),
            subscription.recv().expect("initial notification"),
        );
    }

    let mut metrics = ScenarioMetrics::default();
    for step in 0..config.commits {
        let event = workload.next_event(step);
        let commit_start = Instant::now();
        let mut batch = db.open_batch();
        apply_groove_event(&mut batch, event);
        db.commit_batch(batch).expect("commit");
        metrics.record_commit(commit_start.elapsed());
        let commit = db.last_commit_metrics().expect("commit metrics");
        metrics.record_storage(commit.storage_write_time);
        metrics.record_tick(commit.ivm_tick_time);
        metrics.engine_records_processed += commit.tick.records_processed;
        for (idx, subscription) in subscriptions.iter().enumerate() {
            let user_id = idx as u64 + 1;
            drain_param_feed_subscription(
                caches.entry(user_id).or_default(),
                subscription,
                &mut metrics,
            );
        }
    }
    for user_id in 1..=config.subscriptions as u64 {
        let expected = expected_feed_rows(&workload, user_id);
        let actual = caches.get(&user_id).cloned().unwrap_or_default();
        assert_eq!(
            actual, expected,
            "groove param feed cache mismatch for subscription user {user_id}"
        );
    }

    let stats = db
        .last_tick_metrics()
        .map(|tick| tick.runtime_stats.clone())
        .unwrap_or_default();
    print_json(JsonReport {
        scenario: "social_feed",
        engine: mode.name(),
        durability: "wal_no_sync",
        config,
        subscribe_elapsed,
        metrics,
        graph_nodes: stats.graph_nodes,
        arrangements: stats.arrangement_count,
        arrangement_rows: stats.arrangement_rows,
        arrangement_bytes: stats.arrangement_encoded_bytes,
        logical_nodes_requested: stats.logical_nodes_requested,
        deduped_graph_nodes: stats.deduped_graph_nodes,
        dedupe_ratio: stats.dedupe_ratio(),
        result_cache_rows: caches.values().map(BTreeMap::len).sum(),
    });
}

#[derive(Clone, Copy)]
enum SqliteMode {
    Naive,
    IndexedTouched,
}

impl SqliteMode {
    fn name(self) -> &'static str {
        match self {
            Self::Naive => "sqlite_naive",
            Self::IndexedTouched => "sqlite_indexed",
        }
    }
}

fn run_sqlite_social_feed(mode: SqliteMode) {
    let config = SocialFeedConfig::from_env();
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let mut conn = Connection::open(temp_dir.path().join("sqlite.db")).expect("sqlite");
    setup_sqlite(&conn);
    let mut workload = SocialFeedWorkload::new(config);

    seed_sqlite(&mut conn, &mut workload);

    let subscribe_start = Instant::now();
    let mut caches = BTreeMap::<u64, BTreeMap<FeedRow, i64>>::new();
    for user_id in 1..=config.subscriptions as u64 {
        caches.insert(user_id, sqlite_feed_rows(&conn, user_id));
    }
    let subscribe_elapsed = subscribe_start.elapsed();

    let mut metrics = ScenarioMetrics::default();
    let mut notifications = 0usize;
    let mut notification_records = 0usize;
    for step in 0..config.commits {
        let event = workload.next_event(step);
        let touched = workload.touched_subscriptions(event, mode);
        let commit_start = Instant::now();
        let storage_start = Instant::now();
        apply_sqlite_event(&mut conn, event);
        let storage_elapsed = storage_start.elapsed();
        let tick_start = Instant::now();
        for user_id in touched {
            let next = sqlite_feed_rows(&conn, user_id);
            let previous = caches.entry(user_id).or_default();
            let diff_count = diff_row_count(previous, &next);
            if diff_count > 0 {
                notifications += 1;
                notification_records += diff_count;
            }
            *previous = next;
        }
        let tick_elapsed = tick_start.elapsed();
        metrics.record_commit(commit_start.elapsed());
        metrics.record_storage(storage_elapsed);
        metrics.record_tick(tick_elapsed);
    }
    metrics.notifications = notifications;
    metrics.notification_records = notification_records;
    metrics.engine_records_processed = 0;
    for user_id in 1..=config.subscriptions as u64 {
        let expected = sqlite_feed_rows(&conn, user_id);
        let actual = caches.get(&user_id).cloned().unwrap_or_default();
        assert_eq!(
            actual, expected,
            "sqlite feed cache mismatch for subscription user {user_id}"
        );
    }

    print_json(JsonReport {
        scenario: "social_feed",
        engine: mode.name(),
        durability: "wal_normal",
        config,
        subscribe_elapsed,
        metrics,
        graph_nodes: 0,
        arrangements: 0,
        arrangement_rows: 0,
        arrangement_bytes: 0,
        logical_nodes_requested: 0,
        deduped_graph_nodes: 0,
        dedupe_ratio: 1.0,
        result_cache_rows: caches.values().map(BTreeMap::len).sum(),
    });
}

#[derive(Clone, Copy)]
struct SocialFeedConfig {
    seed: u64,
    users: usize,
    subscriptions: usize,
    initial_follows: usize,
    commits: usize,
}

impl SocialFeedConfig {
    fn from_env() -> Self {
        let subscriptions = env_usize("GROOVE_SUBSCRIPTIONS", 100);
        Self {
            seed: env_u64("GROOVE_SEED", 0x5eed),
            users: env_usize("GROOVE_USERS", 1_000),
            subscriptions,
            initial_follows: env_usize("GROOVE_INITIAL_FOLLOWS", subscriptions * 50),
            commits: env_usize("GROOVE_COMMITS", 250),
        }
    }
}

#[derive(Clone, Copy)]
enum WorkloadEvent {
    Follow {
        id: u64,
        follower: u64,
        followee: u64,
    },
    Like {
        id: u64,
        user: u64,
        post: u64,
        created_at: u64,
    },
    Post {
        id: u64,
        author: u64,
        created_at: u64,
    },
}

struct SocialFeedWorkload {
    config: SocialFeedConfig,
    rng: Rng,
    next_post_id: u64,
    next_like_id: u64,
    next_follow_id: u64,
    followers_by_followee: HashMap<u64, BTreeMap<u64, i64>>,
    posts: Vec<FeedRow>,
}

impl SocialFeedWorkload {
    fn new(config: SocialFeedConfig) -> Self {
        Self {
            config,
            rng: Rng::new(config.seed),
            next_post_id: 1,
            next_like_id: 1,
            next_follow_id: 1,
            followers_by_followee: HashMap::new(),
            posts: Vec::new(),
        }
    }

    fn seed_follows(&mut self) -> Vec<WorkloadEvent> {
        (0..self.config.initial_follows)
            .map(|_| self.next_follow_event())
            .collect()
    }

    fn next_event(&mut self, step: usize) -> WorkloadEvent {
        match step % 10 {
            0 => self.next_follow_event(),
            1 => {
                let user = zipf_sample(&mut self.rng, self.config.users as u64);
                let post = zipf_sample(&mut self.rng, self.next_post_id.max(1));
                let event = WorkloadEvent::Like {
                    id: self.next_like_id,
                    user,
                    post,
                    created_at: step as u64,
                };
                self.next_like_id += 1;
                event
            }
            _ => {
                let author = zipf_sample(&mut self.rng, self.config.users as u64);
                let event = WorkloadEvent::Post {
                    id: self.next_post_id,
                    author,
                    created_at: step as u64,
                };
                self.posts.push(FeedRow {
                    author_id: author,
                    post_id: self.next_post_id,
                    created_at: step as u64,
                });
                self.next_post_id += 1;
                event
            }
        }
    }

    fn next_follow_event(&mut self) -> WorkloadEvent {
        let follower = zipf_sample(&mut self.rng, self.config.subscriptions as u64);
        let followee = zipf_sample(&mut self.rng, self.config.users as u64);
        self.followers_by_followee
            .entry(followee)
            .or_default()
            .entry(follower)
            .and_modify(|count| *count += 1)
            .or_insert(1);
        let event = WorkloadEvent::Follow {
            id: self.next_follow_id,
            follower,
            followee,
        };
        self.next_follow_id += 1;
        event
    }

    fn touched_subscriptions(&self, event: WorkloadEvent, mode: SqliteMode) -> Vec<u64> {
        match mode {
            SqliteMode::Naive => (1..=self.config.subscriptions as u64).collect(),
            SqliteMode::IndexedTouched => match event {
                WorkloadEvent::Post { author, .. } => self
                    .followers_by_followee
                    .get(&author)
                    .map(|followers| followers.keys().copied().collect())
                    .unwrap_or_default(),
                WorkloadEvent::Follow { follower, .. } => vec![follower],
                WorkloadEvent::Like { .. } => Vec::new(),
            },
        }
    }
}

fn expected_feed_rows(workload: &SocialFeedWorkload, user_id: u64) -> BTreeMap<FeedRow, i64> {
    let mut rows = BTreeMap::new();
    for post in &workload.posts {
        let Some(followers) = workload.followers_by_followee.get(&post.author_id) else {
            continue;
        };
        if let Some(count) = followers.get(&user_id) {
            *rows.entry(*post).or_default() += *count;
        }
    }
    rows
}

fn social_feed_schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "users",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("region", ColumnType::String),
                ColumnSchema::new("plan", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "posts",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("author_id", ColumnType::U64),
                ColumnSchema::new("created_at", ColumnType::U64),
                ColumnSchema::new("visibility", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "follows",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("follower_id", ColumnType::U64),
                ColumnSchema::new("followee_id", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "likes",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("user_id", ColumnType::U64),
                ColumnSchema::new("post_id", ColumnType::U64),
                ColumnSchema::new("created_at", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ])
}

fn feed_graph(user_id: u64) -> GraphBuilder {
    let follows = GraphBuilder::table("follows")
        .filter(PredicateExpr::eq("follower_id", Value::U64(user_id)))
        .project(["followee_id"]);
    let posts = GraphBuilder::table("posts").project(["author_id", "id", "created_at"]);
    GraphBuilder::join(follows, posts, ["followee_id"], ["author_id"]).project_fields([
        ProjectField::renamed("right.author_id", "author_id"),
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.created_at", "created_at"),
    ])
}

fn feed_prepared_shape_graph() -> GraphBuilder {
    let params = GraphBuilder::binding_source(
        "feed_params",
        RecordDescriptor::new([("follower_id", ColumnType::U64.value_type())]),
    );
    let follows = GraphBuilder::table("follows").project(["follower_id", "followee_id"]);
    let followed = GraphBuilder::join(params, follows, ["follower_id"], ["follower_id"])
        .project_fields([
            ProjectField::renamed("left.follower_id", "follower_id"),
            ProjectField::renamed("right.followee_id", "followee_id"),
        ]);
    let posts = GraphBuilder::table("posts").project(["author_id", "id", "created_at"]);
    GraphBuilder::join(followed, posts, ["followee_id"], ["author_id"]).project_fields([
        ProjectField::renamed("left.follower_id", "follower_id"),
        ProjectField::renamed("right.author_id", "author_id"),
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.created_at", "created_at"),
    ])
}

fn feed_prepared_shape_query() -> Query {
    Query::Select(Box::new(
        Select::new([
            SelectItem::expr(Expr::Column(ColumnRef::qualified(["f"], "follower_id"))),
            SelectItem::expr(Expr::Column(ColumnRef::qualified(["p"], "author_id"))),
            SelectItem::expr(Expr::Column(ColumnRef::qualified(["p"], "id"))),
            SelectItem::expr(Expr::Column(ColumnRef::qualified(["p"], "created_at"))),
        ])
        .from([TableRef::Join {
            left: Box::new(TableRef::named("follows").aliased("f")),
            right: Box::new(TableRef::named("posts").aliased("p")),
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(Expr::binary(
                Expr::Column(ColumnRef::qualified(["p"], "author_id")),
                BinaryOp::Eq,
                Expr::Column(ColumnRef::qualified(["f"], "followee_id")),
            )),
        }])
        .where_(Expr::binary(
            Expr::Column(ColumnRef::qualified(["f"], "follower_id")),
            BinaryOp::Eq,
            Expr::parameter("follower_id"),
        )),
    ))
}

fn seed_groove(db: &mut Database<RocksDbStorage>, workload: &mut SocialFeedWorkload) {
    let mut batch = db.open_batch();
    for user_id in 1..=workload.config.users as u64 {
        batch.insert(
            "users",
            vec![
                Value::U64(user_id),
                Value::String(format!("r{}", user_id % 16)),
                Value::String(if user_id % 7 == 0 { "pro" } else { "free" }.to_owned()),
            ],
        );
    }
    db.commit_batch(batch).expect("seed users");

    let mut batch = db.open_batch();
    for event in workload.seed_follows() {
        apply_groove_event(&mut batch, event);
    }
    db.commit_batch(batch).expect("seed follows");
}

fn apply_groove_event(batch: &mut groove::db::DatabaseBatch, event: WorkloadEvent) {
    match event {
        WorkloadEvent::Follow {
            id,
            follower,
            followee,
        } => batch.insert(
            "follows",
            vec![Value::U64(id), Value::U64(follower), Value::U64(followee)],
        ),
        WorkloadEvent::Like {
            id,
            user,
            post,
            created_at,
        } => batch.insert(
            "likes",
            vec![
                Value::U64(id),
                Value::U64(user),
                Value::U64(post),
                Value::U64(created_at),
            ],
        ),
        WorkloadEvent::Post {
            id,
            author,
            created_at,
        } => batch.insert(
            "posts",
            vec![
                Value::U64(id),
                Value::U64(author),
                Value::U64(created_at),
                Value::String("public".to_owned()),
            ],
        ),
    }
}

fn setup_sqlite(conn: &Connection) {
    conn.execute_batch(
        "
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        CREATE TABLE users(id INTEGER PRIMARY KEY, region TEXT NOT NULL, plan TEXT NOT NULL);
        CREATE TABLE posts(id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, created_at INTEGER NOT NULL, visibility TEXT NOT NULL);
        CREATE TABLE follows(id INTEGER PRIMARY KEY, follower_id INTEGER NOT NULL, followee_id INTEGER NOT NULL);
        CREATE TABLE likes(id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, post_id INTEGER NOT NULL, created_at INTEGER NOT NULL);
        CREATE INDEX posts_by_author ON posts(author_id);
        CREATE INDEX follows_by_follower ON follows(follower_id);
        CREATE INDEX follows_by_followee ON follows(followee_id);
        ",
    )
    .expect("setup sqlite");
}

fn seed_sqlite(conn: &mut Connection, workload: &mut SocialFeedWorkload) {
    let tx = conn.transaction().expect("tx");
    for user_id in 1..=workload.config.users as u64 {
        tx.execute(
            "INSERT INTO users(id, region, plan) VALUES (?1, ?2, ?3)",
            params![
                user_id,
                format!("r{}", user_id % 16),
                if user_id % 7 == 0 { "pro" } else { "free" }
            ],
        )
        .expect("insert user");
    }
    tx.commit().expect("commit users");

    let tx = conn.transaction().expect("tx");
    for event in workload.seed_follows() {
        apply_sqlite_event_in_tx(&tx, event);
    }
    tx.commit().expect("commit follows");
}

fn apply_sqlite_event(conn: &mut Connection, event: WorkloadEvent) {
    let tx = conn.transaction().expect("tx");
    apply_sqlite_event_in_tx(&tx, event);
    tx.commit().expect("commit event");
}

fn apply_sqlite_event_in_tx(tx: &rusqlite::Transaction<'_>, event: WorkloadEvent) {
    match event {
        WorkloadEvent::Follow {
            id,
            follower,
            followee,
        } => {
            tx.execute(
                "INSERT INTO follows(id, follower_id, followee_id) VALUES (?1, ?2, ?3)",
                params![id, follower, followee],
            )
            .expect("insert follow");
        }
        WorkloadEvent::Like {
            id,
            user,
            post,
            created_at,
        } => {
            tx.execute(
                "INSERT INTO likes(id, user_id, post_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                params![id, user, post, created_at],
            )
            .expect("insert like");
        }
        WorkloadEvent::Post {
            id,
            author,
            created_at,
        } => {
            tx.execute(
                "INSERT INTO posts(id, author_id, created_at, visibility) VALUES (?1, ?2, ?3, 'public')",
                params![id, author, created_at],
            )
            .expect("insert post");
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct FeedRow {
    author_id: u64,
    post_id: u64,
    created_at: u64,
}

fn feed_row_from_values(values: Vec<Value>) -> FeedRow {
    match values.as_slice() {
        [
            Value::U64(author_id),
            Value::U64(post_id),
            Value::U64(created_at),
        ] => FeedRow {
            author_id: *author_id,
            post_id: *post_id,
            created_at: *created_at,
        },
        _ => panic!("unexpected feed row shape: {values:?}"),
    }
}

fn param_feed_row_from_values(values: Vec<Value>) -> FeedRow {
    match values.as_slice() {
        [
            Value::U64(_follower_id),
            Value::U64(author_id),
            Value::U64(post_id),
            Value::U64(created_at),
        ] => FeedRow {
            author_id: *author_id,
            post_id: *post_id,
            created_at: *created_at,
        },
        _ => panic!("unexpected param feed row shape: {values:?}"),
    }
}

fn apply_feed_notification(cache: &mut BTreeMap<FeedRow, i64>, notification: RecordDeltas) {
    for (values, weight) in notification.to_values().expect("feed notification values") {
        let row = feed_row_from_values(values);
        apply_weight(cache, row, weight);
    }
}

fn apply_param_feed_notification(cache: &mut BTreeMap<FeedRow, i64>, notification: RecordDeltas) {
    for (values, weight) in notification
        .to_values()
        .expect("param feed notification values")
    {
        let row = param_feed_row_from_values(values);
        apply_weight(cache, row, weight);
    }
}

fn drain_feed_subscription(
    cache: &mut BTreeMap<FeedRow, i64>,
    subscription: &Subscription,
    metrics: &mut ScenarioMetrics,
) {
    while let Ok(notification) = subscription.try_recv() {
        metrics.notifications += 1;
        metrics.notification_records += notification.deltas.len();
        apply_feed_notification(cache, notification);
    }
}

fn drain_param_feed_subscription(
    cache: &mut BTreeMap<FeedRow, i64>,
    subscription: &Subscription,
    metrics: &mut ScenarioMetrics,
) {
    while let Ok(notification) = subscription.try_recv() {
        metrics.notifications += 1;
        metrics.notification_records += notification.deltas.len();
        apply_param_feed_notification(cache, notification);
    }
}

fn sqlite_feed_rows(conn: &Connection, user_id: u64) -> BTreeMap<FeedRow, i64> {
    let mut stmt = conn
        .prepare(
            "
            SELECT p.author_id, p.id, p.created_at
            FROM follows f
            JOIN posts p ON p.author_id = f.followee_id
            WHERE f.follower_id = ?1
            ",
        )
        .expect("prepare feed");
    let mut rows = BTreeMap::new();
    let iter = stmt
        .query_map(params![user_id], |row| {
            Ok(FeedRow {
                author_id: row.get::<_, u64>(0)?,
                post_id: row.get::<_, u64>(1)?,
                created_at: row.get::<_, u64>(2)?,
            })
        })
        .expect("query feed");
    for row in iter {
        *rows.entry(row.expect("feed row")).or_default() += 1;
    }
    rows
}

fn diff_row_count(previous: &BTreeMap<FeedRow, i64>, next: &BTreeMap<FeedRow, i64>) -> usize {
    let keys = previous
        .keys()
        .chain(next.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    keys.into_iter()
        .filter(|key| previous.get(key) != next.get(key))
        .count()
}

fn apply_weight<Row: Ord>(cache: &mut BTreeMap<Row, i64>, row: Row, weight: i64) {
    let next = cache.get(&row).copied().unwrap_or_default() + weight;
    if next == 0 {
        cache.remove(&row);
    } else {
        cache.insert(row, next);
    }
}

fn run_groove_acl() {
    let config = AclConfig::from_env();
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let storage = RocksDbStorage::open_with_durability(
        temp_dir.path(),
        &[
            "principals",
            "groups",
            "group_membership",
            "resources",
            "grants",
            "indices",
        ],
        Durability::WalNoSync,
    )
    .expect("open rocksdb");
    let mut db = Database::new(acl_schema(), storage).expect("database");
    apply_bench_auto_family_override(&mut db);
    let mut workload = AclWorkload::new(config);
    seed_groove_acl(&mut db, &mut workload);

    let subscribe_start = Instant::now();
    let mut subscriptions = Vec::with_capacity(config.subscriptions);
    for principal in 1..=config.subscriptions as u64 {
        subscriptions.push(
            db.subscribe_one_sink(acl_graph(principal))
                .expect("subscribe acl graph"),
        );
    }
    let subscribe_elapsed = subscribe_start.elapsed();
    let mut caches = BTreeMap::<u64, BTreeMap<AclRow, i64>>::new();
    for (idx, subscription) in subscriptions.iter().enumerate() {
        let principal = idx as u64 + 1;
        apply_acl_notification(
            caches.entry(principal).or_default(),
            subscription.recv().expect("initial notification"),
        );
    }

    let mut metrics = ScenarioMetrics::default();
    for step in 0..config.commits {
        let event = workload.next_event(step);
        let commit_start = Instant::now();
        let mut batch = db.open_batch();
        apply_groove_acl_event(&mut batch, event);
        db.commit_batch(batch).expect("acl commit");
        metrics.record_commit(commit_start.elapsed());
        let commit = db.last_commit_metrics().expect("commit metrics");
        metrics.record_storage(commit.storage_write_time);
        metrics.record_tick(commit.ivm_tick_time);
        metrics.engine_records_processed += commit.tick.records_processed;
        for (idx, subscription) in subscriptions.iter().enumerate() {
            let principal = idx as u64 + 1;
            drain_acl_subscription(
                caches.entry(principal).or_default(),
                subscription,
                &mut metrics,
            );
        }
    }
    for principal in 1..=config.subscriptions as u64 {
        let expected = workload.expected_acl_rows(principal);
        let actual = caches.get(&principal).cloned().unwrap_or_default();
        assert_eq!(
            actual, expected,
            "groove acl cache mismatch for subscription principal {principal}"
        );
    }

    let stats = db
        .last_tick_metrics()
        .map(|tick| tick.runtime_stats.clone())
        .unwrap_or_default();
    print_json(JsonReport {
        scenario: "acl",
        engine: "groove",
        durability: "wal_no_sync",
        config: config.as_report_config(),
        subscribe_elapsed,
        metrics,
        graph_nodes: stats.graph_nodes,
        arrangements: stats.arrangement_count,
        arrangement_rows: stats.arrangement_rows,
        arrangement_bytes: stats.arrangement_encoded_bytes,
        logical_nodes_requested: stats.logical_nodes_requested,
        deduped_graph_nodes: stats.deduped_graph_nodes,
        dedupe_ratio: stats.dedupe_ratio(),
        result_cache_rows: caches.values().map(BTreeMap::len).sum(),
    });
}

fn run_groove_acl_prepared() {
    let config = AclConfig::from_env();
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let storage = RocksDbStorage::open_with_durability(
        temp_dir.path(),
        &[
            "principals",
            "groups",
            "group_membership",
            "resources",
            "grants",
            "indices",
        ],
        Durability::WalNoSync,
    )
    .expect("open rocksdb");
    let mut db = Database::new(acl_schema(), storage).expect("database");
    let mut workload = AclWorkload::new(config);
    seed_groove_acl(&mut db, &mut workload);

    let subscribe_start = Instant::now();
    let family = db
        .prepare_one_sink(
            acl_prepared_shape_graph(),
            "acl_params",
            RecordDescriptor::new([("principal_id", ColumnType::U64.value_type())]),
            ["principal_id"],
        )
        .expect("subscribe acl family");
    let mut subscriptions = Vec::with_capacity(config.subscriptions);
    for principal in 1..=config.subscriptions as u64 {
        subscriptions.push(
            db.bind_shape_one_sink(family.id(), &[Value::U64(principal)])
                .expect("subscribe acl param"),
        );
    }
    let subscribe_elapsed = subscribe_start.elapsed();
    let mut caches = BTreeMap::<u64, BTreeMap<AclRow, i64>>::new();
    for (idx, subscription) in subscriptions.iter().enumerate() {
        let principal = idx as u64 + 1;
        apply_param_acl_notification(
            caches.entry(principal).or_default(),
            subscription.recv().expect("initial notification"),
        );
    }

    let mut metrics = ScenarioMetrics::default();
    for step in 0..config.commits {
        let event = workload.next_event(step);
        let commit_start = Instant::now();
        let mut batch = db.open_batch();
        apply_groove_acl_event(&mut batch, event);
        db.commit_batch(batch).expect("acl commit");
        metrics.record_commit(commit_start.elapsed());
        let commit = db.last_commit_metrics().expect("commit metrics");
        metrics.record_storage(commit.storage_write_time);
        metrics.record_tick(commit.ivm_tick_time);
        metrics.engine_records_processed += commit.tick.records_processed;
        for (idx, subscription) in subscriptions.iter().enumerate() {
            let principal = idx as u64 + 1;
            drain_param_acl_subscription(
                caches.entry(principal).or_default(),
                subscription,
                &mut metrics,
            );
        }
    }
    for principal in 1..=config.subscriptions as u64 {
        let expected = workload.expected_acl_rows(principal);
        let actual = caches.get(&principal).cloned().unwrap_or_default();
        assert_eq!(
            actual, expected,
            "groove param acl cache mismatch for subscription principal {principal}"
        );
    }

    let stats = db
        .last_tick_metrics()
        .map(|tick| tick.runtime_stats.clone())
        .unwrap_or_default();
    print_json(JsonReport {
        scenario: "acl",
        engine: "groove_prepared",
        durability: "wal_no_sync",
        config: config.as_report_config(),
        subscribe_elapsed,
        metrics,
        graph_nodes: stats.graph_nodes,
        arrangements: stats.arrangement_count,
        arrangement_rows: stats.arrangement_rows,
        arrangement_bytes: stats.arrangement_encoded_bytes,
        logical_nodes_requested: stats.logical_nodes_requested,
        deduped_graph_nodes: stats.deduped_graph_nodes,
        dedupe_ratio: stats.dedupe_ratio(),
        result_cache_rows: caches.values().map(BTreeMap::len).sum(),
    });
}

fn run_sqlite_acl(engine: &str) {
    let config = AclConfig::from_env();
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let mut conn = Connection::open(temp_dir.path().join("sqlite.db")).expect("sqlite");
    setup_sqlite_acl(&conn);
    let mut workload = AclWorkload::new(config);
    seed_sqlite_acl(&mut conn, &mut workload);

    let subscribe_start = Instant::now();
    let mut caches = BTreeMap::<u64, BTreeMap<AclRow, i64>>::new();
    for principal in 1..=config.subscriptions as u64 {
        caches.insert(principal, sqlite_acl_rows(&conn, principal));
    }
    let subscribe_elapsed = subscribe_start.elapsed();

    let mut metrics = ScenarioMetrics::default();
    for step in 0..config.commits {
        let event = workload.next_event(step);
        let commit_start = Instant::now();
        let storage_start = Instant::now();
        apply_sqlite_acl_event(&mut conn, event);
        let storage_elapsed = storage_start.elapsed();
        let tick_start = Instant::now();
        for principal in 1..=config.subscriptions as u64 {
            let next = sqlite_acl_rows(&conn, principal);
            let previous = caches.entry(principal).or_default();
            let diff_count = diff_acl_row_count(previous, &next);
            if diff_count > 0 {
                metrics.notifications += 1;
                metrics.notification_records += diff_count;
            }
            *previous = next;
        }
        metrics.record_commit(commit_start.elapsed());
        metrics.record_storage(storage_elapsed);
        metrics.record_tick(tick_start.elapsed());
    }
    metrics.engine_records_processed = 0;
    for principal in 1..=config.subscriptions as u64 {
        let expected = sqlite_acl_rows(&conn, principal);
        let actual = caches.get(&principal).cloned().unwrap_or_default();
        assert_eq!(
            actual, expected,
            "sqlite acl cache mismatch for subscription principal {principal}"
        );
    }

    print_json(JsonReport {
        scenario: "acl",
        engine,
        durability: "wal_normal",
        config: config.as_report_config(),
        subscribe_elapsed,
        metrics,
        graph_nodes: 0,
        arrangements: 0,
        arrangement_rows: 0,
        arrangement_bytes: 0,
        logical_nodes_requested: 0,
        deduped_graph_nodes: 0,
        dedupe_ratio: 1.0,
        result_cache_rows: caches.values().map(BTreeMap::len).sum(),
    });
}

#[derive(Clone, Copy)]
enum AclSeries {
    Insert,
    Delete,
}

#[derive(Clone, Copy)]
struct AclConfig {
    seed: u64,
    principals: usize,
    groups: usize,
    resources: usize,
    subscriptions: usize,
    initial_memberships: usize,
    initial_grants: usize,
    commits: usize,
    series: AclSeries,
}

impl AclConfig {
    fn from_env() -> Self {
        let series = match env::var("GROOVE_ACL_SERIES")
            .unwrap_or_else(|_| "insert".to_owned())
            .as_str()
        {
            "delete" => AclSeries::Delete,
            _ => AclSeries::Insert,
        };
        Self {
            seed: env_u64("GROOVE_SEED", 0xac1),
            principals: env_usize("GROOVE_PRINCIPALS", 500),
            groups: env_usize("GROOVE_GROUPS", 100),
            resources: env_usize("GROOVE_RESOURCES", 500),
            subscriptions: env_usize("GROOVE_SUBSCRIPTIONS", 100),
            initial_memberships: env_usize("GROOVE_INITIAL_MEMBERSHIPS", 1_500),
            initial_grants: env_usize("GROOVE_INITIAL_GRANTS", 1_000),
            commits: env_usize("GROOVE_COMMITS", 200),
            series,
        }
    }

    fn as_report_config(self) -> SocialFeedConfig {
        SocialFeedConfig {
            seed: self.seed,
            users: self.principals,
            subscriptions: self.subscriptions,
            initial_follows: self.initial_memberships + self.initial_grants,
            commits: self.commits,
        }
    }
}

#[derive(Clone, Copy)]
enum AclEvent {
    Membership {
        id: u64,
        parent: u64,
        child: u64,
    },
    DeleteMembership {
        id: u64,
    },
    Grant {
        id: u64,
        principal: u64,
        resource: u64,
    },
    DeleteGrant {
        id: u64,
    },
}

struct AclWorkload {
    config: AclConfig,
    rng: Rng,
    next_membership_id: u64,
    next_grant_id: u64,
    live_memberships: Vec<u64>,
    live_grants: Vec<u64>,
    memberships: BTreeMap<u64, (u64, u64)>,
    grants: BTreeMap<u64, (u64, u64)>,
}

impl AclWorkload {
    fn new(config: AclConfig) -> Self {
        Self {
            config,
            rng: Rng::new(config.seed),
            next_membership_id: 1,
            next_grant_id: 1,
            live_memberships: Vec::new(),
            live_grants: Vec::new(),
            memberships: BTreeMap::new(),
            grants: BTreeMap::new(),
        }
    }

    fn seed_events(&mut self) -> Vec<AclEvent> {
        let mut events =
            Vec::with_capacity(self.config.initial_memberships + self.config.initial_grants);
        for _ in 0..self.config.initial_memberships {
            events.push(self.next_membership());
        }
        for _ in 0..self.config.initial_grants {
            events.push(self.next_grant());
        }
        events
    }

    fn next_event(&mut self, step: usize) -> AclEvent {
        match self.config.series {
            AclSeries::Insert => {
                if step.is_multiple_of(3) {
                    self.next_grant()
                } else {
                    self.next_membership()
                }
            }
            AclSeries::Delete => {
                if step.is_multiple_of(3) && !self.live_grants.is_empty() {
                    let id = self.take_live_grant_biased();
                    self.grants.remove(&id);
                    AclEvent::DeleteGrant { id }
                } else if !self.live_memberships.is_empty() {
                    let id = self.take_live_membership_biased();
                    self.memberships.remove(&id);
                    AclEvent::DeleteMembership { id }
                } else {
                    self.next_membership()
                }
            }
        }
    }

    fn next_membership(&mut self) -> AclEvent {
        let id = self.next_membership_id;
        self.next_membership_id += 1;
        self.live_memberships.push(id);
        let child_space = self.config.principals as u64 + self.config.groups as u64;
        let child = zipf_sample(&mut self.rng, child_space);
        let parent =
            self.config.principals as u64 + zipf_sample(&mut self.rng, self.config.groups as u64);
        self.memberships.insert(id, (parent, child));
        AclEvent::Membership { id, parent, child }
    }

    fn next_grant(&mut self) -> AclEvent {
        let id = self.next_grant_id;
        self.next_grant_id += 1;
        self.live_grants.push(id);
        let principal =
            self.config.principals as u64 + zipf_sample(&mut self.rng, self.config.groups as u64);
        let resource = zipf_sample(&mut self.rng, self.config.resources as u64);
        self.grants.insert(id, (principal, resource));
        AclEvent::Grant {
            id,
            principal,
            resource,
        }
    }

    fn take_live_grant_biased(&mut self) -> u64 {
        let reachable = self.subscribed_reachable_groups();
        if !reachable.is_empty()
            && let Some((idx, _)) = self.live_grants.iter().enumerate().find(|(_, id)| {
                self.grants
                    .get(id)
                    .is_some_and(|(principal, _)| reachable.contains(principal))
            })
        {
            return self.live_grants.swap_remove(idx);
        }
        let idx = (self.rng.next() as usize) % self.live_grants.len();
        self.live_grants.swap_remove(idx)
    }

    fn take_live_membership_biased(&mut self) -> u64 {
        let reachable = self.subscribed_reachable_groups();
        if let Some((idx, _)) = self.live_memberships.iter().enumerate().find(|(_, id)| {
            self.memberships.get(id).is_some_and(|(_, child)| {
                *child <= self.config.subscriptions as u64 || reachable.contains(child)
            })
        }) {
            return self.live_memberships.swap_remove(idx);
        }
        let idx = (self.rng.next() as usize) % self.live_memberships.len();
        self.live_memberships.swap_remove(idx)
    }

    fn subscribed_reachable_groups(&self) -> BTreeSet<u64> {
        let mut groups = BTreeSet::new();
        for principal in 1..=self.config.subscriptions as u64 {
            groups.extend(self.reachable_groups(principal));
        }
        groups
    }

    fn reachable_groups(&self, principal: u64) -> BTreeSet<u64> {
        let mut by_child = HashMap::<u64, Vec<u64>>::new();
        for (parent, child) in self.memberships.values().copied() {
            by_child.entry(child).or_default().push(parent);
        }
        let mut seen = BTreeSet::new();
        let mut frontier = vec![principal];
        while let Some(child) = frontier.pop() {
            let Some(parents) = by_child.get(&child) else {
                continue;
            };
            for parent in parents {
                if seen.insert(*parent) {
                    frontier.push(*parent);
                }
            }
        }
        seen
    }

    fn expected_acl_rows(&self, principal: u64) -> BTreeMap<AclRow, i64> {
        let reachable = self.reachable_groups(principal);
        let mut rows = BTreeMap::new();
        for (grant_principal, resource_id) in self.grants.values().copied() {
            if reachable.contains(&grant_principal) {
                *rows
                    .entry(AclRow {
                        principal_id: grant_principal,
                        resource_id,
                    })
                    .or_default() += 1;
            }
        }
        rows
    }
}

fn acl_schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "principals",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("kind", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new("groups", [ColumnSchema::new("id", ColumnType::U64)])
            .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "group_membership",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("parent_id", ColumnType::U64),
                ColumnSchema::new("child_id", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "resources",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("owner_id", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "grants",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("principal_id", ColumnType::U64),
                ColumnSchema::new("resource_id", ColumnType::U64),
                ColumnSchema::new("permission", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ])
}

fn acl_graph(principal: u64) -> GraphBuilder {
    let seed = GraphBuilder::table("group_membership")
        .filter(PredicateExpr::eq("child_id", Value::U64(principal)))
        .project_fields([ProjectField::renamed("parent_id", "principal_id")]);
    let frontier = GraphBuilder::frontier_source(
        "frontier",
        groove::records::RecordDescriptor::new([("principal_id", ColumnType::U64.value_type())]),
    );
    let step = GraphBuilder::join(
        frontier,
        GraphBuilder::table("group_membership"),
        ["principal_id"],
        ["child_id"],
    )
    .project_fields([ProjectField::renamed("right.parent_id", "principal_id")]);
    let closure = GraphBuilder::recursive(seed, step, "frontier", 256);
    GraphBuilder::join(
        closure,
        GraphBuilder::table("grants").project(["principal_id", "resource_id"]),
        ["principal_id"],
        ["principal_id"],
    )
    .project_fields([
        ProjectField::renamed("right.principal_id", "principal_id"),
        ProjectField::renamed("right.resource_id", "resource_id"),
    ])
}

fn acl_prepared_shape_graph() -> GraphBuilder {
    let reach = RecordDescriptor::new([
        ("principal_id", ColumnType::U64.value_type()),
        ("group_id", ColumnType::U64.value_type()),
    ]);
    let params = GraphBuilder::binding_source(
        "acl_params",
        RecordDescriptor::new([("principal_id", ColumnType::U64.value_type())]),
    );
    let seed = GraphBuilder::join(
        params,
        GraphBuilder::table("group_membership"),
        ["principal_id"],
        ["child_id"],
    )
    .project_fields([
        ProjectField::renamed("left.principal_id", "principal_id"),
        ProjectField::renamed("right.parent_id", "group_id"),
    ]);
    let frontier = GraphBuilder::frontier_source("frontier", reach);
    let step = GraphBuilder::join(
        frontier,
        GraphBuilder::table("group_membership"),
        ["group_id"],
        ["child_id"],
    )
    .project_fields([
        ProjectField::renamed("left.principal_id", "principal_id"),
        ProjectField::renamed("right.parent_id", "group_id"),
    ]);
    let closure = GraphBuilder::recursive(seed, step, "frontier", 256);
    GraphBuilder::join(
        closure,
        GraphBuilder::table("grants").project(["principal_id", "resource_id"]),
        ["group_id"],
        ["principal_id"],
    )
    .project_fields([
        ProjectField::renamed("left.principal_id", "principal_id"),
        ProjectField::renamed("right.principal_id", "grant_principal_id"),
        ProjectField::renamed("right.resource_id", "resource_id"),
    ])
}

fn seed_groove_acl(db: &mut Database<RocksDbStorage>, workload: &mut AclWorkload) {
    let mut batch = db.open_batch();
    for id in 1..=workload.config.principals as u64 {
        batch.insert(
            "principals",
            vec![Value::U64(id), Value::String("user".to_owned())],
        );
    }
    for group in 1..=workload.config.groups as u64 {
        let id = workload.config.principals as u64 + group;
        batch.insert(
            "principals",
            vec![Value::U64(id), Value::String("group".to_owned())],
        );
        batch.insert("groups", vec![Value::U64(id)]);
    }
    for id in 1..=workload.config.resources as u64 {
        batch.insert("resources", vec![Value::U64(id), Value::U64(1)]);
    }
    db.commit_batch(batch).expect("seed acl objects");

    let mut batch = db.open_batch();
    for event in workload.seed_events() {
        apply_groove_acl_event(&mut batch, event);
    }
    db.commit_batch(batch).expect("seed acl edges");
}

fn apply_groove_acl_event(batch: &mut groove::db::DatabaseBatch, event: AclEvent) {
    match event {
        AclEvent::Membership { id, parent, child } => batch.insert(
            "group_membership",
            vec![Value::U64(id), Value::U64(parent), Value::U64(child)],
        ),
        AclEvent::DeleteMembership { id } => {
            batch.delete("group_membership", PrimaryKeyValue::U64(id));
        }
        AclEvent::Grant {
            id,
            principal,
            resource,
        } => batch.insert(
            "grants",
            vec![
                Value::U64(id),
                Value::U64(principal),
                Value::U64(resource),
                Value::String("read".to_owned()),
            ],
        ),
        AclEvent::DeleteGrant { id } => {
            batch.delete("grants", PrimaryKeyValue::U64(id));
        }
    }
}

fn setup_sqlite_acl(conn: &Connection) {
    conn.execute_batch(
        "
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        CREATE TABLE principals(id INTEGER PRIMARY KEY, kind TEXT NOT NULL);
        CREATE TABLE groups(id INTEGER PRIMARY KEY);
        CREATE TABLE group_membership(id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL, child_id INTEGER NOT NULL);
        CREATE TABLE resources(id INTEGER PRIMARY KEY, owner_id INTEGER NOT NULL);
        CREATE TABLE grants(id INTEGER PRIMARY KEY, principal_id INTEGER NOT NULL, resource_id INTEGER NOT NULL, permission TEXT NOT NULL);
        CREATE INDEX membership_by_child ON group_membership(child_id);
        CREATE INDEX membership_by_parent ON group_membership(parent_id);
        CREATE INDEX grants_by_principal ON grants(principal_id);
        ",
    )
    .expect("setup acl sqlite");
}

fn seed_sqlite_acl(conn: &mut Connection, workload: &mut AclWorkload) {
    let tx = conn.transaction().expect("tx");
    for id in 1..=workload.config.principals as u64 {
        tx.execute(
            "INSERT INTO principals(id, kind) VALUES (?1, 'user')",
            params![id],
        )
        .expect("insert principal");
    }
    for group in 1..=workload.config.groups as u64 {
        let id = workload.config.principals as u64 + group;
        tx.execute(
            "INSERT INTO principals(id, kind) VALUES (?1, 'group')",
            params![id],
        )
        .expect("insert group principal");
        tx.execute("INSERT INTO groups(id) VALUES (?1)", params![id])
            .expect("insert group");
    }
    for id in 1..=workload.config.resources as u64 {
        tx.execute(
            "INSERT INTO resources(id, owner_id) VALUES (?1, 1)",
            params![id],
        )
        .expect("insert resource");
    }
    tx.commit().expect("commit acl objects");

    let tx = conn.transaction().expect("tx");
    for event in workload.seed_events() {
        apply_sqlite_acl_event_in_tx(&tx, event);
    }
    tx.commit().expect("commit acl edges");
}

fn apply_sqlite_acl_event(conn: &mut Connection, event: AclEvent) {
    let tx = conn.transaction().expect("tx");
    apply_sqlite_acl_event_in_tx(&tx, event);
    tx.commit().expect("commit acl event");
}

fn apply_sqlite_acl_event_in_tx(tx: &rusqlite::Transaction<'_>, event: AclEvent) {
    match event {
        AclEvent::Membership { id, parent, child } => {
            tx.execute(
                "INSERT INTO group_membership(id, parent_id, child_id) VALUES (?1, ?2, ?3)",
                params![id, parent, child],
            )
            .expect("insert membership");
        }
        AclEvent::DeleteMembership { id } => {
            tx.execute("DELETE FROM group_membership WHERE id = ?1", params![id])
                .expect("delete membership");
        }
        AclEvent::Grant {
            id,
            principal,
            resource,
        } => {
            tx.execute(
                "INSERT INTO grants(id, principal_id, resource_id, permission) VALUES (?1, ?2, ?3, 'read')",
                params![id, principal, resource],
            )
            .expect("insert grant");
        }
        AclEvent::DeleteGrant { id } => {
            tx.execute("DELETE FROM grants WHERE id = ?1", params![id])
                .expect("delete grant");
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct AclRow {
    principal_id: u64,
    resource_id: u64,
}

fn acl_row_from_values(values: Vec<Value>) -> AclRow {
    match values.as_slice() {
        [Value::U64(principal_id), Value::U64(resource_id)] => AclRow {
            principal_id: *principal_id,
            resource_id: *resource_id,
        },
        _ => panic!("unexpected acl row shape: {values:?}"),
    }
}

fn param_acl_row_from_values(values: Vec<Value>) -> AclRow {
    match values.as_slice() {
        [
            Value::U64(_principal_id),
            Value::U64(grant_principal_id),
            Value::U64(resource_id),
        ] => AclRow {
            principal_id: *grant_principal_id,
            resource_id: *resource_id,
        },
        _ => panic!("unexpected param acl row shape: {values:?}"),
    }
}

fn apply_acl_notification(cache: &mut BTreeMap<AclRow, i64>, notification: RecordDeltas) {
    for (values, weight) in notification.to_values().expect("acl notification values") {
        let row = acl_row_from_values(values);
        apply_weight(cache, row, weight);
    }
}

fn apply_param_acl_notification(cache: &mut BTreeMap<AclRow, i64>, notification: RecordDeltas) {
    for (values, weight) in notification.to_values().expect("acl notification values") {
        let row = param_acl_row_from_values(values);
        apply_weight(cache, row, weight);
    }
}

fn drain_acl_subscription(
    cache: &mut BTreeMap<AclRow, i64>,
    subscription: &Subscription,
    metrics: &mut ScenarioMetrics,
) {
    while let Ok(notification) = subscription.try_recv() {
        metrics.notifications += 1;
        metrics.notification_records += notification.deltas.len();
        apply_acl_notification(cache, notification);
    }
}

fn drain_param_acl_subscription(
    cache: &mut BTreeMap<AclRow, i64>,
    subscription: &Subscription,
    metrics: &mut ScenarioMetrics,
) {
    while let Ok(notification) = subscription.try_recv() {
        metrics.notifications += 1;
        metrics.notification_records += notification.deltas.len();
        apply_param_acl_notification(cache, notification);
    }
}

fn sqlite_acl_rows(conn: &Connection, principal: u64) -> BTreeMap<AclRow, i64> {
    let mut stmt = conn
        .prepare(
            "
            WITH RECURSIVE reachable(principal_id) AS (
                SELECT parent_id FROM group_membership WHERE child_id = ?1
                UNION
                SELECT gm.parent_id
                FROM group_membership gm
                JOIN reachable r ON gm.child_id = r.principal_id
            )
            SELECT g.principal_id, g.resource_id
            FROM reachable r
            JOIN grants g ON g.principal_id = r.principal_id
            ",
        )
        .expect("prepare acl");
    let mut rows = BTreeMap::new();
    let iter = stmt
        .query_map(params![principal], |row| {
            Ok(AclRow {
                principal_id: row.get::<_, u64>(0)?,
                resource_id: row.get::<_, u64>(1)?,
            })
        })
        .expect("query acl");
    for row in iter {
        *rows.entry(row.expect("acl row")).or_default() += 1;
    }
    rows
}

fn diff_acl_row_count(previous: &BTreeMap<AclRow, i64>, next: &BTreeMap<AclRow, i64>) -> usize {
    let keys = previous
        .keys()
        .chain(next.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    keys.into_iter()
        .filter(|key| previous.get(key) != next.get(key))
        .count()
}

fn run_oneshot_query() {
    let config = OneshotConfig::from_env();
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let storage = RocksDbStorage::open_with_durability(
        temp_dir.path(),
        &["users", "posts", "follows", "likes", "indices"],
        Durability::WalNoSync,
    )
    .expect("open rocksdb");
    let mut db = Database::new(social_feed_schema(), storage).expect("database");
    seed_oneshot_posts(&mut db, config.rows, config.authors);

    let mut metrics = ScenarioMetrics::default();
    for i in 0..config.queries {
        let author = (i as u64 % config.authors as u64) + 1;
        let start = Instant::now();
        let result = db.query(post_query(author)).expect("query");
        metrics.record_commit(start.elapsed());
        metrics.notification_records += result.deltas.len();
    }
    let stats = db
        .last_tick_metrics()
        .map(|tick| tick.runtime_stats.clone())
        .unwrap_or_default();
    print_json(JsonReport {
        scenario: "oneshot",
        engine: "groove_query",
        durability: "wal_no_sync",
        config: config.as_report_config(),
        subscribe_elapsed: Duration::default(),
        metrics,
        graph_nodes: stats.graph_nodes,
        arrangements: stats.arrangement_count,
        arrangement_rows: stats.arrangement_rows,
        arrangement_bytes: stats.arrangement_encoded_bytes,
        logical_nodes_requested: stats.logical_nodes_requested,
        deduped_graph_nodes: stats.deduped_graph_nodes,
        dedupe_ratio: stats.dedupe_ratio(),
        result_cache_rows: 0,
    });
}

fn run_oneshot_subscribe() {
    let config = OneshotConfig::from_env();
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let storage = RocksDbStorage::open_with_durability(
        temp_dir.path(),
        &["users", "posts", "follows", "likes", "indices"],
        Durability::WalNoSync,
    )
    .expect("open rocksdb");
    let mut db = Database::new(social_feed_schema(), storage).expect("database");
    seed_oneshot_posts(&mut db, config.rows, config.authors);

    let mut metrics = ScenarioMetrics::default();
    for i in 0..config.queries {
        let author = (i as u64 % config.authors as u64) + 1;
        let start = Instant::now();
        let subscription = db
            .subscribe_one_sink(post_graph(author))
            .expect("subscribe");
        let result = subscription.recv().expect("initial");
        db.unsubscribe(subscription.id());
        metrics.record_commit(start.elapsed());
        metrics.notification_records += result.deltas.len();
    }
    let stats = db
        .last_tick_metrics()
        .map(|tick| tick.runtime_stats.clone())
        .unwrap_or_default();
    print_json(JsonReport {
        scenario: "oneshot",
        engine: "groove_subscribe",
        durability: "wal_no_sync",
        config: config.as_report_config(),
        subscribe_elapsed: Duration::default(),
        metrics,
        graph_nodes: stats.graph_nodes,
        arrangements: stats.arrangement_count,
        arrangement_rows: stats.arrangement_rows,
        arrangement_bytes: stats.arrangement_encoded_bytes,
        logical_nodes_requested: stats.logical_nodes_requested,
        deduped_graph_nodes: stats.deduped_graph_nodes,
        dedupe_ratio: stats.dedupe_ratio(),
        result_cache_rows: 0,
    });
}

fn run_oneshot_scan() {
    let config = OneshotConfig::from_env();
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let storage =
        RocksDbStorage::open_with_durability(temp_dir.path(), &["posts"], Durability::WalNoSync)
            .expect("open rocksdb");
    let descriptor = posts_descriptor();
    for id in 1..=config.rows as u64 {
        let author = (id % config.authors as u64) + 1;
        let record = descriptor
            .create(&[
                Value::U64(id),
                Value::U64(author),
                Value::U64(id),
                Value::String("public".to_owned()),
            ])
            .expect("record");
        storage
            .set("posts", &u64_key(id), &record)
            .expect("set post");
    }

    let mut metrics = ScenarioMetrics::default();
    for i in 0..config.queries {
        let author = (i as u64 % config.authors as u64) + 1;
        let start = Instant::now();
        let mut rows = 0usize;
        storage
            .scan_prefix("posts", b"", &mut |_, value| {
                if descriptor.get(value, "author_id").ok() == Some(Value::U64(author)) {
                    rows += 1;
                }
                Ok(())
            })
            .expect("scan posts");
        metrics.record_commit(start.elapsed());
        metrics.notification_records += rows;
    }
    print_json(JsonReport {
        scenario: "oneshot",
        engine: "groove_scan",
        durability: "wal_no_sync",
        config: config.as_report_config(),
        subscribe_elapsed: Duration::default(),
        metrics,
        graph_nodes: 0,
        arrangements: 0,
        arrangement_rows: 0,
        arrangement_bytes: 0,
        logical_nodes_requested: 0,
        deduped_graph_nodes: 0,
        dedupe_ratio: 1.0,
        result_cache_rows: 0,
    });
}

#[derive(Clone, Copy)]
struct OneshotConfig {
    seed: u64,
    rows: usize,
    authors: usize,
    queries: usize,
}

impl OneshotConfig {
    fn from_env() -> Self {
        Self {
            seed: env_u64("GROOVE_SEED", 0xd),
            rows: env_usize("GROOVE_ROWS", 10_000),
            authors: env_usize("GROOVE_AUTHORS", 1_000),
            queries: env_usize("GROOVE_QUERIES", 1_000),
        }
    }

    fn as_report_config(self) -> SocialFeedConfig {
        SocialFeedConfig {
            seed: self.seed,
            users: self.rows,
            subscriptions: self.authors,
            initial_follows: 0,
            commits: self.queries,
        }
    }
}

fn post_query(author: u64) -> Query {
    Query::Select(Box::new(
        Select::new([
            SelectItem::expr(Expr::column("author_id")),
            SelectItem::expr(Expr::column("id")),
            SelectItem::expr(Expr::column("created_at")),
        ])
        .from([TableRef::named("posts")])
        .where_(Expr::binary(
            Expr::column("author_id"),
            BinaryOp::Eq,
            Expr::Literal(Value::U64(author)),
        )),
    ))
}

fn post_graph(author: u64) -> GraphBuilder {
    GraphBuilder::table("posts")
        .filter(PredicateExpr::eq("author_id", Value::U64(author)))
        .project(["author_id", "id", "created_at"])
}

fn seed_oneshot_posts(db: &mut Database<RocksDbStorage>, rows: usize, authors: usize) {
    let mut batch = db.open_batch();
    for id in 1..=rows as u64 {
        let author = (id % authors as u64) + 1;
        batch.insert(
            "posts",
            vec![
                Value::U64(id),
                Value::U64(author),
                Value::U64(id),
                Value::String("public".to_owned()),
            ],
        );
    }
    db.commit_batch(batch).expect("seed posts");
}

fn posts_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([
        ("id", ColumnType::U64.value_type()),
        ("author_id", ColumnType::U64.value_type()),
        ("created_at", ColumnType::U64.value_type()),
        ("visibility", ColumnType::String.value_type()),
    ])
}

fn u64_key(value: u64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

struct ScenarioMetrics {
    commit_hist: Histogram<u64>,
    storage_hist: Histogram<u64>,
    tick_hist: Histogram<u64>,
    notifications: usize,
    notification_records: usize,
    engine_records_processed: usize,
}

impl Default for ScenarioMetrics {
    fn default() -> Self {
        Self {
            commit_hist: Histogram::new(3).expect("hist"),
            storage_hist: Histogram::new(3).expect("hist"),
            tick_hist: Histogram::new(3).expect("hist"),
            notifications: 0,
            notification_records: 0,
            engine_records_processed: 0,
        }
    }
}

impl ScenarioMetrics {
    fn record_commit(&mut self, duration: Duration) {
        self.commit_hist
            .record(duration_micros(duration))
            .expect("record");
    }

    fn record_storage(&mut self, duration: Duration) {
        self.storage_hist
            .record(duration_micros(duration))
            .expect("record");
    }

    fn record_tick(&mut self, duration: Duration) {
        self.tick_hist
            .record(duration_micros(duration))
            .expect("record");
    }
}

struct JsonReport<'a> {
    scenario: &'a str,
    engine: &'a str,
    durability: &'a str,
    config: SocialFeedConfig,
    subscribe_elapsed: Duration,
    metrics: ScenarioMetrics,
    graph_nodes: usize,
    arrangements: usize,
    arrangement_rows: usize,
    arrangement_bytes: usize,
    logical_nodes_requested: u64,
    deduped_graph_nodes: usize,
    dedupe_ratio: f64,
    result_cache_rows: usize,
}

fn print_json(report: JsonReport<'_>) {
    println!(
        concat!(
            "{{",
            "\"scenario\":\"{}\",",
            "\"engine\":\"{}\",",
            "\"durability\":\"{}\",",
            "\"seed\":{},",
            "\"users\":{},",
            "\"subscriptions\":{},",
            "\"commits\":{},",
            "\"subscribe_us\":{},",
            "\"commit_us\":{},",
            "\"storage_us\":{},",
            "\"tick_us\":{},",
            "\"notifications\":{},",
            "\"notification_records\":{},",
            "\"engine_records_processed\":{},",
            "\"graph_nodes\":{},",
            "\"arrangements\":{},",
            "\"arrangement_rows\":{},",
            "\"arrangement_bytes\":{},",
            "\"logical_nodes_requested\":{},",
            "\"deduped_graph_nodes\":{},",
            "\"dedupe_ratio\":{},",
            "\"result_cache_rows\":{}",
            "}}"
        ),
        report.scenario,
        report.engine,
        report.durability,
        report.config.seed,
        report.config.users,
        report.config.subscriptions,
        report.config.commits,
        duration_micros(report.subscribe_elapsed),
        histogram_json(&report.metrics.commit_hist),
        histogram_json(&report.metrics.storage_hist),
        histogram_json(&report.metrics.tick_hist),
        report.metrics.notifications,
        report.metrics.notification_records,
        report.metrics.engine_records_processed,
        report.graph_nodes,
        report.arrangements,
        report.arrangement_rows,
        report.arrangement_bytes,
        report.logical_nodes_requested,
        report.deduped_graph_nodes,
        report.dedupe_ratio,
        report.result_cache_rows,
    );
}

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(2862933555777941757)
            .wrapping_add(3037000493);
        self.0
    }

    fn next_unit_f64(&mut self) -> f64 {
        let bits = self.next() >> 11;
        (bits as f64) / ((1u64 << 53) as f64)
    }
}

fn zipf_sample(rng: &mut Rng, max: u64) -> u64 {
    let harmonic = (1..=max).map(|rank| 1.0 / rank as f64).sum::<f64>();
    let mut target = rng.next_unit_f64() * harmonic;
    for rank in 1..=max {
        target -= 1.0 / rank as f64;
        if target <= 0.0 {
            return rank;
        }
    }
    max
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn duration_micros(duration: Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}

fn histogram_json(histogram: &Histogram<u64>) -> String {
    format!(
        "{{\"p50\":{},\"p95\":{},\"p99\":{},\"max\":{}}}",
        histogram.value_at_quantile(0.50),
        histogram.value_at_quantile(0.95),
        histogram.value_at_quantile(0.99),
        histogram.max()
    )
}
