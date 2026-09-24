//! Pull-executor ceiling experiment: hand-written index-probe plans versus the
//! Groove snapshot and prepared-bind paths for the same one-shot reads.
//!
//! Question under test: if Jazz replaced incremental view maintenance with
//! plain one-shot queries re-run on invalidation, how much cheaper could each
//! query be than Groove's current one-shot machinery? The `pull_*` engines are
//! a speed-of-light lower bound for a lean pull executor: they use only
//! primary-key prefix scans (index nested-loop joins) and decode rows once,
//! with no arrangements, memo, or delta bookkeeping. They are not a general
//! executor.
//!
//! Workload: a social feed. `posts` is keyed `(author, id)` and `follows` is
//! keyed `(follower, followee)`, so every per-user access is a key-prefix scan
//! for every engine. Each benchmark iteration reads for the next user in a
//! cycle, so repeated reads model many similar users rather than one hot key.
//!
//! - `author_posts`: posts of one author (20 rows). A single-source plan; the
//!   difference is fixed per-query overhead.
//! - `feed`: `follows(follower = u) ⋈ posts ON followee = author` (600 rows).
//!   Groove hydrates `posts` fully as the join's right input; the pull plan
//!   probes `posts` once per followee.
//! - `feed_top20`: the feed ordered by `created DESC`, limit 20.
//!
//! Engines:
//! - `snapshot`: `Database::query_graph` with the user literal in the scan.
//! - `prepared_warm`: one prepared shape with a binding source; each read binds,
//!   drives progress, takes the first delta, and unsubscribes (the one-shot =
//!   degenerate subscription path). The shape stays registered, so arrangements
//!   shared across bindings stay warm; this is Groove's own form of caching.
//! - `prepared_cold`: prepare, first bind, and retire in one iteration; the
//!   cost a cold node pays before any shared arrangement exists.
//! - `pull`: hand-written index-probe plan.
//!
//! Each engine runs at two table sizes (`users` = 500 and 5,000; posts are
//! 20 per user, follows 30 per user) while per-user result sizes stay fixed,
//! so scale dependence shows directly.
//!
//! Before measuring, every engine's output is checked for multiset equality
//! against the others for a sample of users (benchmark validity, INV-PERF-2).
//!
//! ```text
//! cargo bench -p groove --bench pull_vs_snapshot
//! ```

use std::cell::Cell;
use std::collections::BTreeMap;

use futures::executor::block_on;
use groove::db::{Database, GraphBuilder, ProjectField, Subscription};
use groove::ivm::{PreparedShape, StaticScanSpec, TopByLimit, TopByOrder};
use groove::records::{RecordDescriptor, Value};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, PrimaryKeyColumn,
    TableSchema,
};
use groove::storage::MemoryStorage;

/// Table sizes: posts = users * 20, follows = users * 30. Per-user result
/// sizes are fixed, so a scale-independent engine stays flat across sizes.
const USER_COUNTS: [u64; 2] = [500, 5_000];
const VERIFY_USERS: u64 = 500;
const POSTS_PER_USER: u64 = 20;
const FOLLOWS_PER_USER: u64 = 30;
const TOP_K: usize = 20;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    verify_engines_agree();
    divan::main();
}

type Rows = Vec<Vec<Value>>;

fn schema() -> DatabaseSchema {
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
    ])
}

fn followee(users: u64, user: u64, slot: u64) -> u64 {
    // Deterministic, distinct followees per user, never the user itself.
    (user + 1 + slot * 7) % users
}

fn created(users: u64, author: u64, post: u64) -> u64 {
    // Interleave authors in time so a feed's top-k spans many followees.
    post * users + (author * 7919) % users
}

fn seeded_database(users: u64) -> Database {
    block_on(async {
        let storage =
            MemoryStorage::new(&["posts", "follows", "indices"]).expect("valid memory storage");
        let mut database = Database::new(schema(), storage).await.expect("database");
        let mut batch = database.open_batch();
        for author in 0..users {
            for post in 0..POSTS_PER_USER {
                batch.insert(
                    "posts",
                    vec![
                        Value::U64(author),
                        Value::U64(author * POSTS_PER_USER + post),
                        Value::U64(created(users, author, post)),
                        Value::String(format!("post {post} by {author}")),
                    ],
                );
            }
            for slot in 0..FOLLOWS_PER_USER {
                batch.insert(
                    "follows",
                    vec![
                        Value::U64(author),
                        Value::U64(followee(users, author, slot)),
                    ],
                );
            }
        }
        let applied = database.apply_batch(batch).await.expect("seed");
        let persisted = applied.persist().await;
        database
            .finish_persistence(persisted)
            .expect("persist seed");
        database
    })
}

fn user_prefix(user: u64) -> StaticScanSpec {
    StaticScanSpec::Prefix(vec![Value::U64(user).into()])
}

/// Output of every engine: `(author, id, created)` of each post.
fn post_fields(prefix: &str) -> [ProjectField; 3] {
    [
        ProjectField::renamed(format!("{prefix}author"), "author"),
        ProjectField::renamed(format!("{prefix}id"), "id"),
        ProjectField::renamed(format!("{prefix}created"), "created"),
    ]
}

fn feed_join(follows: GraphBuilder) -> GraphBuilder {
    GraphBuilder::join(
        follows,
        GraphBuilder::table("posts"),
        ["followee"],
        ["author"],
    )
    .project_fields(post_fields("right."))
}

fn top20(input: GraphBuilder) -> GraphBuilder {
    GraphBuilder::top_by(
        input,
        Vec::<String>::new(),
        [TopByOrder::desc("created")],
        ["author", "id"],
        0,
        TopByLimit::Finite(TOP_K as u64),
    )
}

#[derive(Clone, Copy, Debug)]
enum Scenario {
    AuthorPosts,
    Feed,
    FeedTop20,
}

impl Scenario {
    const ALL: [Self; 3] = [Self::AuthorPosts, Self::Feed, Self::FeedTop20];

    fn snapshot_graph(self, user: u64) -> GraphBuilder {
        match self {
            Self::AuthorPosts => {
                GraphBuilder::table_scan("posts", user_prefix(user)).project_fields(post_fields(""))
            }
            Self::Feed => feed_join(GraphBuilder::table_scan("follows", user_prefix(user))),
            Self::FeedTop20 => top20(feed_join(GraphBuilder::table_scan(
                "follows",
                user_prefix(user),
            ))),
        }
    }

    fn prepared_graph(self) -> GraphBuilder {
        let user = GraphBuilder::binding_source(
            "user",
            RecordDescriptor::new([("user", ColumnType::U64.clone())]),
        );
        let keyed = |fields: [ProjectField; 3]| {
            let mut all = vec![ProjectField::renamed("left.user", "user")];
            all.extend(fields);
            all
        };
        match self {
            Self::AuthorPosts => {
                GraphBuilder::join(user, GraphBuilder::table("posts"), ["user"], ["author"])
                    .project_fields(keyed(post_fields("right.")))
            }
            Self::Feed | Self::FeedTop20 => {
                let follows = GraphBuilder::join(
                    user,
                    GraphBuilder::table("follows"),
                    ["user"],
                    ["follower"],
                )
                .project_fields([
                    ProjectField::renamed("left.user", "user"),
                    ProjectField::renamed("right.followee", "followee"),
                ]);
                let feed = GraphBuilder::join(
                    follows,
                    GraphBuilder::table("posts"),
                    ["followee"],
                    ["author"],
                )
                .project_fields(keyed(post_fields("right.")));
                match self {
                    Self::FeedTop20 => GraphBuilder::top_by(
                        feed,
                        ["user"],
                        [TopByOrder::desc("created")],
                        ["author", "id"],
                        0,
                        TopByLimit::Finite(TOP_K as u64),
                    ),
                    _ => feed,
                }
            }
        }
    }
}

fn snapshot_read(database: &mut Database, scenario: Scenario, user: u64) -> Rows {
    let deltas = block_on(database.query_graph(scenario.snapshot_graph(user))).expect("snapshot");
    expand(deltas.to_values().expect("decode snapshot"))
}

fn prepare(database: &mut Database, scenario: Scenario) -> PreparedShape {
    block_on(database.prepare_one_sink(
        scenario.prepared_graph(),
        "user",
        RecordDescriptor::new([("user", ColumnType::U64.clone())]),
        ["user"],
    ))
    .expect("prepare")
}

fn prepared_read(database: &mut Database, shape: &PreparedShape, user: u64) -> Rows {
    let subscription: Subscription =
        block_on(database.bind_shape_one_sink(shape.id(), &[Value::U64(user)])).expect("bind");
    block_on(database.drive_progress()).expect("drive");
    let first = subscription.recv().expect("initial hydration");
    assert!(database.unsubscribe(subscription.id()));
    // Drop the hidden routing `user` field so outputs compare across engines.
    expand(first.to_values().expect("decode prepared"))
        .into_iter()
        .map(|mut row| {
            row.remove(0);
            row
        })
        .collect()
}

fn pull_posts_of(database: &Database, author: u64, out: &mut Rows) {
    let rows = block_on(database.primary_key_scan("posts", &[Value::U64(author)])).expect("posts");
    for row in rows {
        let record = row.record();
        out.push(vec![
            record.get_idx(0).expect("author"),
            record.get_idx(1).expect("id"),
            record.get_idx(2).expect("created"),
        ]);
    }
}

fn pull_read(database: &Database, scenario: Scenario, user: u64) -> Rows {
    let mut out = Vec::new();
    match scenario {
        Scenario::AuthorPosts => pull_posts_of(database, user, &mut out),
        Scenario::Feed | Scenario::FeedTop20 => {
            let follows = block_on(database.primary_key_scan("follows", &[Value::U64(user)]))
                .expect("follows");
            for follow in follows {
                let Value::U64(followee) = follow.record().get_idx(1).expect("followee") else {
                    panic!("followee is u64");
                };
                pull_posts_of(database, followee, &mut out);
            }
            if matches!(scenario, Scenario::FeedTop20) {
                let key = |row: &Vec<Value>| match (&row[2], &row[0], &row[1]) {
                    (Value::U64(created), Value::U64(author), Value::U64(id)) => {
                        (std::cmp::Reverse(*created), *author, *id)
                    }
                    _ => panic!("u64 fields"),
                };
                if out.len() > TOP_K {
                    out.select_nth_unstable_by_key(TOP_K - 1, key);
                    out.truncate(TOP_K);
                }
                out.sort_unstable_by_key(key);
            }
        }
    }
    out
}

fn expand(weighted: Vec<(Vec<Value>, i64)>) -> Rows {
    let mut rows = Vec::new();
    for (row, weight) in weighted {
        assert!(weight > 0, "snapshot rows carry positive weight");
        for _ in 0..weight {
            rows.push(row.clone());
        }
    }
    rows
}

/// `Value` has no total order; its debug form is unambiguous for these u64
/// fields and suffices as a multiset key.
fn multiset(rows: Rows) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for row in rows {
        *counts.entry(format!("{row:?}")).or_default() += 1;
    }
    counts
}

fn verify_engines_agree() {
    let users = VERIFY_USERS;
    let mut database = seeded_database(users);
    for scenario in Scenario::ALL {
        let shape = prepare(&mut database, scenario);
        for user in [0, 1, users / 2, users - 1] {
            let pull = pull_read(&database, scenario, user);
            let expected_len = match scenario {
                Scenario::AuthorPosts => POSTS_PER_USER as usize,
                Scenario::Feed => (FOLLOWS_PER_USER * POSTS_PER_USER) as usize,
                Scenario::FeedTop20 => TOP_K,
            };
            assert_eq!(pull.len(), expected_len, "{scenario:?} user {user}");
            let pull = multiset(pull);
            let snapshot = multiset(snapshot_read(&mut database, scenario, user));
            let prepared = multiset(prepared_read(&mut database, &shape, user));
            assert_eq!(snapshot, pull, "{scenario:?} snapshot vs pull, user {user}");
            assert_eq!(prepared, pull, "{scenario:?} prepared vs pull, user {user}");
        }
    }
}

fn next_user(cursor: &Cell<u64>, users: u64) -> u64 {
    let user = cursor.get();
    cursor.set((user + 37) % users);
    user
}

fn bench_snapshot(bencher: divan::Bencher, scenario: Scenario, users: u64) {
    let mut database = seeded_database(users);
    let cursor = Cell::new(0);
    bencher.bench_local(|| snapshot_read(&mut database, scenario, next_user(&cursor, users)));
}

fn bench_prepared(bencher: divan::Bencher, scenario: Scenario, users: u64) {
    let mut database = seeded_database(users);
    let shape = prepare(&mut database, scenario);
    let cursor = Cell::new(0);
    // Warm the shared arrangements once, as a long-lived server would be.
    prepared_read(&mut database, &shape, next_user(&cursor, users));
    bencher.bench_local(|| prepared_read(&mut database, &shape, next_user(&cursor, users)));
}

/// First read of a freshly prepared shape: what a cold server or client pays
/// before shared arrangements exist. Prepare and retire are inside the timing.
fn bench_prepared_cold(bencher: divan::Bencher, scenario: Scenario, users: u64) {
    let mut database = seeded_database(users);
    let cursor = Cell::new(0);
    bencher.bench_local(|| {
        let shape = prepare(&mut database, scenario);
        let rows = prepared_read(&mut database, &shape, next_user(&cursor, users));
        database.retire_prepared_shape(shape.id()).expect("retire");
        rows
    });
}

fn bench_pull(bencher: divan::Bencher, scenario: Scenario, users: u64) {
    let database = seeded_database(users);
    let cursor = Cell::new(0);
    bencher.bench_local(|| pull_read(&database, scenario, next_user(&cursor, users)));
}

macro_rules! scenario_benches {
    ($module:ident, $scenario:expr) => {
        mod $module {
            use super::*;

            #[divan::bench(args = USER_COUNTS)]
            fn snapshot(bencher: divan::Bencher, users: u64) {
                bench_snapshot(bencher, $scenario, users);
            }

            #[divan::bench(args = USER_COUNTS)]
            fn prepared_warm(bencher: divan::Bencher, users: u64) {
                bench_prepared(bencher, $scenario, users);
            }

            #[divan::bench(args = USER_COUNTS)]
            fn prepared_cold(bencher: divan::Bencher, users: u64) {
                bench_prepared_cold(bencher, $scenario, users);
            }

            #[divan::bench(args = USER_COUNTS)]
            fn pull(bencher: divan::Bencher, users: u64) {
                bench_pull(bencher, $scenario, users);
            }
        }
    };
}

scenario_benches!(author_posts, Scenario::AuthorPosts);
scenario_benches!(feed, Scenario::Feed);
scenario_benches!(feed_top20, Scenario::FeedTop20);
