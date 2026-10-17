//! Stored arrangements: an arrangement of a stored table, seen through
//! key-preserving operators and keyed on its primary key, is read from
//! storage by the keys a tick touches instead of being held.
//!
//! Every maintained case runs on databases fed the same edits, one without
//! stored arrangements and others with them under several memory budgets:
//! unbounded, zero (every bucket read from storage), and small enough to
//! evict buckets between edits. Their results must agree after every edit,
//! with each other and with one-shot queries.

use std::collections::BTreeMap;
use std::sync::mpsc::TryRecvError;

use groove::db::{Database, GraphBuilder, PredicateExpr, PrimaryKeyValue, ProjectField};
use groove::ivm::{AggregateExpr, AggregateFunction, LiteralValue, StaticScanSpec};
use groove::records::RecordDescriptor;
use groove::records::Value;
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, DictionarySchema, IntegerKeyType, PrimaryKey,
    PrimaryKeyColumn, PrimaryKeyType, TableSchema,
};
use groove::storage::MemoryStorage;

fn schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "docs",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("project", ColumnType::U64),
                ColumnSchema::new("bucket", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64).user_supplied()),
        TableSchema::new(
            "projects",
            [
                ColumnSchema::new("branch", ColumnType::U64),
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("visible", ColumnType::U64),
                ColumnSchema::new("name", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::composite([
            PrimaryKeyColumn::integer("branch", IntegerKeyType::U64),
            PrimaryKeyColumn::integer("id", IntegerKeyType::U64),
        ])),
        TableSchema::new(
            "versions",
            [
                ColumnSchema::new("branch", ColumnType::U64),
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("version", ColumnType::U64),
                ColumnSchema::new("visible", ColumnType::U64),
                ColumnSchema::new("name", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::composite([
            PrimaryKeyColumn::integer("branch", IntegerKeyType::U64),
            PrimaryKeyColumn::integer("id", IntegerKeyType::U64),
            PrimaryKeyColumn::integer("version", IntegerKeyType::U64),
        ])),
        TableSchema::new(
            "edges",
            [
                ColumnSchema::new("src", ColumnType::U64),
                ColumnSchema::new("dst", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::composite([
            PrimaryKeyColumn::integer("src", IntegerKeyType::U64),
            PrimaryKeyColumn::integer("dst", IntegerKeyType::U64),
        ])),
    ])
}

async fn open_db(stored: bool) -> Database {
    let storage =
        MemoryStorage::new(&["docs", "projects", "versions", "edges"]).expect("valid families");
    let mut db = Database::new(schema(), storage).await.unwrap();
    db.set_stored_arrangements_enabled(stored);
    db
}

async fn open_budgeted(budget: Option<usize>) -> Database {
    let mut db = open_db(true).await;
    db.set_cache_memory_budget(budget);
    db
}

/// Arrangement budgets every maintained case runs under: unbounded, none
/// resident, and a few buckets resident.
const BUDGETS: [Option<usize>; 3] = [None, Some(0), Some(400)];

fn branch(table: &str) -> GraphBuilder {
    GraphBuilder::table_scan(
        table,
        StaticScanSpec::Prefix(vec![LiteralValue::from(Value::U64(1))]),
    )
}

/// The visible current projects of branch 1: a union of a plain layer and
/// a latest-version layer, filtered and projected, as a stored source
/// usually reaches a join.
fn project_source() -> GraphBuilder {
    GraphBuilder::union([
        branch("projects").project(["id", "visible", "name"]),
        GraphBuilder::arg_max_by(branch("versions"), ["branch", "id"], ["version"])
            .project(["id", "visible", "name"]),
    ])
    .filter(PredicateExpr::gt("visible", Value::U64(0)))
    .project(["id", "name"])
}

/// The docs in bucket 1 among docs 1 to 9, read by id: a narrow key
/// relation, as a selective read of a subscription usually is.
fn bucket_docs() -> GraphBuilder {
    let ids = RecordDescriptor::new([("id", ColumnType::U64)]);
    let wanted = GraphBuilder::inline_records(
        ids,
        (1..10).map(|id| ids.create(&[Value::U64(id)]).unwrap()),
    );
    GraphBuilder::lookup(GraphBuilder::table("docs"), wanted, ["id"])
        .expect("docs is a table source")
        .filter(PredicateExpr::eq("bucket", Value::U64(1)))
}

fn doc(id: u64, project: u64, bucket: u64) -> Vec<Value> {
    vec![Value::U64(id), Value::U64(project), Value::U64(bucket)]
}

fn project(id: u64, visible: u64, name: &str) -> Vec<Value> {
    vec![
        Value::U64(1),
        Value::U64(id),
        Value::U64(visible),
        Value::String(name.into()),
    ]
}

fn version(id: u64, version: u64, name: &str) -> Vec<Value> {
    vec![
        Value::U64(1),
        Value::U64(id),
        Value::U64(version),
        Value::U64(1),
        Value::String(name.into()),
    ]
}

fn project_key(id: u64) -> PrimaryKeyValue {
    PrimaryKeyValue::Composite(vec![PrimaryKeyValue::U64(1), PrimaryKeyValue::U64(id)])
}

type Bag = BTreeMap<String, i64>;

type Edit = Box<dyn FnOnce(&mut groove::db::DatabaseBatch)>;

fn add(bag: &mut Bag, rows: Vec<(Vec<Value>, i64)>) {
    for (values, weight) in rows {
        *bag.entry(format!("{values:?}")).or_default() += weight;
    }
    bag.retain(|_, weight| *weight != 0);
}

async fn snapshot(db: &mut Database, graph: GraphBuilder) -> Bag {
    let mut bag = Bag::new();
    add(
        &mut bag,
        db.query_graph(graph).await.unwrap().to_values().unwrap(),
    );
    bag
}

async fn apply(db: &mut Database, edit: impl FnOnce(&mut groove::db::DatabaseBatch)) {
    let mut batch = db.open_batch();
    edit(&mut batch);
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();
}

async fn seed(db: &mut Database) {
    apply(db, |b| {
        b.insert("docs", doc(1, 10, 1));
        b.insert("docs", doc(2, 10, 1));
        b.insert("docs", doc(3, 20, 1));
        b.insert("docs", doc(4, 30, 9));
        b.insert("projects", project(10, 1, "ten"));
        b.insert("projects", project(30, 1, "thirty"));
        b.insert("projects", project(40, 1, "forty"));
        b.insert("versions", version(20, 1, "twenty v1"));
        b.insert("versions", version(20, 2, "twenty v2"));
        b.insert("versions", version(50, 1, "fifty"));
    })
    .await;
}

/// Changes on both sides of a join: keys entering and leaving, rows
/// of live keys inserted, updated, hidden and deleted, a newer version
/// winning, and a key changing together with its row.
fn edits() -> Vec<Edit> {
    vec![
        Box::new(|b| b.insert("docs", doc(5, 40, 1))),
        Box::new(|b| b.update("projects", project(10, 1, "ten renamed"))),
        Box::new(|b| b.insert("versions", version(20, 3, "twenty v3"))),
        Box::new(|b| b.update("projects", project(40, 0, "forty hidden"))),
        Box::new(|b| b.update("docs", doc(4, 30, 1))),
        Box::new(|b| b.delete("projects", project_key(30))),
        Box::new(|b| {
            b.update("docs", doc(3, 50, 1));
            b.insert("versions", version(50, 2, "fifty v2"));
        }),
        Box::new(|b| b.insert("projects", project(60, 1, "sixty"))),
        Box::new(|b| b.insert("docs", doc(6, 60, 1))),
        Box::new(|b| {
            b.delete("docs", PrimaryKeyValue::U64(1));
            b.delete("docs", PrimaryKeyValue::U64(2));
            b.update("projects", project(10, 1, "ten again"));
        }),
        Box::new(|b| b.insert("projects", project(30, 1, "thirty back"))),
    ]
}

/// Subscribe to `graph` with and without stored arrangements, under each
/// budget, and check both against each other and against one-shot queries
/// after every edit. Returns how many arrangements the stored database
/// reads from storage.
async fn check_maintained(graph: GraphBuilder) -> usize {
    let mut counts = Vec::new();
    for budget in BUDGETS {
        counts.push(check_maintained_under(graph.clone(), budget).await);
    }
    assert!(counts.iter().all(|count| *count == counts[0]), "{counts:?}");
    counts[0]
}

async fn check_maintained_under(graph: GraphBuilder, budget: Option<usize>) -> usize {
    let mut stored = open_budgeted(budget).await;
    let mut held = open_db(false).await;
    seed(&mut stored).await;
    seed(&mut held).await;
    check_agree(stored, held, graph, edits, false, budget).await
}

/// Drain every update `sub` has into `live`.
fn drain(sub: &groove::db::Subscription, live: &mut Bag) {
    loop {
        match sub.try_recv() {
            Ok(deltas) => add(live, deltas.to_values().unwrap()),
            Err(TryRecvError::Empty) => return,
            Err(error) => panic!("subscription failed: {error:?}"),
        }
    }
}

/// Subscribe to `graph` on two seeded databases, one with stored
/// arrangements, and check them against each other and one-shot queries
/// after every edit. `stats_between` reads runtime statistics after every
/// edit, as a server does: statistics must not count as reads. `budget` is
/// the stored database's arrangement budget, which its resident bytes stay
/// within.
async fn check_agree(
    mut stored: Database,
    mut held: Database,
    graph: GraphBuilder,
    edits: impl Fn() -> Vec<Edit>,
    stats_between: bool,
    budget: Option<usize>,
) -> usize {
    let stored_sub = stored.subscribe_one_sink(graph.clone()).await.unwrap();
    let held_sub = held.subscribe_one_sink(graph.clone()).await.unwrap();
    let count = stored.runtime_stats().stored_arrangement_count;
    assert_eq!(held.runtime_stats().stored_arrangement_count, 0);
    let mut stored_live = Bag::new();
    let mut held_live = Bag::new();
    add(
        &mut stored_live,
        stored_sub.recv().unwrap().to_values().unwrap(),
    );
    add(
        &mut held_live,
        held_sub.recv().unwrap().to_values().unwrap(),
    );
    assert_eq!(stored_live, held_live, "hydration, budget {budget:?}");
    for (step, (stored_edit, held_edit)) in edits().into_iter().zip(edits()).enumerate() {
        apply(&mut stored, stored_edit).await;
        apply(&mut held, held_edit).await;
        if stats_between {
            stored.runtime_stats();
        }
        drain(&stored_sub, &mut stored_live);
        drain(&held_sub, &mut held_live);
        assert_eq!(stored_live, held_live, "step {step}, budget {budget:?}");
        assert_eq!(
            held_live,
            snapshot(&mut held, graph.clone()).await,
            "held one-shot, step {step}"
        );
        assert_eq!(
            stored_live,
            snapshot(&mut stored, graph.clone()).await,
            "stored one-shot, step {step}, budget {budget:?}"
        );
        if let Some(budget) = budget {
            let resident = stored.runtime_stats().stored_arrangement_resident_bytes;
            assert!(resident <= budget, "{resident} resident over {budget}");
        }
    }
    count
}

/// Reading runtime statistics between edits walks every arrangement; it
/// must not count as an unprobed read and fail the next evaluation.
#[futures_test::test]
async fn statistics_between_edits_leave_stored_reads_intact() {
    let graphs = [
        GraphBuilder::join(
            GraphBuilder::table("docs"),
            project_source(),
            ["project"],
            ["id"],
        ),
        GraphBuilder::anti_join(
            GraphBuilder::table("docs"),
            layered_source(),
            ["project"],
            ["id"],
        ),
        versions_per_project(),
    ];
    for graph in graphs {
        for budget in BUDGETS {
            let mut stored = open_budgeted(budget).await;
            let mut held = open_db(false).await;
            seed(&mut stored).await;
            seed(&mut held).await;
            let count = check_agree(stored, held, graph.clone(), edits, true, budget).await;
            assert!(count > 0);
        }
    }
}

/// Tags keyed by one non-integer column, and notes pointing at them.
fn keyed_schema(key: ColumnType, key_type: PrimaryKeyType) -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "tags",
            [
                ColumnSchema::new("key", key.clone()),
                ColumnSchema::new("label", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::composite([PrimaryKeyColumn::new(
            "key", key_type,
        )])),
        TableSchema::new(
            "notes",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("tag", key),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64).user_supplied()),
    ])
}

/// Joins on a tag key of `key` type agree with the held plan. `keys` holds
/// five distinct keys; the tests pick ones whose encodings are prefixes of
/// each other or hold zero bytes, which a key codec must keep apart.
async fn check_keyed(
    key: ColumnType,
    key_type: PrimaryKeyType,
    keys: [(Value, PrimaryKeyValue); 5],
) {
    let open = |stored: bool| {
        let schema = keyed_schema(key.clone(), key_type.clone());
        async move {
            let storage = MemoryStorage::new(&["tags", "notes"]).expect("valid families");
            let mut db = Database::new(schema, storage).await.unwrap();
            db.set_stored_arrangements_enabled(stored);
            db
        }
    };
    let tag = |k: usize, label: &str| vec![keys[k].0.clone(), Value::String(label.into())];
    let note = |id: u64, k: usize| vec![Value::U64(id), keys[k].0.clone()];
    let seed = |b: &mut groove::db::DatabaseBatch| {
        for k in 0..4 {
            b.insert("tags", tag(k, &format!("tag {k}")));
        }
        b.insert("notes", note(1, 0));
        b.insert("notes", note(2, 1));
        b.insert("notes", note(3, 2));
        b.insert("notes", note(4, 4));
    };
    let edits = || -> Vec<Edit> {
        let tag_key = |k: usize| PrimaryKeyValue::Composite(vec![keys[k].1.clone()]);
        vec![
            Box::new({
                let row = tag(4, "tag 4");
                move |b| b.insert("tags", row)
            }),
            Box::new({
                let row = tag(1, "tag 1 renamed");
                move |b| b.update("tags", row)
            }),
            Box::new({
                let key = tag_key(0);
                move |b| b.delete("tags", key)
            }),
            Box::new({
                let row = note(2, 3);
                move |b| b.update("notes", row)
            }),
            Box::new({
                let row = note(5, 1);
                move |b| b.insert("notes", row)
            }),
            Box::new(|b| b.delete("notes", PrimaryKeyValue::U64(1))),
            Box::new({
                let row = tag(0, "tag 0 back");
                move |b| b.insert("tags", row)
            }),
        ]
    };
    let notes = || GraphBuilder::table("notes");
    let tags = || GraphBuilder::table("tags");
    let graphs = [
        GraphBuilder::join(notes(), tags(), ["tag"], ["key"]),
        GraphBuilder::semi_join(notes(), tags(), ["tag"], ["key"]),
        GraphBuilder::anti_join(notes(), tags(), ["tag"], ["key"]),
    ];
    for graph in graphs {
        let mut stored = open(true).await;
        let mut held = open(false).await;
        apply(&mut stored, seed).await;
        apply(&mut held, seed).await;
        assert!(check_agree(stored, held, graph, edits, true, None).await > 0);
    }
}

#[futures_test::test]
async fn string_keys_read_by_key() {
    let key = |s: &str| (Value::String(s.into()), PrimaryKeyValue::String(s.into()));
    check_keyed(
        ColumnType::String,
        PrimaryKeyType::String,
        [key("ab"), key("abc"), key("a\0"), key("a"), key("")],
    )
    .await;
}

#[futures_test::test]
async fn bytes_keys_read_by_key() {
    let key = |b: &[u8]| (Value::Bytes(b.to_vec()), PrimaryKeyValue::Bytes(b.to_vec()));
    check_keyed(
        ColumnType::Bytes,
        PrimaryKeyType::Bytes,
        [
            key(b"ab"),
            key(b"abc"),
            key(&[0]),
            key(&[0, 0]),
            key(b"a\0c"),
        ],
    )
    .await;
}

#[futures_test::test]
async fn uuid_keys_read_by_key() {
    let key = |n: u128| {
        let id = uuid::Uuid::from_u128(n);
        (Value::Uuid(id), PrimaryKeyValue::Uuid(id))
    };
    check_keyed(
        ColumnType::Uuid,
        PrimaryKeyType::Uuid,
        [
            key(0),
            key(1),
            key(1 << 8),
            key(u128::MAX),
            key(u128::MAX - 1),
        ],
    )
    .await;
}

/// Joins with the project source on either side: the source is read by
/// its id through the union, the latest-version winner, the filter and the
/// projections. Docs are joined on `project`, not their key, so they stay
/// held.
#[futures_test::test]
async fn joins_read_stored_sources_by_key() {
    let graph = GraphBuilder::join(
        GraphBuilder::table("docs"),
        project_source(),
        ["project"],
        ["id"],
    );
    assert!(check_maintained(graph).await > 0);
    let graph = GraphBuilder::join(
        project_source(),
        GraphBuilder::table("docs"),
        ["id"],
        ["project"],
    );
    assert!(check_maintained(graph).await > 0);
    let graph = GraphBuilder::join(bucket_docs(), project_source(), ["project"], ["id"]);
    assert!(check_maintained(graph).await > 0);
}

/// Semi-joins in both directions and the anti-join of docs without a
/// visible project.
#[futures_test::test]
async fn semi_and_anti_joins_read_stored_sources_by_key() {
    let docs = GraphBuilder::table("docs");
    let docs_with_project =
        GraphBuilder::semi_join(docs.clone(), project_source(), ["project"], ["id"]);
    assert!(check_maintained(docs_with_project).await > 0);
    let referenced_projects =
        GraphBuilder::semi_join(project_source(), docs.clone(), ["id"], ["project"]);
    assert!(check_maintained(referenced_projects).await > 0);
    let orphans = GraphBuilder::anti_join(docs.clone(), project_source(), ["project"], ["id"]);
    assert!(check_maintained(orphans).await > 0);
    let unreferenced = GraphBuilder::anti_join(project_source(), docs, ["id"], ["project"]);
    assert!(check_maintained(unreferenced).await > 0);
}

/// A layered current view, as Jazz builds one: the latest versions, and the
/// plain projects no version shadows. The anti-join is on the view's key, so
/// a key's rows are decided by both layers' rows under that key.
fn layered_source() -> GraphBuilder {
    let latest = GraphBuilder::arg_max_by(branch("versions"), ["branch", "id"], ["version"])
        .project(["id", "visible", "name"]);
    let plain = branch("projects").project(["id", "visible", "name"]);
    GraphBuilder::union([
        latest.clone(),
        GraphBuilder::anti_join(plain, latest, ["id"], ["id"]),
    ])
}

/// Joins over a layered view read the view by key: the view's own
/// arrangement is stored, not only the layers' arrangements under it.
#[futures_test::test]
async fn layered_views_read_by_key() {
    let docs = GraphBuilder::table("docs");
    let graph = GraphBuilder::join(docs.clone(), layered_source(), ["project"], ["id"]);
    assert!(check_maintained(graph).await >= 3);
    let orphans = GraphBuilder::anti_join(docs, layered_source(), ["project"], ["id"]);
    assert!(check_maintained(orphans).await >= 3);
}

/// A key that extends the primary key (a row and one of its values) is read
/// by the primary-key part; the full key then picks the bucket.
#[futures_test::test]
async fn keys_extending_the_primary_key_read_by_its_leading_part() {
    let same = GraphBuilder::join(
        project_source(),
        layered_source().project(["id", "name"]),
        ["id", "name"],
        ["id", "name"],
    );
    assert!(check_maintained(same).await > 0);
    let renamed = GraphBuilder::anti_join(
        layered_source(),
        project_source(),
        ["id", "name"],
        ["id", "name"],
    );
    assert!(check_maintained(renamed).await > 0);
}

/// Grouping a stored table by its primary key after the static prefix reads
/// each changed group from storage.
#[futures_test::test]
async fn aggregates_read_stored_groups_by_key() {
    assert!(check_maintained(versions_per_project()).await > 0);
}

fn versions_per_project() -> GraphBuilder {
    GraphBuilder::aggregate(
        branch("versions"),
        ["id"],
        [AggregateExpr {
            function: AggregateFunction::Count,
            expression: None,
            distinct: false,
            output_name: Some("versions".to_owned()),
            output_identity: None,
        }],
    )
}

/// Only an arrangement keyed on the primary key after the static prefix is
/// stored: a join on another column holds its arrangements.
#[futures_test::test]
async fn joins_on_other_columns_hold_their_arrangements() {
    let by_name = GraphBuilder::join(project_source(), project_source(), ["name"], ["name"]);
    assert_eq!(check_maintained(by_name).await, 0);
}

/// Under a zero budget a stored arrangement holds only a tick's changes;
/// under a small one, only the buckets read most recently. The held plan,
/// like an unbounded budget after hydration, arranges every project and
/// every latest version.
#[futures_test::test]
async fn budgets_bound_the_rows_stored_arrangements_hold() {
    let graph = GraphBuilder::join(
        GraphBuilder::table("docs"),
        project_source(),
        ["project"],
        ["id"],
    );
    let mut rows = Vec::new();
    let mut resident = Vec::new();
    for budget in [Some(0), Some(2_000), None] {
        let mut db = open_budgeted(budget).await;
        apply(&mut db, |b| {
            for id in 1..=2_000 {
                b.insert("projects", project(id, 1, "p"));
                b.insert("versions", version(10_000 + id, 1, "v"));
            }
            b.insert("docs", doc(1, 7, 1));
            b.insert("docs", doc(2, 8, 1));
        })
        .await;
        let sub = db.subscribe_one_sink(graph.clone()).await.unwrap();
        let mut live = Bag::new();
        add(&mut live, sub.recv().unwrap().to_values().unwrap());
        assert_eq!(live.len(), 2);
        apply(&mut db, |b| b.insert("docs", doc(3, 9, 1))).await;
        while let Ok(deltas) = sub.try_recv() {
            add(&mut live, deltas.to_values().unwrap());
        }
        assert_eq!(live.len(), 3);
        // A one-shot query sharing the arrangement rebuilds it, then
        // keeps what its budget allows.
        assert_eq!(snapshot(&mut db, graph.clone()).await, live);
        rows.push(db.runtime_stats().arrangement_rows);
        resident.push(db.runtime_stats().stored_arrangement_resident_bytes);
    }
    let mut held = open_db(false).await;
    apply(&mut held, |b| {
        for id in 1..=2_000 {
            b.insert("projects", project(id, 1, "p"));
            b.insert("versions", version(10_000 + id, 1, "v"));
        }
    })
    .await;
    let _sub = held.subscribe_one_sink(graph.clone()).await.unwrap();
    let held_rows = held.runtime_stats().arrangement_rows;
    assert!(rows[0] < 20, "zero budget holds {rows:?}");
    assert_eq!(resident[0], 0);
    assert!(rows[1] < 100, "small budget holds {rows:?}");
    assert!(resident[1] > 0 && resident[1] <= 2_000, "{resident:?}");
    assert!(rows[2] >= 4_000, "unbounded budget holds {rows:?}");
    assert!(held_rows >= 4_000, "held plan holds {held_rows}");
}

/// Lowering the budget evicts at once; the evicted buckets are read from
/// storage again when a tick needs them.
#[futures_test::test]
async fn lowering_the_budget_evicts_resident_buckets() {
    let graph = GraphBuilder::join(
        GraphBuilder::table("docs"),
        project_source(),
        ["project"],
        ["id"],
    );
    let mut db = open_budgeted(None).await;
    apply(&mut db, |b| {
        for id in 1..=500 {
            b.insert("projects", project(id, 1, "p"));
        }
        b.insert("docs", doc(1, 7, 1));
    })
    .await;
    let sub = db.subscribe_one_sink(graph.clone()).await.unwrap();
    let mut live = Bag::new();
    add(&mut live, sub.recv().unwrap().to_values().unwrap());
    let before = db.runtime_stats().stored_arrangement_resident_bytes;
    assert!(before > 10_000, "{before}");
    db.set_cache_memory_budget(Some(1_000));
    let after = db.runtime_stats().stored_arrangement_resident_bytes;
    assert!(after <= 1_000, "{after}");
    apply(&mut db, |b| {
        b.insert("docs", doc(2, 300, 1));
        b.update("projects", project(7, 1, "seven"));
    })
    .await;
    while let Ok(deltas) = sub.try_recv() {
        add(&mut live, deltas.to_values().unwrap());
    }
    assert_eq!(live.len(), 2);
    assert_eq!(live, snapshot(&mut db, graph).await);
    assert!(db.runtime_stats().stored_arrangement_resident_bytes <= 1_000);
}

/// A new subscription sharing a stored arrangement probes it by its own
/// keys rather than rebuilding it, and agrees with the held plan.
#[futures_test::test]
async fn new_subscribers_probe_a_shared_arrangement_by_key() {
    let docs_in = |bucket: u64| {
        GraphBuilder::table("docs").filter(PredicateExpr::eq("bucket", Value::U64(bucket)))
    };
    for stored in [true, false] {
        let mut db = open_db(stored).await;
        apply(&mut db, |b| {
            for id in 1..=2_000 {
                b.insert("projects", project(id, 1, "p"));
            }
            b.insert("docs", doc(1, 7, 1));
            b.insert("docs", doc(2, 8, 2));
        })
        .await;
        let first = GraphBuilder::join(docs_in(1), project_source(), ["project"], ["id"]);
        let second = GraphBuilder::join(docs_in(2), project_source(), ["project"], ["id"]);
        let _first = db.subscribe_one_sink(first).await.unwrap();
        let sub = db.subscribe_one_sink(second.clone()).await.unwrap();
        let mut live = Bag::new();
        add(&mut live, sub.recv().unwrap().to_values().unwrap());
        assert_eq!(live, snapshot(&mut db, second).await, "stored {stored}");
        assert_eq!(live.len(), 1);
    }
}

/// A recursive relation's step runs without an evaluation session; its
/// arrangements stay held, and the root's still agree with the held plan.
#[futures_test::test]
async fn recursive_relations_agree_with_the_held_plan() {
    let reach = RecordDescriptor::new([("node", ColumnType::U64)]);
    let seed = GraphBuilder::table("edges")
        .filter(PredicateExpr::eq("src", Value::U64(1)))
        .project_fields([groove::db::ProjectField::renamed("src", "node")]);
    let step = GraphBuilder::join(
        GraphBuilder::frontier_source("frontier", reach),
        GraphBuilder::table("edges"),
        ["node"],
        ["src"],
    )
    .project_fields([groove::db::ProjectField::renamed("right.dst", "node")]);
    let graph = GraphBuilder::join(
        GraphBuilder::recursive(seed, step, "frontier", 16),
        GraphBuilder::table("edges"),
        ["node"],
        ["src"],
    );
    let mut lives = Vec::new();
    for stored in [true, false] {
        let mut db = open_db(stored).await;
        apply(&mut db, |b| {
            for (src, dst) in [(1, 2), (2, 3), (3, 4), (7, 8)] {
                b.insert("edges", vec![Value::U64(src), Value::U64(dst)]);
            }
        })
        .await;
        let sub = db.subscribe_one_sink(graph.clone()).await.unwrap();
        let mut live = Bag::new();
        add(&mut live, sub.recv().unwrap().to_values().unwrap());
        for (src, dst) in [(4, 5), (5, 7)] {
            apply(&mut db, |b| {
                b.insert("edges", vec![Value::U64(src), Value::U64(dst)])
            })
            .await;
        }
        apply(&mut db, |b| {
            b.delete(
                "edges",
                PrimaryKeyValue::Composite(vec![PrimaryKeyValue::U64(2), PrimaryKeyValue::U64(3)]),
            )
        })
        .await;
        while let Ok(deltas) = sub.try_recv() {
            add(&mut live, deltas.to_values().unwrap());
        }
        assert_eq!(
            live,
            snapshot(&mut db, graph.clone()).await,
            "stored {stored}"
        );
        lives.push(live);
    }
    assert_eq!(lives[0], lives[1]);
}

fn author(name: &str) -> Value {
    let descriptor = RecordDescriptor::new([("name", ColumnType::String)]);
    let raw = descriptor.create(&[Value::String(name.into())]).unwrap();
    Value::Record(groove::records::OwnedRecord::new(raw, descriptor))
}

/// Docs and notes whose `author` is interned: rows store a dictionary code.
async fn open_interned(stored: bool, budget: Option<usize>) -> Database {
    let author_type = ColumnType::Record(Box::new(RecordDescriptor::new([(
        "name",
        ColumnType::String,
    )])));
    let schema = DatabaseSchema::new([
        schema().table("docs").unwrap().clone(),
        TableSchema::new(
            "notes",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new(
                    "author",
                    ColumnType::interned("authors", author_type.clone()),
                ),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64).user_supplied()),
    ])
    .with_dictionary(DictionarySchema::new("authors", author_type));
    let storage = MemoryStorage::new(&schema.column_families()).unwrap();
    let mut db = Database::new(schema, storage).await.unwrap();
    db.set_stored_arrangements_enabled(stored);
    db.set_cache_memory_budget(budget);
    apply(&mut db, |batch| {
        for (id, project) in [(1, 1), (2, 1), (3, 2)] {
            batch.insert("docs", doc(id, project, 0));
        }
        batch.insert("notes", vec![Value::U64(1), author("alice")]);
        batch.insert("notes", vec![Value::U64(2), author("bob")]);
    })
    .await;
    db
}

/// A stored source with an interned column is read from storage as codes;
/// its probed rows must reach the join as values, under every budget, as
/// the held side's do.
#[futures_test::test]
async fn stored_sources_resolve_interned_columns() {
    let notes = GraphBuilder::table("notes").project_fields([
        ProjectField::named("id"),
        ProjectField::record_field("author", ["name"], "author_name"),
    ]);
    let graph = GraphBuilder::join(GraphBuilder::table("docs"), notes, ["project"], ["id"]);
    let edits = || -> Vec<Edit> {
        vec![
            Box::new(|batch| batch.update("notes", vec![Value::U64(2), author("carol")])),
            Box::new(|batch| batch.insert("docs", doc(4, 2, 0))),
            Box::new(|batch| batch.insert("notes", vec![Value::U64(3), author("alice")])),
            Box::new(|batch| batch.insert("docs", doc(5, 3, 0))),
        ]
    };
    for budget in BUDGETS {
        let stored = open_interned(true, budget).await;
        let held = open_interned(false, None).await;
        let count = check_agree(stored, held, graph.clone(), edits, false, budget).await;
        assert!(count > 0, "the notes source is stored, budget {budget:?}");
    }
    let mut stored = open_interned(true, Some(0)).await;
    let rows = snapshot(&mut stored, graph).await;
    assert!(rows.keys().any(|row| row.contains("alice")), "{rows:?}");
}
