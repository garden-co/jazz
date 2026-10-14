//! Keyed joins: a join whose input is a stored table, seen through
//! key-preserving operators and joined on its primary key, reads that table
//! by the other input's keys instead of arranging all of it.
//!
//! Every maintained case is checked against the whole-table plan of the same
//! graph, compiled with keyed joins switched off.

use std::collections::BTreeMap;

use groove::db::{Database, GraphBuilder, PredicateExpr, PrimaryKeyValue};
use groove::ivm::{LiteralValue, StaticScanSpec};
use groove::records::RecordDescriptor;
use groove::records::Value;
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, PrimaryKeyColumn,
    TableSchema,
};
use groove::storage::MemoryStorage;

/// `docs(id, project, bucket, owner)` refer to projects; a doc's owner is
/// the UUID of its bucket. A project's current row is
/// either in `projects(branch, id, ..)` or the latest of its
/// `versions(branch, id, version, ..)`, under a static branch prefix.
/// `edges(src, dst)` feed a recursive relation.
fn schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "docs",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("project", ColumnType::U64),
                ColumnSchema::new("bucket", ColumnType::U64),
                ColumnSchema::new("owner", ColumnType::Uuid),
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

async fn open_db() -> Database {
    let storage =
        MemoryStorage::new(&["docs", "projects", "versions", "edges"]).expect("valid families");
    Database::new(schema(), storage).await.unwrap()
}

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
    vec![
        Value::U64(id),
        Value::U64(project),
        Value::U64(bucket),
        owner(bucket),
    ]
}

fn owner(bucket: u64) -> Value {
    Value::Uuid(uuid::Uuid::from_u128(u128::from(bucket)))
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

/// The graph's rows under the whole-table plan.
async fn oracle(db: &mut Database, graph: GraphBuilder) -> Bag {
    db.set_keyed_joins_enabled(false);
    let rows = snapshot(db, graph).await;
    db.set_keyed_joins_enabled(true);
    rows
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

/// Changes on both sides of a keyed join: keys entering and leaving, rows
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

/// Subscribe to `graph` and check it against its whole-table plan after
/// every edit. Returns how many join inputs its compilation keyed.
async fn check_maintained(graph: GraphBuilder) -> u64 {
    let mut db = open_db().await;
    seed(&mut db).await;
    let before = db.runtime_stats().keyed_join_inputs;
    let sub = db.subscribe_one_sink(graph.clone()).await.unwrap();
    let keyed = db.runtime_stats().keyed_join_inputs - before;
    let mut live = Bag::new();
    add(&mut live, sub.recv().unwrap().to_values().unwrap());
    assert_eq!(live, oracle(&mut db, graph.clone()).await, "hydration");
    for (step, edit) in edits().into_iter().enumerate() {
        apply(&mut db, edit).await;
        while let Ok(deltas) = sub.try_recv() {
            add(&mut live, deltas.to_values().unwrap());
        }
        assert_eq!(
            live,
            oracle(&mut db, graph.clone()).await,
            "step {step}: {live:?}"
        );
        assert_eq!(
            live,
            snapshot(&mut db, graph.clone()).await,
            "one-shot, step {step}"
        );
    }
    keyed
}

/// Inner join with the stored source on the right: the right side is read
/// by the docs' project ids, through the union, the latest-version winner,
/// the filter and the projections.
#[futures_test::test]
async fn join_reads_its_right_table_by_key() {
    let graph = GraphBuilder::join(bucket_docs(), project_source(), ["project"], ["id"]);
    assert!(check_maintained(graph).await > 0);
}

/// With the stored source on the left, the left side is read by key.
#[futures_test::test]
async fn join_reads_its_left_table_by_key() {
    let graph = GraphBuilder::join(project_source(), bucket_docs(), ["id"], ["project"]);
    assert!(check_maintained(graph).await > 0);
}

/// Semi-joins in both directions and the anti-join of docs without a
/// visible project.
#[futures_test::test]
async fn semi_and_anti_joins_read_tables_by_key() {
    let docs_with_project =
        GraphBuilder::semi_join(bucket_docs(), project_source(), ["project"], ["id"]);
    assert!(check_maintained(docs_with_project).await > 0);
    let referenced_projects =
        GraphBuilder::semi_join(project_source(), bucket_docs(), ["id"], ["project"]);
    assert!(check_maintained(referenced_projects).await > 0);
    let orphans = GraphBuilder::anti_join(bucket_docs(), project_source(), ["project"], ["id"]);
    assert!(check_maintained(orphans).await > 0);
}

/// An anti-join that filters a stored source is looked through: the
/// projects not referenced from bucket 9 are still read by the bucket-1
/// docs' keys.
#[futures_test::test]
async fn keyed_reads_pass_through_an_anti_join_filter() {
    let unreferenced = GraphBuilder::anti_join(
        project_source(),
        GraphBuilder::table("docs")
            .filter(PredicateExpr::eq("bucket", Value::U64(9)))
            .project(["project"]),
        ["id"],
        ["project"],
    );
    let graph = GraphBuilder::join(bucket_docs(), unreferenced, ["project"], ["id"]);
    assert!(check_maintained(graph).await > 0);
}

/// Only a primary key continuing the static prefix can be probed: a join on
/// another column, or the left side of an anti-join, keeps the whole-table
/// plan.
#[futures_test::test]
async fn joins_on_other_columns_keep_the_whole_table() {
    let by_name = GraphBuilder::join(project_source(), project_source(), ["name"], ["name"]);
    assert_eq!(check_maintained(by_name).await, 0);
    let anti = GraphBuilder::anti_join(project_source(), bucket_docs(), ["id"], ["project"]);
    assert_eq!(check_maintained(anti).await, 0);
}

/// Keys drawn from a whole table would probe once per row and hold the
/// matched rows twice, so a join whose other side is a whole table keeps
/// the whole-table plan.
#[futures_test::test]
async fn keys_from_a_whole_table_keep_the_whole_table() {
    let graph = GraphBuilder::join(
        GraphBuilder::table("docs"),
        project_source(),
        ["project"],
        ["id"],
    );
    assert_eq!(check_maintained(graph).await, 0);
}

/// A filter pinning a column to one UUID selects few rows of a whole
/// table, so its rows key the join.
#[futures_test::test]
async fn uuid_equality_filters_are_narrow_keys() {
    let owned = GraphBuilder::join(
        GraphBuilder::table("docs").filter(PredicateExpr::eq("owner", owner(1))),
        project_source(),
        ["project"],
        ["id"],
    );
    assert!(check_maintained(owned).await > 0);
}

/// The keyed plan holds the matched rows; the whole-table plan arranges
/// every project and every latest version.
#[futures_test::test]
async fn keyed_join_memory_follows_the_keys() {
    let graph = GraphBuilder::join(bucket_docs(), project_source(), ["project"], ["id"]);
    let mut held = Vec::new();
    for keyed in [true, false] {
        let mut db = open_db().await;
        db.set_keyed_joins_enabled(keyed);
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
        let stats = db.runtime_stats();
        held.push(stats.arrangement_rows + stats.lookup_rows);
    }
    assert!(held[0] < 20, "keyed plan holds {held:?}");
    assert!(held[1] >= 4_000, "whole-table plan holds {held:?}");
}

/// A recursive relation's step cannot probe storage, so its joins keep the
/// whole-table plan even where a keyed read would apply.
#[futures_test::test]
async fn recursive_steps_keep_whole_table_joins() {
    let mut db = open_db().await;
    apply(&mut db, |b| {
        for (src, dst) in [(1, 2), (2, 3), (3, 4), (7, 8)] {
            b.insert("edges", vec![Value::U64(src), Value::U64(dst)]);
        }
    })
    .await;
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
    let graph = GraphBuilder::recursive(seed, step, "frontier", 16);
    let sub = db.subscribe_one_sink(graph.clone()).await.unwrap();
    assert_eq!(db.runtime_stats().keyed_join_inputs, 0);
    let mut live = Bag::new();
    add(&mut live, sub.recv().unwrap().to_values().unwrap());
    assert_eq!(live.len(), 4, "{live:?}");
    apply(&mut db, |b| {
        b.insert("edges", vec![Value::U64(4), Value::U64(5)])
    })
    .await;
    while let Ok(deltas) = sub.try_recv() {
        add(&mut live, deltas.to_values().unwrap());
    }
    assert_eq!(live, oracle(&mut db, graph).await);
    assert_eq!(live.len(), 5, "{live:?}");
}
