//! Stored arrangements: an arrangement of a stored table, seen through
//! key-preserving operators and keyed on its primary key, is read from
//! storage by the keys a tick touches instead of being held.
//!
//! Every maintained case runs on two databases fed the same edits, one with
//! stored arrangements and one without, and their results must agree after
//! every edit, with each other and with one-shot queries.

use std::collections::BTreeMap;

use groove::db::{Database, GraphBuilder, PredicateExpr, PrimaryKeyValue};
use groove::ivm::{AggregateExpr, AggregateFunction, LiteralValue, StaticScanSpec};
use groove::records::RecordDescriptor;
use groove::records::Value;
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, PrimaryKeyColumn,
    TableSchema,
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

/// Subscribe to `graph` with and without stored arrangements and check both
/// against each other and against one-shot queries after every edit.
/// Returns how many arrangements the stored database reads from storage.
async fn check_maintained(graph: GraphBuilder) -> usize {
    let mut stored = open_db(true).await;
    let mut held = open_db(false).await;
    seed(&mut stored).await;
    seed(&mut held).await;
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
    assert_eq!(stored_live, held_live, "hydration");
    for (step, (stored_edit, held_edit)) in edits().into_iter().zip(edits()).enumerate() {
        apply(&mut stored, stored_edit).await;
        apply(&mut held, held_edit).await;
        while let Ok(deltas) = stored_sub.try_recv() {
            add(&mut stored_live, deltas.to_values().unwrap());
        }
        while let Ok(deltas) = held_sub.try_recv() {
            add(&mut held_live, deltas.to_values().unwrap());
        }
        assert_eq!(stored_live, held_live, "step {step}");
        assert_eq!(
            held_live,
            snapshot(&mut held, graph.clone()).await,
            "held one-shot, step {step}"
        );
        assert_eq!(
            stored_live,
            snapshot(&mut stored, graph.clone()).await,
            "stored one-shot, step {step}"
        );
    }
    count
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
    let graph = GraphBuilder::aggregate(
        branch("versions"),
        ["id"],
        [AggregateExpr {
            function: AggregateFunction::Count,
            expression: None,
            distinct: false,
            output_name: Some("versions".to_owned()),
            output_identity: None,
        }],
    );
    assert!(check_maintained(graph).await > 0);
}

/// Only an arrangement keyed on the primary key after the static prefix is
/// stored: a join on another column holds its arrangements.
#[futures_test::test]
async fn joins_on_other_columns_hold_their_arrangements() {
    let by_name = GraphBuilder::join(project_source(), project_source(), ["name"], ["name"]);
    assert_eq!(check_maintained(by_name).await, 0);
}

/// A stored arrangement holds only a tick's changes; the held plan arranges
/// every project and every latest version.
#[futures_test::test]
async fn stored_arrangements_hold_no_table_rows() {
    let graph = GraphBuilder::join(
        GraphBuilder::table("docs"),
        project_source(),
        ["project"],
        ["id"],
    );
    let mut rows = Vec::new();
    for stored in [true, false] {
        let mut db = open_db(stored).await;
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
        // A one-shot query sharing the arrangement rebuilds it for its own
        // evaluation only.
        assert_eq!(snapshot(&mut db, graph.clone()).await, live);
        rows.push(db.runtime_stats().arrangement_rows);
    }
    assert!(rows[0] < 20, "stored plan holds {rows:?}");
    assert!(rows[1] >= 4_000, "held plan holds {rows:?}");
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
