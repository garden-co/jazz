//! Slow-query log: every query run at or over the threshold is recorded
//! under its compiled shape with what SQLite did for it and its plan, so
//! the host can show the developer which index would make it fast.

use crate::common;

use std::collections::BTreeMap;
use std::time::Duration;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn db() -> Db {
    let tasks = TableSchemaBuilder::new("tasks")
        .column("title", ColumnType::Text)
        .column("owner", ColumnType::Text)
        .index_only(["owner"])
        .policies(allow_all_policies());
    let db = block_on(Db::open(DbConfig::new(
        compile_schema(&SchemaBuilder::new().table(tasks).build()),
        DbIdentity {
            node: NodeUuid::from_bytes([5; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    for n in 0..40u8 {
        block_on(db.insert(
            "tasks",
            BTreeMap::from([
                ("title".to_owned(), Value::String(format!("task {n}"))),
                ("owner".to_owned(), Value::String(format!("user {}", n % 4))),
            ]),
            InsertOptions {
                row_id: Some(RowUuid::from_bytes([n + 1; 16])),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    db
}

fn run(db: &Db, query: Query) -> usize {
    let prepared = db.prepare_query(&query).unwrap();
    block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .len()
}

#[test]
fn slow_queries_report_their_plan_and_the_index_that_would_help() {
    let db = db();
    // Record every run.
    db.set_slow_query_threshold(Some(Duration::ZERO));
    let by_title =
        Query::from("tasks").filter(eq(col("title"), lit(Value::String("task 7".into()))));
    assert_eq!(run(&db, by_title.clone()), 1);
    assert_eq!(run(&db, by_title), 1);
    let ordered = Query::from("tasks").order_by("title", OrderDirection::Asc);
    assert_eq!(run(&db, ordered), 40);
    let by_owner =
        Query::from("tasks").filter(eq(col("owner"), lit(Value::String("user 1".into()))));
    assert_eq!(run(&db, by_owner), 10);

    let (shapes, dropped) = db.take_slow_queries();
    assert_eq!(dropped, 0);
    assert_eq!(shapes.len(), 3, "{shapes:#?}");
    let find = |needle: &str| {
        shapes
            .iter()
            .find(|shape| common::unhashed(&shape.sql).contains(needle) && shape.table == "tasks")
            .unwrap_or_else(|| panic!("no shape with {needle}: {shapes:#?}"))
    };
    // The same query twice is one shape with two runs; its literal is a
    // bound parameter, not part of the shape.
    let title = shapes
        .iter()
        .find(|shape| shape.runs == 2)
        .unwrap_or_else(|| panic!("{shapes:#?}"));
    assert!(!title.sql.contains("task 7"));
    assert_eq!(title.rows, 1);
    assert!(title.scanned >= 39, "{title:#?}");
    let hint = title.hint().unwrap_or_else(|| panic!("{title:#?}"));
    assert!(hint.contains("scans all of tasks"), "{hint}");

    let ordered = find("ORDER BY t.\"title\" ASC");
    assert_eq!(ordered.rows, 40);
    assert!(
        ordered
            .hint()
            .is_some_and(|hint| hint.contains("ordering columns")),
        "{ordered:#?}"
    );

    let owner = shapes
        .iter()
        .find(|shape| shape.rows == 10)
        .unwrap_or_else(|| panic!("{shapes:#?}"));
    assert_eq!(
        owner.scanned, 0,
        "an indexed lookup scans nothing: {owner:#?}"
    );
    assert!(owner.hint().is_none(), "{owner:#?}");

    assert!(
        db.take_slow_queries().0.is_empty(),
        "taking starts a new window"
    );
}

#[test]
fn runs_under_the_threshold_are_not_recorded() {
    let db = db();
    db.set_slow_query_threshold(Some(Duration::from_secs(3600)));
    run(&db, Query::from("tasks"));
    assert!(db.take_slow_queries().0.is_empty());
    db.set_slow_query_threshold(None);
    run(&db, Query::from("tasks"));
    assert!(db.take_slow_queries().0.is_empty());
}
