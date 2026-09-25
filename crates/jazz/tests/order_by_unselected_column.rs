//! Ordering by a column that the query's `select` projects away (#3495).
//!
//! SQL semantics: an order key orders the result whether or not it is
//! selected, and it is not returned when it isn't selected.

use std::collections::BTreeMap;

mod common;

use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, LocalUpdates, MergeableTxOps, Propagation, ReadOpts,
    SubscriptionEvent,
};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::node::CurrentRow;
use jazz::query::{OrderDirection, Query};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

use common::{allow_all_policies, compile_schema};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("tasks")
                    .column("title", ColumnType::Text)
                    .column("rank", ColumnType::Integer)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open_db() -> Db<TestStorage> {
    let schema = schema();
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(DbConfig::new(
        schema,
        TestStorage::new(&refs),
        DbIdentity {
            node: NodeUuid::from_bytes([0x5e; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .expect("open db")
}

fn local() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Local,
        local_updates: LocalUpdates::Immediate,
        propagation: Propagation::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

fn task_cells(title: &str, rank: i32) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("rank".to_owned(), Value::I32(rank)),
    ])
}

/// Alice's four tasks, inserted so that row-id (insertion) order differs from
/// rank order: row-id order is c, a, d, b; rank order is a, b, c, d.
fn seeded_db() -> Db<TestStorage> {
    let db = open_db();
    for (byte, title, rank) in [(1u8, "c", 3), (2, "a", 1), (3, "d", 4), (4, "b", 2)] {
        block_on(db.insert(
            "tasks",
            task_cells(title, rank),
            InsertOptions {
                row_id: Some(RowUuid::from_bytes([byte; 16])),
                ..Default::default()
            },
        ))
        .expect("insert task");
    }
    db
}

fn by_rank(direction: OrderDirection) -> Query {
    Query::from("tasks")
        .order_by("rank", direction)
        .select(["title"])
}

/// Titles in result order. Also checks that the unselected order key is not
/// returned.
fn titles(rows: impl IntoIterator<Item = CurrentRow>) -> Vec<String> {
    let schema = schema();
    let table = &schema.tables()[0];
    rows.into_iter()
        .map(|row| {
            assert_eq!(
                row.cell(table, "rank"),
                None,
                "an unselected order key must not be returned"
            );
            match row.cell(table, "title") {
                Some(Value::String(title)) => title,
                other => panic!("expected a title, got {other:?}"),
            }
        })
        .collect()
}

fn read_titles(db: &Db<TestStorage>, query: &Query) -> Vec<String> {
    let prepared = db.prepare_query(query).expect("prepare query");
    titles(block_on(db.all(&prepared, local())).expect("read"))
}

/// Alice lists her tasks by rank but only selects the title. The rows come
/// back in rank order in both directions, and `rank` itself is not returned.
///
/// ```text
/// alice ──insert c(3), a(1), d(4), b(2)──► db
/// alice ──select title order by rank asc──► [a, b, c, d]
/// alice ──select title order by rank desc──► [d, c, b, a]
/// ```
#[test]
fn one_shot_read_orders_by_an_unselected_column() {
    let db = seeded_db();
    assert_eq!(
        read_titles(&db, &by_rank(OrderDirection::Asc)),
        ["a", "b", "c", "d"]
    );
    assert_eq!(
        read_titles(&db, &by_rank(OrderDirection::Desc)),
        ["d", "c", "b", "a"]
    );
}

/// The synchronous local-preview read (`Db::read`) orders the same way.
#[test]
fn local_preview_read_orders_by_an_unselected_column() {
    let db = seeded_db();
    let prepared = db
        .prepare_query(&by_rank(OrderDirection::Desc))
        .expect("prepare query");
    assert_eq!(
        titles(db.read(&prepared).expect("read")),
        ["d", "c", "b", "a"]
    );
}

/// Alice pages through her tasks by rank while selecting only the title. Each
/// page holds the right rows (limit and offset follow rank order) and lists
/// them in rank order.
///
/// ```text
/// alice ──limit 2──────────► [a, b]
/// alice ──offset 1 limit 2─► [b, c]
/// alice ──desc offset 1 limit 2─► [c, b]
/// ```
#[test]
fn limit_and_offset_pages_order_by_an_unselected_column() {
    let db = seeded_db();
    assert_eq!(
        read_titles(&db, &by_rank(OrderDirection::Asc).limit(2)),
        ["a", "b"]
    );
    assert_eq!(
        read_titles(&db, &by_rank(OrderDirection::Asc).offset(1).limit(2)),
        ["b", "c"]
    );
    assert_eq!(
        read_titles(&db, &by_rank(OrderDirection::Desc).offset(1).limit(2)),
        ["c", "b"]
    );
}

/// A trusted host serving bob's read (`all_for_identity`) orders by the
/// unselected column too.
#[test]
fn trusted_identity_read_orders_by_an_unselected_column() {
    let db = seeded_db();
    let bob = AuthorSubject::for_test_bytes([0xb0; 16]);
    let prepared = db
        .prepare_query(&by_rank(OrderDirection::Asc))
        .expect("prepare query");
    let rows = block_on(db.all_for_identity(&prepared, local(), bob)).expect("read as bob");
    assert_eq!(titles(rows), ["a", "b", "c", "d"]);
}

/// Inside a transaction, alice stages a new task with rank 0 and reads her
/// tasks by rank: the staged row is read back first, in rank order with the
/// committed rows.
///
/// ```text
/// alice ──tx: insert e(0)──► read select title order by rank ──► [e, a, b, c, d]
/// ```
#[test]
fn transaction_read_orders_by_an_unselected_column() {
    let db = seeded_db();
    let prepared = db
        .prepare_query(&by_rank(OrderDirection::Asc))
        .expect("prepare query");
    let (read, _) = block_on(db.transaction(async |tx| {
        tx.insert(
            "tasks",
            task_cells("e", 0),
            InsertOptions {
                row_id: Some(RowUuid::from_bytes([5; 16])),
                ..Default::default()
            },
        )
        .await?;
        tx.all_prepared(&prepared).await
    }))
    .expect("transaction");
    assert_eq!(titles(read), ["e", "a", "b", "c", "d"]);
}

/// A subscription to the same shape already ordered correctly before #3495
/// was fixed; this pins that it still does, including for a page, and that
/// the unselected order key is not delivered.
#[test]
fn subscription_orders_by_an_unselected_column() {
    let db = seeded_db();
    for (query, expected) in [
        (by_rank(OrderDirection::Asc), vec!["a", "b", "c", "d"]),
        (
            by_rank(OrderDirection::Asc).offset(1).limit(2),
            vec!["b", "c"],
        ),
    ] {
        let prepared = db.prepare_query(&query).expect("prepare query");
        let mut subscription = block_on(db.subscribe(&prepared, local())).expect("subscribe");
        let Some(SubscriptionEvent::Delta {
            reset: true,
            mut added,
            ..
        }) = block_on(subscription.next_event())
        else {
            panic!("expected an initial reset");
        };
        added.sort_by_key(|row| row.index);
        assert_eq!(titles(added.into_iter().map(|row| row.row)), expected);
    }
}
