//! Ordering by a column that the query's `select` projects away (#3495).
//!
//! SQL semantics: an order key orders the result whether or not it is
//! selected, and it is not returned when it isn't selected.

use std::collections::BTreeMap;

mod common;

use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, MergeableTxOps, ReadOpts, SubscriptionEvent,
};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::node::CurrentRow;
use jazz::query::{ArraySubquery, OrderDirection, Query};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

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
            .table(
                TableSchemaBuilder::new("notes")
                    .fk_column("task_id", "tasks")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open_db() -> Db {
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
        tier: jazz::db::ReadTier::LocalOnly,
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
fn seeded_db() -> Db {
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
    let table = schema
        .tables()
        .iter()
        .find(|table| table.name.as_str() == "tasks")
        .expect("tasks table");
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

fn read_titles(db: &Db, query: &Query) -> Vec<String> {
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

/// A read with an include (array subquery) must not return the unselected
/// order key either. Ordering such reads by an unselected column is tracked
/// separately in #3503; this pins only that the key does not leak.
#[test]
fn include_read_does_not_return_the_unselected_order_key() {
    let db = seeded_db();
    let query = by_rank(OrderDirection::Asc)
        .array_subquery(ArraySubquery::new("notes", "notes", "task_id", "id").select(["body"]));
    let prepared = db.prepare_query(&query).expect("prepare query");
    let mut read = titles(block_on(db.all(&prepared, local())).expect("read"));
    read.sort();
    assert_eq!(read, ["a", "b", "c", "d"]);
}

/// Alice reads one metric through prepared queries. Non-null scalar double
/// predicates use numeric ordering and equality, whether literals are bare or
/// nullable-present; column predicates provide the same observable membership.
#[test]
fn float_literal_comparisons_match_numeric_order() {
    use jazz::query::{
        Operand, Predicate, col, eq, gt, gte, in_list, is_null, lit, lt, lte, ne, not,
    };

    // One source row suffices: constant predicates retain its exact identity or
    // exclude it. No NaN is stored; unordered cases use query literals only.
    let stored = [
        ("score", -2.0),
        ("negative", -1.0),
        ("zero", -0.0),
        ("positive_zero", 0.0),
        ("one", 1.0),
        ("two", 2.0),
        ("negative_infinity", f64::NEG_INFINITY),
        ("positive_infinity", f64::INFINITY),
    ];
    let mut table = TableSchemaBuilder::new("metrics")
        .nullable_column("optional_score", ColumnType::Double)
        .nullable_column("null_score", ColumnType::Double)
        .policies(allow_all_policies());
    for (name, _) in stored {
        table = table.column(name, ColumnType::Double);
    }
    let schema = compile_schema(&SchemaBuilder::new().table(table).build());
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let db = block_on(Db::open(DbConfig::new(
        schema,
        TestStorage::new(&refs),
        DbIdentity {
            node: NodeUuid::from_bytes([0x6f; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .expect("open metrics db");
    let metric = RowUuid::from_bytes([0x42; 16]);
    let mut cells = stored
        .into_iter()
        .map(|(name, value)| (name.to_owned(), Value::F64(value)))
        .collect::<BTreeMap<_, _>>();
    cells.insert(
        "optional_score".to_owned(),
        Value::Nullable(Some(Box::new(Value::F64(-2.0)))),
    );
    cells.insert("null_score".to_owned(), Value::Nullable(None));
    block_on(db.insert(
        "metrics",
        cells,
        InsertOptions {
            row_id: Some(metric),
            ..Default::default()
        },
    ))
    .expect("insert metric");

    let operators: [(&str, fn(Operand, Operand) -> Predicate); 6] = [
        ("eq", eq),
        ("ne", ne),
        ("lt", lt),
        ("lte", lte),
        ("gt", gt),
        ("gte", gte),
    ];
    // Worked numeric outcomes in operator order, independent of the comparator.
    let less = [false, true, true, true, false, false];
    let equal = [true, false, false, true, false, true];
    let greater = [false, true, false, false, true, true];
    let unordered = [false; 6];
    let pairs = [
        ("negative ascending", -2.0, -1.0, "score", "negative", less),
        (
            "negative descending",
            -1.0,
            -2.0,
            "negative",
            "score",
            greater,
        ),
        (
            "negative to zero",
            -1.0,
            0.0,
            "negative",
            "positive_zero",
            less,
        ),
        (
            "zero to negative",
            0.0,
            -1.0,
            "positive_zero",
            "negative",
            greater,
        ),
        ("cross-sign", -2.0, 1.0, "score", "one", less),
        ("positive ascending", 1.0, 2.0, "one", "two", less),
        ("positive descending", 2.0, 1.0, "two", "one", greater),
        ("finite equal", -2.0, -2.0, "score", "score", equal),
        (
            "negative zero to positive zero",
            -0.0,
            0.0,
            "zero",
            "positive_zero",
            equal,
        ),
        (
            "positive zero to negative zero",
            0.0,
            -0.0,
            "positive_zero",
            "zero",
            equal,
        ),
        (
            "negative infinity",
            f64::NEG_INFINITY,
            -2.0,
            "negative_infinity",
            "score",
            less,
        ),
        (
            "positive infinity",
            f64::INFINITY,
            2.0,
            "positive_infinity",
            "two",
            greater,
        ),
        (
            "infinite equal",
            f64::INFINITY,
            f64::INFINITY,
            "positive_infinity",
            "positive_infinity",
            equal,
        ),
        ("NaN left", f64::NAN, 1.0, "", "one", unordered),
        ("NaN right", 1.0, f64::NAN, "one", "", unordered),
        ("NaN both", f64::NAN, f64::NAN, "", "", unordered),
    ];
    let float = |value, present| {
        let value = Value::F64(value);
        lit(if present {
            Value::Nullable(Some(Box::new(value)))
        } else {
            value
        })
    };
    let mut failures = Vec::new();
    let mut exercised = 0;
    let mut check = |label: String, predicate, matches| {
        let query = Query::from("metrics").filter(predicate);
        let prepared = db
            .prepare_query(&query)
            .unwrap_or_else(|error| panic!("{label}: prepare numeric query: {error:?}"));
        let identities = block_on(db.all(&prepared, local()))
            .unwrap_or_else(|error| panic!("{label}: read numeric query: {error:?}"))
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>();
        let expected = if matches { vec![metric] } else { vec![] };
        println!("{label}: expected={expected:?}, actual={identities:?}");
        exercised += 1;
        if identities != expected {
            failures.push((label, expected, identities));
        }
    };
    for (label, left, right, left_column, right_column, outcomes) in pairs {
        for ((name, compare), matches) in operators.into_iter().zip(outcomes) {
            for (left_present, right_present) in
                [(false, false), (true, false), (false, true), (true, true)]
            {
                check(
                    format!("{label} {name} literals present=({left_present},{right_present})"),
                    compare(float(left, left_present), float(right, right_present)),
                    matches,
                );
                if left.is_nan() || right.is_nan() {
                    check(
                        format!(
                            "Not {label} {name} literals present=({left_present},{right_present})"
                        ),
                        not(compare(
                            float(left, left_present),
                            float(right, right_present),
                        )),
                        true,
                    );
                }
            }
            if !left_column.is_empty() {
                check(
                    format!("{label} {name} field/literal"),
                    compare(col(left_column), float(right, false)),
                    matches,
                );
            }
            if !right_column.is_empty() {
                check(
                    format!("{label} {name} literal/field"),
                    compare(float(left, false), col(right_column)),
                    matches,
                );
            }
        }
    }
    for ((name, compare), (forward, reverse)) in
        operators.into_iter().zip(less.into_iter().zip(greater))
    {
        for present in [false, true] {
            check(
                format!("nullable Double {name} field/literal present={present}"),
                compare(col("optional_score"), float(-1.0, present)),
                forward,
            );
            check(
                format!("nullable Double {name} literal/field present={present}"),
                compare(float(-1.0, present), col("optional_score")),
                reverse,
            );
        }
    }
    let controls = [
        (
            "integer ordering",
            lt(lit(Value::I32(-2)), lit(Value::I32(-1))),
            true,
        ),
        (
            "integer equality",
            eq(lit(Value::I32(1)), lit(Value::I32(2))),
            false,
        ),
        ("text ordering", lt(lit("a"), lit("b")), true),
        ("boolean inequality", ne(lit(true), lit(false)), true),
        (
            "null equality",
            eq(lit(Value::Nullable(None)), lit(Value::Nullable(None))),
            true,
        ),
        (
            "null inequality",
            ne(lit(Value::Nullable(None)), lit(Value::Nullable(None))),
            false,
        ),
        ("null Double column", is_null(col("null_score")), true),
        (
            "present Double column",
            is_null(col("optional_score")),
            false,
        ),
        (
            "present integer equality",
            eq(
                lit(Value::Nullable(Some(Box::new(Value::I32(1))))),
                lit(Value::Nullable(Some(Box::new(Value::I32(1))))),
            ),
            true,
        ),
        (
            "In signed zero",
            in_list(float(-0.0, false), [float(1.0, false), float(0.0, false)]),
            true,
        ),
        (
            "In mixed signed zero",
            in_list(float(0.0, true), [float(-0.0, false)]),
            true,
        ),
        (
            "In absent value",
            in_list(float(-2.0, false), [float(-1.0, false), float(0.0, false)]),
            false,
        ),
        (
            "negated true negative ordering",
            not(lt(float(-2.0, false), float(-1.0, false))),
            false,
        ),
        (
            "negated false cross-sign ordering",
            not(gt(float(-1.0, false), float(0.0, false))),
            true,
        ),
        (
            "negated In unordered-only",
            not(in_list(float(f64::NAN, true), [float(1.0, false)])),
            true,
        ),
        (
            "negated In unordered and nonmatching",
            not(in_list(
                float(1.0, false),
                [float(f64::NAN, true), float(2.0, false)],
            )),
            true,
        ),
        (
            "negated In unordered and matching",
            not(in_list(
                float(1.0, true),
                [float(f64::NAN, false), float(1.0, false)],
            )),
            false,
        ),
        (
            "negated In signed-zero matching",
            not(in_list(float(-0.0, true), [float(0.0, false)])),
            false,
        ),
        (
            "negated In empty options",
            not(in_list(float(f64::NAN, true), [])),
            true,
        ),
    ];
    for (label, predicate, matches) in controls {
        check(label.to_owned(), predicate, matches);
    }
    println!(
        "exercised {exercised} prepared memberships; {} mismatches",
        failures.len()
    );
    assert!(
        failures.is_empty(),
        "prepared float predicates must preserve numeric semantics: {failures:#?}"
    );
}
