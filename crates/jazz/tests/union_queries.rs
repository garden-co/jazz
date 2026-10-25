//! Public unions on SQLite (`app.union([...])`, `.union({ a, b })`): a
//! relation query whose body is a `UNION ALL` of arms, ordered and windowed
//! as a whole. Each arm reads the output table under its own read policy
//! (an arm never widens access), a row matched by several arms is one
//! result row per arm, and that row's occurrence carries its arm, so
//! maintained results add and remove each arm's occurrence on its own.
//! Synced unions attach on the server as the union and ship every arm's
//! rows.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;

use common::{allow_all_policies, compile_schema, read_and_allow_all_writes, session_eq};
use duplex_transport::duplex;
use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, ReadTier, SubscriptionEvent,
    SubscriptionStream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, RelationOrderBy, RelationQuery};
use jazz::records::Value as CoreValue;
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{
    RelationCmpOp, RelationColumnRef, RelationExpr, RelationPredicate, RelationProjectColumn,
    RelationProjectExpr, RelationRowIdRef, RelationUnionArm, RelationValueRef,
};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder, Value};

fn schema(read: Option<PolicyExpr>) -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .column("done", ColumnType::Boolean)
                    .column("owner", ColumnType::Text)
                    .policies(match read {
                        Some(read) => read_and_allow_all_writes(read),
                        None => allow_all_policies(),
                    }),
            )
            .build(),
    )
}

fn open(schema: JazzSchema, node: u8) -> Db {
    let config = DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    );
    if node == 0 {
        block_on(Db::open_history_complete(config))
    } else {
        block_on(Db::open(config))
    }
    .unwrap()
}

fn todo_id(n: u8) -> RowUuid {
    RowUuid::from_bytes([1, n, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
}

fn insert(db: &Db, n: u8, title: &str, done: bool, owner: &str) {
    let cells = BTreeMap::from([
        ("title".to_owned(), CoreValue::String(title.to_owned())),
        ("done".to_owned(), CoreValue::Bool(done)),
        ("owner".to_owned(), CoreValue::String(owner.to_owned())),
    ]);
    block_on(db.insert(
        "todos",
        cells,
        InsertOptions {
            row_id: Some(todo_id(n)),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn set_done(db: &Db, n: u8, done: bool) {
    block_on(db.update(
        "todos",
        todo_id(n),
        BTreeMap::from([("done".to_owned(), CoreValue::Bool(done))]),
        Default::default(),
    ))
    .unwrap();
}

fn todos_column(column: &str) -> RelationColumnRef {
    RelationColumnRef {
        scope: Some("todos".to_owned()),
        column: column.to_owned(),
    }
}

/// `todos.where({ column: value })` as the TS builder sends it: a filter
/// under a full-row projection.
fn todos_where(column: &str, value: Value) -> RelationExpr {
    let columns = ["id", "title", "done", "owner"]
        .iter()
        .map(|alias| RelationProjectColumn {
            alias: (*alias).to_owned(),
            expr: if *alias == "id" {
                RelationProjectExpr::RowId(RelationRowIdRef::Current)
            } else {
                RelationProjectExpr::Column(todos_column(alias))
            },
        })
        .collect();
    RelationExpr::Project {
        input: Box::new(RelationExpr::Filter {
            input: Box::new(RelationExpr::TableScan {
                table: "todos".to_owned(),
                alias: None,
            }),
            predicate: RelationPredicate::Cmp {
                left: todos_column(column),
                op: RelationCmpOp::Eq,
                right: RelationValueRef::literal(value),
            },
        }),
        columns,
    }
}

fn not_done() -> RelationExpr {
    todos_where("done", Value::Boolean(false))
}

fn titled(title: &str) -> RelationExpr {
    todos_where("title", Value::Text(title.to_owned()))
}

/// `union(arms).orderBy(order).offset(offset).limit(limit)`.
fn union(
    arms: Vec<(&str, RelationExpr)>,
    order: Option<(&str, OrderDirection)>,
    offset: Option<usize>,
    limit: Option<usize>,
) -> Query {
    let mut rel = RelationExpr::Union {
        inputs: arms
            .into_iter()
            .map(|(label, input)| RelationUnionArm {
                label: label.to_owned(),
                input,
            })
            .collect(),
    };
    if let Some((column, direction)) = order {
        rel = RelationExpr::OrderBy {
            input: Box::new(rel),
            terms: vec![RelationOrderBy {
                column: todos_column(column),
                direction,
            }],
        };
    }
    if let Some(offset) = offset {
        rel = RelationExpr::Offset {
            input: Box::new(rel),
            offset,
        };
    }
    if let Some(limit) = limit {
        rel = RelationExpr::Limit {
            input: Box::new(rel),
            limit,
        };
    }
    let mut query = Query::from("todos");
    query.relation = Some(RelationQuery { rel });
    query
}

fn title(row: &CurrentRow) -> String {
    match row.cells().find(|(name, _)| *name == "title") {
        Some((_, Some(CoreValue::String(title)))) => title.clone(),
        other => panic!("no title: {other:?}"),
    }
}

/// (title, arm) of each row.
fn shown(rows: &[CurrentRow]) -> Vec<(String, String)> {
    rows.iter()
        .map(|row| (title(row), row.union_arm().unwrap_or_default().to_owned()))
        .collect()
}

fn pair(title: &str, arm: &str) -> (String, String) {
    (title.to_owned(), arm.to_owned())
}

fn read(db: &Db, query: &Query) -> Vec<CurrentRow> {
    block_on(db.all(&db.prepare_query(query).unwrap(), ReadOpts::default())).unwrap()
}

fn seed(db: &Db) {
    insert(db, 1, "alpha", false, "ada");
    insert(db, 2, "beta", false, "bob");
    insert(db, 3, "gamma", true, "ada");
}

#[test]
fn a_union_reads_every_arms_rows_with_their_arm() {
    let db = open(schema(None), 1);
    seed(&db);
    let arms = || vec![("open", not_done()), ("alpha", titled("alpha"))];

    // A row two arms match is a row per arm; ties break by id, then arm.
    let all = read(
        &db,
        &union(arms(), Some(("title", OrderDirection::Asc)), None, None),
    );
    assert_eq!(
        shown(&all),
        [
            pair("alpha", "alpha"),
            pair("alpha", "open"),
            pair("beta", "open")
        ]
    );
    assert_eq!(all[0].row_uuid(), all[1].row_uuid());
    assert_ne!(all[0].occurrence_key(), all[1].occurrence_key());

    // The window applies to the whole union, across the duplicate.
    let page = read(
        &db,
        &union(
            arms(),
            Some(("title", OrderDirection::Asc)),
            Some(1),
            Some(2),
        ),
    );
    assert_eq!(shown(&page), [pair("alpha", "open"), pair("beta", "open")]);
    let first = read(
        &db,
        &union(arms(), Some(("title", OrderDirection::Asc)), None, Some(2)),
    );
    assert_eq!(
        shown(&first),
        [pair("alpha", "alpha"), pair("alpha", "open")]
    );

    // Descending order breaks ties descending too.
    let desc = read(
        &db,
        &union(arms(), Some(("title", OrderDirection::Desc)), None, None),
    );
    assert_eq!(
        shown(&desc),
        [
            pair("beta", "open"),
            pair("alpha", "open"),
            pair("alpha", "alpha")
        ]
    );

    // Repeated arms under different labels stay apart.
    let repeated = read(
        &db,
        &union(
            vec![("first", not_done()), ("second", not_done())],
            Some(("title", OrderDirection::Asc)),
            None,
            Some(2),
        ),
    );
    assert_eq!(
        shown(&repeated),
        [pair("alpha", "first"), pair("alpha", "second")]
    );
}

#[test]
fn union_arm_labels_must_be_unique_and_unions_do_not_nest() {
    let db = open(schema(None), 1);
    let duplicate = db
        .prepare_query(&union(
            vec![("a", not_done()), ("a", titled("alpha"))],
            None,
            None,
            None,
        ))
        .expect_err("duplicate labels are refused");
    assert!(duplicate.to_string().contains("unique"), "{duplicate}");

    let inner = RelationExpr::Union {
        inputs: vec![RelationUnionArm {
            label: "inner".to_owned(),
            input: not_done(),
        }],
    };
    let nested = db
        .prepare_query(&union(vec![("outer", inner)], None, None, None))
        .expect_err("a nested union is refused");
    assert!(nested.to_string().contains("not supported yet"), "{nested}");
}

/// Every arm is read through the table's read policy: an arm that names
/// rows the reader may not read does not list them.
#[test]
fn every_union_arm_applies_the_read_policy() {
    let db = open(
        schema(Some(session_eq("owner", &["user", "identity", "subject"]))),
        1,
    );
    let reader = AuthorSubject::for_test_bytes([0xb0; 16]);
    let me = reader.principal_parts().1;
    insert(&db, 1, "visible", false, &me);
    insert(&db, 2, "hidden", true, "someone else");
    let query = union(
        vec![
            ("all", todos_where("done", Value::Boolean(false))),
            ("hidden", titled("hidden")),
        ],
        None,
        None,
        None,
    );
    let prepared = db.prepare_query(&query).unwrap();
    let rows = block_on(db.all_for_identity(&prepared, ReadOpts::default(), reader)).unwrap();
    assert_eq!(shown(&rows), [pair("visible", "all")]);
    // Unrestricted, the second arm lists the hidden row.
    assert_eq!(read(&db, &query).len(), 2);
}

/// Large signed integers compare exactly in every arm.
#[test]
fn union_arms_compare_out_of_safe_range_bigints_exactly() {
    let db = open(
        compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("metrics")
                        .column("label", ColumnType::Text)
                        .column("value", ColumnType::BigInt)
                        .policies(allow_all_policies()),
                )
                .build(),
        ),
        1,
    );
    let positive = 9_007_199_254_740_993_i64;
    for (n, label, value) in [
        (1, "positive", positive),
        (2, "negative", -positive),
        (3, "nearby", positive - 1),
    ] {
        block_on(db.insert(
            "metrics",
            BTreeMap::from([
                ("label".to_owned(), CoreValue::String(label.to_owned())),
                ("value".to_owned(), CoreValue::I64(value)),
            ]),
            InsertOptions {
                row_id: Some(todo_id(n)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    let arm = |value: i64| {
        let columns = ["id", "label", "value"]
            .iter()
            .map(|alias| RelationProjectColumn {
                alias: (*alias).to_owned(),
                expr: if *alias == "id" {
                    RelationProjectExpr::RowId(RelationRowIdRef::Current)
                } else {
                    RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("metrics".to_owned()),
                        column: (*alias).to_owned(),
                    })
                },
            })
            .collect();
        RelationExpr::Project {
            input: Box::new(RelationExpr::Filter {
                input: Box::new(RelationExpr::TableScan {
                    table: "metrics".to_owned(),
                    alias: None,
                }),
                predicate: RelationPredicate::Cmp {
                    left: RelationColumnRef {
                        scope: Some("metrics".to_owned()),
                        column: "value".to_owned(),
                    },
                    op: RelationCmpOp::Eq,
                    right: RelationValueRef::literal(Value::BigInt(value)),
                },
            }),
            columns,
        }
    };
    let mut query = Query::from("metrics");
    query.relation = Some(RelationQuery {
        rel: RelationExpr::OrderBy {
            input: Box::new(RelationExpr::Union {
                inputs: vec![
                    RelationUnionArm {
                        label: "positive".to_owned(),
                        input: arm(positive),
                    },
                    RelationUnionArm {
                        label: "negative".to_owned(),
                        input: arm(-positive),
                    },
                ],
            }),
            terms: vec![RelationOrderBy {
                column: RelationColumnRef {
                    scope: Some("metrics".to_owned()),
                    column: "label".to_owned(),
                },
                direction: OrderDirection::Asc,
            }],
        },
    });
    let labels: Vec<String> = read(&db, &query)
        .iter()
        .map(|row| match row.cells().find(|(name, _)| *name == "label") {
            Some((_, Some(CoreValue::String(label)))) => label.clone(),
            other => panic!("no label: {other:?}"),
        })
        .collect();
    assert_eq!(labels, ["negative", "positive"]);
}

fn drain(stream: &mut SubscriptionStream) -> Vec<SubscriptionEvent> {
    std::iter::from_fn(|| stream.try_next_event()).collect()
}

/// The (title, arm) rows a subscription shows after applying its events.
fn apply(rows: &mut Vec<(RowUuid, String, String)>, events: Vec<SubscriptionEvent>) {
    for event in events {
        let SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            ..
        } = event
        else {
            continue;
        };
        if reset {
            rows.clear();
        }
        for gone in removed {
            let at = rows
                .iter()
                .position(|(id, _, arm)| {
                    *id == gone.row_uuid && Some(arm.as_str()) == gone.union_arm.as_deref()
                })
                .expect("a removed occurrence was shown");
            rows.remove(at);
        }
        for row in updated {
            let at = rows
                .iter()
                .position(|(id, _, arm)| {
                    *id == row.row.row_uuid() && Some(arm.as_str()) == row.row.union_arm()
                })
                .expect("an updated occurrence was shown");
            rows.remove(at);
            rows.insert(
                row.index,
                (
                    row.row.row_uuid(),
                    title(&row.row),
                    row.row.union_arm().unwrap().to_owned(),
                ),
            );
        }
        for row in added {
            rows.insert(
                row.index,
                (
                    row.row.row_uuid(),
                    title(&row.row),
                    row.row.union_arm().unwrap().to_owned(),
                ),
            );
        }
    }
}

fn titles_and_arms(rows: &[(RowUuid, String, String)]) -> Vec<(String, String)> {
    rows.iter()
        .map(|(_, title, arm)| (title.clone(), arm.clone()))
        .collect()
}

/// A live order window over a union: a row that leaves one arm removes
/// that arm's occurrence only, and a new first row pushes the last out.
#[test]
fn union_subscriptions_maintain_each_arms_occurrences() {
    let db = open(schema(None), 1);
    seed(&db);
    let query = union(
        vec![("open", not_done()), ("alpha", titled("alpha"))],
        Some(("title", OrderDirection::Asc)),
        None,
        Some(2),
    );
    let prepared = db.prepare_query(&query).unwrap();
    let mut stream = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    let mut rows = Vec::new();
    apply(&mut rows, drain(&mut stream));
    assert_eq!(
        titles_and_arms(&rows),
        [pair("alpha", "alpha"), pair("alpha", "open")]
    );

    set_done(&db, 1, true);
    apply(&mut rows, drain(&mut stream));
    assert_eq!(
        titles_and_arms(&rows),
        [pair("alpha", "alpha"), pair("beta", "open")]
    );

    insert(&db, 4, "aardvark", false, "ada");
    apply(&mut rows, drain(&mut stream));
    assert_eq!(
        titles_and_arms(&rows),
        [pair("aardvark", "open"), pair("alpha", "alpha")]
    );
    assert_eq!(
        titles_and_arms(&rows),
        shown(&read(&db, &query)),
        "the maintained result is the one-shot read"
    );
}

fn pump(server: &Db, client: &Db) {
    for _ in 0..4 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
}

/// A synced union: the server serves it under the client's read policy,
/// ships every arm's rows, and the client's result follows the server's,
/// windowed by the union's offset over the prefix it holds.
#[test]
fn a_synced_union_ships_every_arms_rows_under_the_readers_policy() {
    let server = open(
        schema(Some(session_eq("owner", &["user", "identity", "subject"]))),
        0,
    );
    let reader = AuthorSubject::for_test_bytes([0xb0; 16]);
    let me = reader.principal_parts().1;
    insert(&server, 1, "alpha", false, &me);
    insert(&server, 2, "beta", false, &me);
    insert(&server, 3, "gamma", true, "someone else");
    insert(&server, 4, "delta", true, &me);
    let client = open(
        schema(Some(session_eq("owner", &["user", "identity", "subject"]))),
        1,
    );
    let (client_side, server_side) = duplex();
    let _ = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, reader);

    let query = union(
        vec![
            ("open", not_done()),
            ("gamma", titled("gamma")),
            ("delta", titled("delta")),
        ],
        Some(("title", OrderDirection::Asc)),
        Some(1),
        Some(2),
    );
    let prepared = client.prepare_query(&query).unwrap();
    let mut stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    let mut rows = Vec::new();
    apply(&mut rows, drain(&mut stream));
    // alpha, beta, delta are readable; the page skips alpha.
    assert_eq!(
        titles_and_arms(&rows),
        [pair("beta", "open"), pair("delta", "delta")]
    );
    let local = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    let held =
        block_on(client.all(&client.prepare_query(&Query::from("todos")).unwrap(), local)).unwrap();
    let mut held: Vec<String> = held.iter().map(title).collect();
    held.sort();
    assert_eq!(held, ["alpha", "beta", "delta"], "gamma is not readable");

    insert(&server, 5, "aardvark", false, &me);
    pump(&server, &client);
    apply(&mut rows, drain(&mut stream));
    assert_eq!(
        titles_and_arms(&rows),
        [pair("alpha", "open"), pair("beta", "open")]
    );
}

/// An arm's own order and limit pick its rows before the union.
#[test]
fn a_union_arms_own_window_applies_before_the_union() {
    let db = open(schema(None), 1);
    seed(&db);
    insert(&db, 4, "aardvark", false, "ada");
    let first_open = RelationExpr::Limit {
        input: Box::new(RelationExpr::OrderBy {
            input: Box::new(not_done()),
            terms: vec![RelationOrderBy {
                column: todos_column("title"),
                direction: OrderDirection::Desc,
            }],
        }),
        limit: 1,
    };
    let rows = read(
        &db,
        &union(
            vec![("last open", first_open), ("gamma", titled("gamma"))],
            Some(("title", OrderDirection::Asc)),
            None,
            None,
        ),
    );
    assert_eq!(
        shown(&rows),
        [pair("beta", "last open"), pair("gamma", "gamma")]
    );
}

/// Titles past the inline sort prefix that share it order by their whole
/// value, in a union as in a plain read.
#[test]
fn a_union_orders_long_values_sharing_their_sort_prefix() {
    let db = open(schema(None), 1);
    let prefix = "p".repeat(5000);
    insert(&db, 1, &format!("{prefix}c"), false, "ada");
    insert(&db, 2, &format!("{prefix}a"), false, "ada");
    insert(&db, 3, &format!("{prefix}b"), true, "ada");
    let rows = read(
        &db,
        &union(
            vec![
                ("open", not_done()),
                ("done", todos_where("done", Value::Boolean(true))),
            ],
            Some(("title", OrderDirection::Asc)),
            None,
            None,
        ),
    );
    let tails: Vec<String> = rows
        .iter()
        .map(|row| title(row)[prefix.len()..].to_owned())
        .collect();
    assert_eq!(tails, ["a", "b", "c"]);
}
