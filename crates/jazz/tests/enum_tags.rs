//! Enum columns are stored as INTEGER tags, which each store keeps for
//! itself in `_jazz_enum_tags`, numbering each stored column's variants in
//! the order they were added. Everything above SQLite, sync included, uses
//! variant names; a column sorts in tag order.

use crate::common;

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, SubscriptionEvent, SyncMessage,
    Transport, TransportError, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{LensOp, MigrationLens, TableLens};
use jazz::query::{Operand, OrderDirection, Predicate, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn enumeration(variants: &[&str]) -> ColumnType {
    ColumnType::Enum {
        variants: variants.iter().map(|v| (*v).to_owned()).collect(),
    }
}

/// `tasks(title: text, <status>: enum(variants), labels: enum(bug, ui)[]?)`.
fn tasks_with(status: &str, column_type: ColumnType) -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("tasks")
                    .column("title", ColumnType::Text)
                    .column(status, column_type)
                    .nullable_column(
                        "labels",
                        ColumnType::Array {
                            element: Box::new(enumeration(&["bug", "ui"])),
                        },
                    )
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn tasks(variants: &[&str]) -> JazzSchema {
    tasks_with("status", enumeration(variants))
}

/// The first version: `todo`, `doing`, `done`.
fn v1() -> JazzSchema {
    tasks(&["todo", "doing", "done"])
}

/// v1 plus `blocked`, declared first.
fn v2() -> JazzSchema {
    tasks(&["blocked", "todo", "doing", "done"])
}

fn lens(source: &JazzSchema, target: &JazzSchema, ops: Vec<LensOp>) -> MigrationLens {
    MigrationLens::new(
        source.version_id(),
        target.version_id(),
        vec![TableLens {
            source_table: "tasks".into(),
            target_table: "tasks".into(),
            ops,
        }],
    )
    .unwrap()
}

fn config(schema: JazzSchema, node: u8) -> DbConfig {
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

fn open_at(schema: JazzSchema, node: u8, path: &Path) -> Db {
    block_on(Db::open_at(
        config(schema, node),
        path,
        FileOptions::default(),
    ))
    .unwrap()
}

fn authority_at(schema: JazzSchema, path: &Path) -> Db {
    let db = open_at(schema, 0, path);
    db.set_authority();
    db
}

struct Pipe {
    out: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
}

impl Transport for Pipe {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.out.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

/// `client` connected to `server`, which serves it as SYSTEM.
fn link(client: &Db, server: &Db) -> jazz::db::Upstream {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let link = block_on(client.connect_upstream(Box::new(Pipe {
        out: up.clone(),
        inbound: down.clone(),
    })));
    server.accept_subscriber(
        Box::new(Pipe {
            out: down,
            inbound: up,
        }),
        AuthorSubject::SYSTEM,
    );
    link
}

fn pump(server: &Db, clients: &[&Db]) {
    for _ in 0..6 {
        for client in clients {
            block_on(client.tick()).unwrap();
        }
        block_on(server.tick()).unwrap();
    }
    for client in clients {
        block_on(client.tick()).unwrap();
    }
}

fn name(value: &str) -> Value {
    Value::String(value.to_owned())
}

fn labels(items: &[&str]) -> Value {
    Value::Nullable(Some(Box::new(Value::Array(
        items.iter().map(|item| name(item)).collect(),
    ))))
}

fn try_insert(db: &Db, n: u8, cells: &[(&str, Value)]) -> Result<(), jazz::db::Error> {
    block_on(
        db.insert(
            "tasks",
            cells
                .iter()
                .map(|(column, value)| ((*column).to_owned(), value.clone()))
                .collect::<BTreeMap<_, _>>(),
            InsertOptions {
                row_id: Some(RowUuid::from_bytes([n; 16])),
                ..Default::default()
            },
        ),
    )
    .map(|_| ())
}

fn insert(db: &Db, n: u8, status: &str) {
    try_insert(
        db,
        n,
        &[
            ("title", name(&format!("task {n}"))),
            ("status", name(status)),
        ],
    )
    .unwrap();
}

/// `(row byte, column)` for each row `query` returns, in its order.
fn column(db: &Db, query: Query, column: &str) -> Vec<(u8, Option<Value>)> {
    let prepared = db.prepare_query(&query).unwrap();
    block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|row| {
            let value = row
                .cells()
                .find(|(name, _)| *name == column)
                .and_then(|(_, value)| value.cloned());
            (row.row_uuid().as_bytes()[0], value)
        })
        .collect()
}

fn statuses(db: &Db, query: Query) -> Vec<(u8, String)> {
    column(db, query, "status")
        .into_iter()
        .map(|(n, value)| match value {
            Some(Value::String(name)) => (n, name),
            other => panic!("row {n}: status {other:?}"),
        })
        .collect()
}

fn ids(db: &Db, query: Query) -> Vec<u8> {
    let mut ids: Vec<u8> = column(db, query, "title")
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    ids.sort();
    ids
}

fn by_status() -> Query {
    Query::from("tasks").order_by("status", OrderDirection::Asc)
}

fn status_is(value: &str) -> Predicate {
    Predicate::Eq(
        Operand::Column("status".into()),
        Operand::Literal(name(value)),
    )
}

/// The raw catalogue rows for `tasks.status` (by its stored names), as
/// `(tag, name)` in tag order.
fn raw_tags(path: &Path) -> Vec<(i64, String)> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT t.tag, t.name FROM _jazz_enum_tags t \
             WHERE t.col NOT IN (SELECT col FROM _jazz_enum_tags WHERE name = 'bug') \
             ORDER BY t.tag",
        )
        .unwrap();
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// The raw stored `status` cells of the current rows, as
/// `(SQLite type, cell as text)`, sorted.
fn raw_cells(path: &Path) -> Vec<(String, String)> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let (table, col): (String, String) = conn
        .query_row(
            "SELECT tbl, col FROM _jazz_enum_tags WHERE name = 'todo' LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let sql =
        format!("SELECT typeof(\"{col}\"), CAST(\"{col}\" AS TEXT) FROM \"{table}\" ORDER BY 2");
    let mut stmt = conn.prepare(&sql).unwrap();
    stmt.query_map([], |row| {
        Ok((
            row.get(0)?,
            row.get::<_, Option<String>>(1)?.unwrap_or_default(),
        ))
    })
    .unwrap()
    .collect::<Result<_, _>>()
    .unwrap()
}

fn integers(tags: &[i64]) -> Vec<(String, String)> {
    let mut cells: Vec<_> = tags
        .iter()
        .map(|tag| ("integer".to_owned(), tag.to_string()))
        .collect();
    cells.sort_by(|a, b| a.1.cmp(&b.1));
    cells
}

/// Tags follow declaration order; cells hold tags, reads return names,
/// filters take names, and ORDER BY follows tag order, not the alphabet.
#[test]
fn an_authority_stores_tags_in_declaration_order_and_reads_names() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("authority.sqlite");
    let db = authority_at(v1(), &path);
    insert(&db, 1, "done");
    insert(&db, 2, "todo");
    insert(&db, 3, "doing");

    assert_eq!(
        raw_tags(&path),
        [(0, "todo".into()), (1, "doing".into()), (2, "done".into())]
    );
    assert_eq!(raw_cells(&path), integers(&[0, 1, 2]));
    assert_eq!(
        statuses(&db, by_status()),
        [(2, "todo".into()), (3, "doing".into()), (1, "done".into())]
    );
    assert_eq!(
        statuses(
            &db,
            Query::from("tasks").order_by("status", OrderDirection::Desc)
        ),
        [(1, "done".into()), (3, "doing".into()), (2, "todo".into())]
    );
    assert_eq!(
        ids(&db, Query::from("tasks").filter(status_is("doing"))),
        [3]
    );
    assert_eq!(
        ids(
            &db,
            Query::from("tasks").filter(Predicate::Ne(
                Operand::Column("status".into()),
                Operand::Literal(name("doing")),
            ))
        ),
        [1, 2]
    );
    assert_eq!(
        ids(
            &db,
            Query::from("tasks").filter(Predicate::In(
                Operand::Column("status".into()),
                vec![
                    Operand::Literal(name("todo")),
                    Operand::Literal(name("done"))
                ],
            ))
        ),
        [1, 2]
    );
    // A name that is no variant matches nothing.
    assert_eq!(
        ids(&db, Query::from("tasks").filter(status_is("nope"))),
        Vec::<u8>::new()
    );

    // Reopening keeps the tags and reads the same.
    drop(db);
    let db = authority_at(v1(), &path);
    insert(&db, 4, "doing");
    assert_eq!(raw_cells(&path), integers(&[0, 1, 1, 2]));
    assert_eq!(
        statuses(&db, by_status()),
        [
            (2, "todo".into()),
            (3, "doing".into()),
            (4, "doing".into()),
            (1, "done".into())
        ]
    );
}

/// A variant added later gets the next tag and sorts after the earlier
/// ones, wherever it is declared; a removed variant keeps its tag, which is
/// never handed to another one.
#[test]
fn added_variants_sort_last_and_removed_tags_are_never_reused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("authority.sqlite");
    let db = authority_at(v1(), &path);
    insert(&db, 1, "done");
    insert(&db, 2, "todo");
    block_on(db.activate_schema_with_lenses(v2(), &[lens(&v1(), &v2(), vec![])])).unwrap();
    insert(&db, 3, "blocked");
    assert_eq!(
        statuses(&db, by_status()),
        [
            (2, "todo".into()),
            (1, "done".into()),
            (3, "blocked".into())
        ]
    );

    // v3 drops `doing` and adds `review`.
    let v3 = tasks(&["blocked", "todo", "review", "done"]);
    block_on(db.activate_schema_with_lenses(v3.clone(), &[lens(&v2(), &v3, vec![])])).unwrap();
    insert(&db, 4, "review");
    assert_eq!(
        raw_tags(&path),
        [
            (0, "todo".into()),
            (1, "doing".into()),
            (2, "done".into()),
            (3, "blocked".into()),
            (4, "review".into())
        ]
    );
    assert_eq!(raw_cells(&path), integers(&[0, 2, 3, 4]));
    // `doing` is no variant of v3 any more.
    let refused =
        try_insert(&db, 5, &[("title", name("t")), ("status", name("doing"))]).unwrap_err();
    assert!(
        refused.to_string().contains("not one of its variants"),
        "{refused}"
    );
}

/// A replica tags variants itself, as it admits versions: rows travel by
/// variant name, and a version the replica admits first tags its new
/// variant there at once.
#[test]
fn a_replica_tags_variants_itself_and_rows_travel_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(v1(), &dir.path().join("server.sqlite"));
    insert(&server, 1, "done");
    let client_path = dir.path().join("client.sqlite");
    let client = open_at(v1(), 1, &client_path);
    let _link = link(&client, &server);
    let q = client.prepare_query(&Query::from("tasks")).unwrap();
    let _sub = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    insert(&client, 2, "todo");
    pump(&server, &[&client]);

    // A version only the client has admitted: it tags `blocked` itself.
    block_on(client.admit_schema_with_lenses(v2(), &[lens(&v1(), &v2(), vec![])])).unwrap();
    let ahead = block_on(client.register_schema_view(v2())).unwrap();
    try_insert(
        &ahead,
        3,
        &[("title", name("t")), ("status", name("blocked"))],
    )
    .unwrap();
    assert_eq!(raw_cells(&client_path), integers(&[0, 2, 3]));
    assert_eq!(
        statuses(&ahead, by_status()),
        [
            (2, "todo".into()),
            (1, "done".into()),
            (3, "blocked".into())
        ]
    );

    // The authority admits v2 and takes the client's row by name.
    block_on(server.activate_schema_with_lenses(v2(), &[lens(&v1(), &v2(), vec![])])).unwrap();
    pump(&server, &[&client]);
    assert_eq!(
        statuses(&server, by_status()),
        [
            (2, "todo".into()),
            (1, "done".into()),
            (3, "blocked".into())
        ]
    );
    assert_eq!(raw_cells(&client_path), integers(&[0, 2, 3]));
}

/// A write is checked against its version's variants, and an enum value
/// is never a large value.
#[test]
fn undeclared_and_oversized_variants_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = authority_at(v1(), &dir.path().join("authority.sqlite"));
    let refused =
        try_insert(&db, 1, &[("title", name("t")), ("status", name("later"))]).unwrap_err();
    assert!(
        refused.to_string().contains("not one of its variants"),
        "{refused}"
    );
    let huge = "x".repeat(70 * 1024);
    let refused = try_insert(&db, 2, &[("title", name("t")), ("status", name(&huge))]).unwrap_err();
    assert!(refused.to_string().contains("not one of its variants"));
    let refused = try_insert(
        &db,
        3,
        &[
            ("title", name("t")),
            ("status", name("todo")),
            ("labels", labels(&["bug", "feature"])),
        ],
    )
    .unwrap_err();
    assert!(
        refused.to_string().contains("not one of its variants"),
        "{refused}"
    );
}

/// Arrays of enums hold tags too, and `contains` takes a name.
#[test]
fn arrays_of_enums_store_tags_and_search_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("authority.sqlite");
    let db = authority_at(v1(), &path);
    for (n, items) in [(1, &["ui"][..]), (2, &["bug", "ui"][..]), (3, &[][..])] {
        try_insert(
            &db,
            n,
            &[
                ("title", name("t")),
                ("status", name("todo")),
                ("labels", labels(items)),
            ],
        )
        .unwrap();
    }
    let conn = rusqlite::Connection::open(&path).unwrap();
    let (table, col): (String, String) = conn
        .query_row(
            "SELECT tbl, col FROM _jazz_enum_tags WHERE name = 'bug'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let mut stored: Vec<String> = conn
        .prepare(&format!("SELECT \"{col}\" FROM \"{table}\""))
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    stored.sort();
    assert_eq!(stored, ["[0,1]", "[1]", "[]"]);

    let contains = |item: &str| {
        Query::from("tasks").filter(Predicate::Contains(
            Operand::Column("labels".into()),
            Operand::Literal(name(item)),
        ))
    };
    assert_eq!(ids(&db, contains("ui")), [1, 2]);
    assert_eq!(ids(&db, contains("bug")), [2]);
    let read = column(
        &db,
        Query::from("tasks").filter(contains_bug_only()),
        "labels",
    );
    assert_eq!(read, [(2, Some(labels(&["bug", "ui"])))]);
}

fn contains_bug_only() -> Predicate {
    Predicate::Contains(
        Operand::Column("labels".into()),
        Operand::Literal(name("bug")),
    )
}

/// A rename keeps the stored column and its tags; a copy gives the new
/// stored column the source's tags, so both read the same names.
#[test]
fn rename_and_copy_lenses_keep_the_tags() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("authority.sqlite");
    let db = authority_at(v1(), &path);
    insert(&db, 1, "doing");
    insert(&db, 2, "todo");

    let renamed = tasks_with("state", enumeration(&["todo", "doing", "done"]));
    block_on(db.activate_schema_with_lenses(
        renamed.clone(),
        &[lens(
            &v1(),
            &renamed,
            vec![LensOp::RenameColumn {
                from: "status".into(),
                to: "state".into(),
            }],
        )],
    ))
    .unwrap();
    insert_state(&db, 3, "done");
    assert_eq!(raw_cells(&path), integers(&[0, 1, 2]));
    let by_state = Query::from("tasks").order_by("state", OrderDirection::Asc);
    assert_eq!(
        column(&db, by_state, "state"),
        [
            (2, Some(name("todo"))),
            (1, Some(name("doing"))),
            (3, Some(name("done")))
        ]
    );
    let old = block_on(db.register_schema_view(v1())).unwrap();
    assert_eq!(
        ids(&old, Query::from("tasks").filter(status_is("done"))),
        [3]
    );

    // Copy `state` into a new `status`, alongside it.
    let copied = copied_schema();
    block_on(db.activate_schema_with_lenses(
        copied.clone(),
        &[lens(
            &renamed,
            &copied,
            vec![LensOp::CopyColumn {
                from: "state".into(),
                to: "status".into(),
            }],
        )],
    ))
    .unwrap();
    assert_eq!(
        statuses(&db, by_status()),
        [(2, "todo".into()), (1, "doing".into()), (3, "done".into())]
    );
    assert_eq!(
        ids(&db, Query::from("tasks").filter(status_is("doing"))),
        [1]
    );
}

/// `tasks(title, state, status: enum, labels)`.
fn copied_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("tasks")
                    .column("title", ColumnType::Text)
                    .column("state", enumeration(&["todo", "doing", "done"]))
                    .column("status", enumeration(&["todo", "doing", "done"]))
                    .nullable_column(
                        "labels",
                        ColumnType::Array {
                            element: Box::new(enumeration(&["bug", "ui"])),
                        },
                    )
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn insert_state(db: &Db, n: u8, state: &str) {
    try_insert(db, n, &[("title", name("t")), ("state", name(state))]).unwrap();
}

/// Tags are never read as text or text as tags: activating a version that
/// changes a column between enum and text is refused, in place and after a
/// drop; the store keeps reading its enum after a reopen; and an app opened
/// with the column as text gets a stored column of its own.
#[test]
fn enum_and_text_never_share_a_stored_column() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("authority.sqlite");
    let db = authority_at(v1(), &path);
    insert(&db, 1, "doing");

    let text = tasks_with("status", ColumnType::Text);
    let refused = block_on(db.activate_schema(text.clone())).unwrap_err();
    assert!(refused.message.contains("changing the type"), "{refused}");

    let without = untracked();
    block_on(db.activate_schema(without.clone())).unwrap();
    let readd = lens(
        &without,
        &text,
        vec![LensOp::AddColumn {
            column: "status".into(),
            default: name("unknown"),
        }],
    );
    let refused = block_on(db.activate_schema_with_lenses(text.clone(), &[readd])).unwrap_err();
    assert!(refused.message.contains("changing the type"), "{refused}");

    // text -> enum, on a store that holds the column as text.
    let notes = tempfile::tempdir().unwrap();
    let texts = authority_at(text, &notes.path().join("text.sqlite"));
    try_insert(&texts, 1, &[("title", name("t")), ("status", name("todo"))]).unwrap();
    let refused = block_on(texts.activate_schema(v1())).unwrap_err();
    assert!(refused.message.contains("changing the type"), "{refused}");

    // Reopening agrees with the open store.
    drop(db);
    let db = authority_at(v1(), &path);
    assert_eq!(statuses(&db, by_status()), [(1, "doing".into())]);
    assert_eq!(raw_cells(&path), integers(&[1]));

    // An app opened with `status` as required text and no default is
    // refused: the stored rows have no text value for it.
    drop(db);
    let refused = block_on(Db::open_at(
        config(tasks_with("status", ColumnType::Text), 0),
        &path,
        FileOptions::default(),
    ))
    .err()
    .expect("a required retyped column without a default is refused");
    assert!(refused.message.contains("changing the type"), "{refused}");

    // Opened with `status` as nullable text (shipped ahead of a migration),
    // it gets a stored column of its own: the tags are not its values.
    let ahead = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("tasks")
                    .column("title", ColumnType::Text)
                    .nullable_column("status", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let db = authority_at(ahead, &path);
    assert_eq!(
        column(&db, Query::from("tasks"), "status"),
        [(1, Some(Value::Nullable(None)))]
    );
    try_insert(
        &db,
        2,
        &[
            ("title", name("t")),
            ("status", Value::Nullable(Some(Box::new(name("1"))))),
        ],
    )
    .unwrap();
    assert_eq!(
        ids(
            &db,
            Query::from("tasks").filter(Predicate::Eq(
                Operand::Column("status".into()),
                Operand::Literal(name("1")),
            ))
        ),
        [2]
    );
    assert_eq!(
        raw_cells(&path),
        [
            ("null".to_owned(), String::new()),
            ("integer".to_owned(), "1".to_owned())
        ]
    );
}

/// `tasks(title, labels)`: `status` dropped.
fn untracked() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("tasks")
                    .column("title", ColumnType::Text)
                    .nullable_column(
                        "labels",
                        ColumnType::Array {
                            element: Box::new(enumeration(&["bug", "ui"])),
                        },
                    )
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

/// A subscription filtered by a variant wakes when a matching row is
/// written, on the authority and on a replica.
#[test]
fn a_subscription_filtered_by_a_variant_sees_matching_writes() {
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(v1(), &dir.path().join("server.sqlite"));
    let client = open_at(v1(), 1, &dir.path().join("client.sqlite"));
    let _link = link(&client, &server);
    let q = client
        .prepare_query(&Query::from("tasks").filter(status_is("doing")))
        .unwrap();
    let mut stream = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    while stream.try_next_event().is_some() {}
    insert(&server, 1, "doing");
    insert(&server, 2, "todo");
    pump(&server, &[&client]);
    let mut woke = false;
    while let Some(event) = stream.try_next_event() {
        if !matches!(event, SubscriptionEvent::Rejected { .. }) {
            woke = true;
        }
    }
    assert!(woke, "the subscription sees the matching write");
    assert_eq!(
        ids(&client, Query::from("tasks").filter(status_is("doing"))),
        [1]
    );
}

/// `tasks(title, status: enum(todo, doing, done)?, labels)`, or with
/// `status` as a nullable boolean.
fn optional_status(enum_status: bool) -> JazzSchema {
    let status = if enum_status {
        enumeration(&["todo", "doing", "done"])
    } else {
        ColumnType::Boolean
    };
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("tasks")
                    .column("title", ColumnType::Text)
                    .nullable_column("status", status)
                    .nullable_column(
                        "labels",
                        ColumnType::Array {
                            element: Box::new(enumeration(&["bug", "ui"])),
                        },
                    )
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn some(value: &str) -> Value {
    Value::Nullable(Some(Box::new(name(value))))
}

fn status_is_some(value: &str) -> Predicate {
    Predicate::Eq(
        Operand::Column("status".into()),
        Operand::Literal(some(value)),
    )
}

/// The authority stores `status` under another stored name (it was a
/// boolean there first); a replica that stored it under its own name moves it to
/// the authority's while it mirrors the catalogue, and keeps its tags.
#[test]
fn a_replica_moving_its_column_to_the_authoritys_name_keeps_the_tags() {
    let dir = tempfile::tempdir().unwrap();
    let server_path = dir.path().join("server.sqlite");
    let server = authority_at(optional_status(false), &server_path);
    try_insert(
        &server,
        9,
        &[
            ("title", name("t")),
            ("status", Value::Nullable(Some(Box::new(Value::Bool(true))))),
        ],
    )
    .unwrap();
    drop(server);
    let server = authority_at(optional_status(true), &server_path);
    try_insert(
        &server,
        1,
        &[("title", name("t")), ("status", some("doing"))],
    )
    .unwrap();

    let client_path = dir.path().join("client.sqlite");
    let client = open_at(optional_status(true), 1, &client_path);
    let _link = link(&client, &server);
    let q = client.prepare_query(&Query::from("tasks")).unwrap();
    let _sub = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    assert_eq!(raw_tags(&client_path), raw_tags(&server_path));
    assert_eq!(
        ids(
            &client,
            Query::from("tasks").filter(status_is_some("doing"))
        ),
        [1]
    );
    try_insert(
        &client,
        2,
        &[("title", name("t")), ("status", some("todo"))],
    )
    .unwrap();
    assert_eq!(
        ids(&client, Query::from("tasks").filter(status_is_some("todo"))),
        [2]
    );
    pump(&server, &[&client]);
    // Its own write holds its tag.
    let conn = rusqlite::Connection::open(&client_path).unwrap();
    let todo: Option<i64> = conn
        .query_row(
            "SELECT \"status~2\" FROM tasks WHERE typeof(\"status~2\") = 'integer' AND title = 't' \
             AND \"status~2\" = 0",
            [],
            |row| row.get(0),
        )
        .ok();
    assert_eq!(todo, Some(0));
    // A catalogue sent again is taken as it is.
    drop(client);
    let client = open_at(optional_status(true), 1, &client_path);
    let _link = link(&client, &server);
    let q = client.prepare_query(&Query::from("tasks")).unwrap();
    let _sub = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    assert_eq!(ids(&client, Query::from("tasks")), [1, 2, 9]);
}

/// A copy has tags of its own: variants added later to the source and to
/// the copy get the same tag number in each, and a fresh replica reads
/// both, because it fills a copy by variant name.
#[test]
fn a_copy_and_its_source_tag_later_variants_independently() {
    let dir = tempfile::tempdir().unwrap();
    let server_path = dir.path().join("server.sqlite");
    let server = authority_at(v1(), &server_path);
    insert(&server, 1, "doing");
    let schema = |status: &[&str], copy: &[&str]| {
        compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("tasks")
                        .column("title", ColumnType::Text)
                        .column("status", enumeration(status))
                        .column("copy", enumeration(copy))
                        .nullable_column(
                            "labels",
                            ColumnType::Array {
                                element: Box::new(enumeration(&["bug", "ui"])),
                            },
                        )
                        .policies(allow_all_policies()),
                )
                .build(),
        )
    };
    let copied = schema(&["todo", "doing", "done"], &["todo", "doing", "done"]);
    block_on(server.activate_schema_with_lenses(
        copied.clone(),
        &[lens(
            &v1(),
            &copied,
            vec![LensOp::CopyColumn {
                from: "status".into(),
                to: "copy".into(),
            }],
        )],
    ))
    .unwrap();
    let v4 = schema(
        &["todo", "doing", "done", "later"],
        &["todo", "doing", "done", "urgent"],
    );
    block_on(server.activate_schema_with_lenses(v4.clone(), &[lens(&copied, &v4, vec![])]))
        .unwrap();
    try_insert(
        &server,
        2,
        &[
            ("title", name("t")),
            ("status", name("later")),
            ("copy", name("urgent")),
        ],
    )
    .unwrap();

    let client_path = dir.path().join("client.sqlite");
    let client = open_at(v1(), 1, &client_path);
    let _link = link(&client, &server);
    let q = client.prepare_query(&Query::from("tasks")).unwrap();
    let _sub = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    assert_eq!(ids(&client, Query::from("tasks")), [1, 2]);
    for db in [&server, &client] {
        let view = block_on(db.register_schema_view(v4.clone())).unwrap();
        assert_eq!(
            column(&view, Query::from("tasks"), "copy"),
            [(1, Some(name("doing"))), (2, Some(name("urgent")))]
        );
        assert_eq!(
            column(&view, Query::from("tasks"), "status"),
            [(1, Some(name("doing"))), (2, Some(name("later")))]
        );
    }
}

/// A client tags its variants before it ever syncs: its cells are tags and
/// sort in declaration order from the start, and syncing changes neither.
#[test]
fn a_client_tags_its_variants_before_it_ever_syncs() {
    let dir = tempfile::tempdir().unwrap();
    let client_path = dir.path().join("client.sqlite");
    let client = open_at(v1(), 1, &client_path);
    insert(&client, 1, "done");
    insert(&client, 2, "todo");
    insert(&client, 3, "doing");
    let declared = [(2, "todo".into()), (3, "doing".into()), (1, "done".into())];
    assert_eq!(raw_cells(&client_path), integers(&[0, 1, 2]));
    assert_eq!(statuses(&client, by_status()), declared);
    assert_eq!(
        ids(&client, Query::from("tasks").filter(status_is("todo"))),
        [2]
    );

    let server = authority_at(v1(), &dir.path().join("server.sqlite"));
    let _link = link(&client, &server);
    let q = client.prepare_query(&Query::from("tasks")).unwrap();
    let _sub = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    assert_eq!(raw_cells(&client_path), integers(&[0, 1, 2]));
    assert_eq!(statuses(&client, by_status()), declared);
}

/// The raw `status` cells of stored table `table`, as `(SQLite type, cell
/// as text)`, by the stored column `_jazz_names` gives it.
fn raw_cells_of(path: &Path, table: &str) -> Vec<(String, String)> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let (stored, col): (String, String) = conn
        .query_row(
            "SELECT stored_tbl, stored_col FROM _jazz_names WHERE tbl = ?1 AND col = 'status' LIMIT 1",
            [table],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let sql = format!("SELECT typeof(\"{col}\"), CAST(\"{col}\" AS TEXT) FROM \"{stored}\"");
    let mut stmt = conn.prepare(&sql).unwrap();
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// `tasks(title, status?, rank?)`.
fn small_schema(status: ColumnType, rank: Option<ColumnType>) -> JazzSchema {
    let mut t = TableSchemaBuilder::new("tasks")
        .column("title", ColumnType::Text)
        .nullable_column("status", status);
    if let Some(rank) = rank {
        t = t.nullable_column("rank", rank);
    }
    compile_schema(
        &SchemaBuilder::new()
            .table(t.policies(allow_all_policies()))
            .build(),
    )
}

/// The authority keeps an old enum column under the stored name a replica
/// moves its own column away from: each keeps its own tags, and the
/// replica takes the authority's later rows.
#[test]
fn a_replica_never_folds_another_columns_tags_into_a_moved_one() {
    let dir = tempfile::tempdir().unwrap();
    let sp = dir.path().join("server.sqlite");
    let arr = |v: &[&str]| ColumnType::Array {
        element: Box::new(enumeration(v)),
    };
    let server = authority_at(small_schema(enumeration(&["a", "b", "c"]), None), &sp);
    try_insert(&server, 9, &[("title", name("t")), ("status", some("b"))]).unwrap();
    drop(server);
    let b = small_schema(arr(&["a"]), None);
    let server = authority_at(b.clone(), &sp);
    try_insert(
        &server,
        1,
        &[("title", name("t")), ("status", labels(&["a"]))],
    )
    .unwrap();
    let cp = dir.path().join("client.sqlite");
    let client = open_at(b.clone(), 1, &cp);
    let _link = link(&client, &server);
    let q = client.prepare_query(&Query::from("tasks")).unwrap();
    let _sub = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    let c = small_schema(arr(&["a", "z"]), None);
    block_on(server.activate_schema_with_lenses(c.clone(), &[lens(&b, &c, vec![])])).unwrap();
    try_insert(
        &server,
        3,
        &[("title", name("t")), ("status", labels(&["z"]))],
    )
    .unwrap();
    pump(&server, &[&client]);
    assert_eq!(ids(&client, Query::from("tasks")), [1, 3, 9]);
}

/// Tags are each store's own: a client that tagged `rank`'s `x` 0 takes
/// rows from an authority that tags `y` 0, through a catalogue that moves
/// the client's `rank` to another stored column. The rows come by name;
/// the client tags `y` itself and both read the same values.
#[test]
fn each_store_keeps_its_own_tags() {
    let dir = tempfile::tempdir().unwrap();
    let e = || enumeration(&["todo", "doing", "done"]);
    let with_rank = |rank: &str| small_schema(e(), Some(enumeration(&[rank])));
    // Two authorities of one app: copies of one file, which then tag
    // `rank` differently, the second at another stored column.
    let sp = dir.path().join("server.sqlite");
    let server = authority_at(small_schema(e(), None), &sp);
    try_insert(
        &server,
        9,
        &[("title", name("t")), ("status", some("todo"))],
    )
    .unwrap();
    // Serving a catalogue mints the app's namespace, which the copy keeps.
    let other = open_at(small_schema(e(), None), 2, &dir.path().join("other.sqlite"));
    let other_link = link(&other, &server);
    pump(&server, &[&other]);
    drop((other_link, other, server));
    let sp2 = dir.path().join("server2.sqlite");
    for suffix in ["", "-wal"] {
        let from = dir.path().join(format!("server.sqlite{suffix}"));
        if from.exists() {
            std::fs::copy(from, dir.path().join(format!("server2.sqlite{suffix}"))).unwrap();
        }
    }
    let server = authority_at(with_rank("x"), &sp);
    let cp = dir.path().join("client.sqlite");
    let client = open_at(with_rank("x"), 1, &cp);
    let link1 = link(&client, &server);
    let q = client.prepare_query(&Query::from("tasks")).unwrap();
    let sub = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    drop((sub, link1, client, server));
    drop(authority_at(
        small_schema(e(), Some(ColumnType::Boolean)),
        &sp2,
    ));
    let server = authority_at(with_rank("y"), &sp2);
    let before = raw_tags_named(&cp);
    assert!(before.contains(&("rank".to_owned(), 0, "x".to_owned())));
    // The client's `rank` moves to the second authority's stored column,
    // where the authority tags `y` 0 and the client keeps `x` 0.
    let client = open_at(with_rank("y"), 1, &cp);
    let _link = link(&client, &server);
    let q = client.prepare_query(&Query::from("tasks")).unwrap();
    let _sub = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    try_insert(
        &server,
        3,
        &[
            ("title", name("t")),
            ("status", some("doing")),
            ("rank", some("y")),
        ],
    )
    .unwrap();
    pump(&server, &[&client]);
    let client_tags = raw_tags_named(&cp);
    assert!(
        client_tags.contains(&("rank~2".to_owned(), 0, "x".to_owned())),
        "{client_tags:?}"
    );
    assert!(
        client_tags.contains(&("rank~2".to_owned(), 1, "y".to_owned())),
        "{client_tags:?}"
    );
    assert!(raw_tags_named(&sp2).contains(&("rank~2".to_owned(), 0, "y".to_owned())));
    let read = |db: &Db| {
        column(
            db,
            Query::from("tasks").filter(status_is_some("doing")),
            "rank",
        )
    };
    assert_eq!(read(&client), [(3, Some(some("y")))]);
    assert_eq!(read(&client), read(&server));
    drop((_sub, _link, client));
    let client = open_at(with_rank("y"), 1, &cp);
    assert_eq!(read(&client), [(3, Some(some("y")))]);
}

/// Every `(stored column, tag, name)` in `_jazz_enum_tags`.
fn raw_tags_named(path: &Path) -> Vec<(String, i64, String)> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut stmt = conn
        .prepare("SELECT col, tag, name FROM _jazz_enum_tags ORDER BY col, tag")
        .unwrap();
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// A replica whose column moved to the authority's stored name keeps a
/// write made after the move, alongside the synced rows, across a reopen.
#[test]
fn a_moved_column_keeps_its_writes_across_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let sp = dir.path().join("server.sqlite");
    let e = || enumeration(&["todo", "doing", "done"]);
    let server = authority_at(small_schema(ColumnType::Boolean, None), &sp);
    try_insert(
        &server,
        9,
        &[
            ("title", name("t")),
            ("status", Value::Nullable(Some(Box::new(Value::Bool(true))))),
        ],
    )
    .unwrap();
    drop(server);
    let server = authority_at(small_schema(e(), None), &sp);
    try_insert(
        &server,
        1,
        &[("title", name("t")), ("status", some("doing"))],
    )
    .unwrap();
    drop(server);
    let server = authority_at(small_schema(e(), Some(ColumnType::Boolean)), &sp);
    let cp = dir.path().join("client.sqlite");
    drop(open_at(small_schema(e(), None), 1, &cp));
    let client = open_at(small_schema(e(), Some(ColumnType::Boolean)), 1, &cp);
    let _link = link(&client, &server);
    let q = client.prepare_query(&Query::from("tasks")).unwrap();
    let _sub = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    try_insert(
        &client,
        2,
        &[("title", name("t")), ("status", some("done"))],
    )
    .unwrap();
    drop(_sub);
    drop(_link);
    drop(client);
    let client = open_at(small_schema(e(), Some(ColumnType::Boolean)), 1, &cp);
    let got = column(&client, Query::from("tasks"), "status");
    assert_eq!(
        got,
        [
            (1, Some(some("doing"))),
            (2, Some(some("done"))),
            (9, Some(Value::Nullable(None))),
        ]
    );
}

/// A catalogue refused over a column the replica typed differently leaves
/// the replica's own enum writes readable after a reopen.
#[test]
fn a_catalogue_refused_for_another_column_keeps_local_writes() {
    let dir = tempfile::tempdir().unwrap();
    let sp = dir.path().join("server.sqlite");
    let e = || enumeration(&["todo", "doing", "done"]);
    let server = authority_at(small_schema(ColumnType::Boolean, None), &sp);
    try_insert(
        &server,
        9,
        &[
            ("title", name("t")),
            ("status", Value::Nullable(Some(Box::new(Value::Bool(true))))),
        ],
    )
    .unwrap();
    drop(server);
    let server = authority_at(small_schema(e(), None), &sp);
    try_insert(
        &server,
        1,
        &[("title", name("t")), ("status", some("doing"))],
    )
    .unwrap();
    drop(server);
    let server = authority_at(small_schema(e(), Some(ColumnType::Boolean)), &sp);
    let cp = dir.path().join("client.sqlite");
    drop(open_at(small_schema(e(), Some(ColumnType::Text)), 1, &cp));
    let client = open_at(small_schema(e(), None), 1, &cp);
    let _link = link(&client, &server);
    let q = client.prepare_query(&Query::from("tasks")).unwrap();
    let _sub = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    try_insert(
        &client,
        2,
        &[("title", name("t")), ("status", some("done"))],
    )
    .unwrap();
    drop(_sub);
    drop(_link);
    drop(client);
    let client = open_at(small_schema(e(), None), 1, &cp);
    let got = column(&client, Query::from("tasks"), "status");
    assert_eq!(got, [(2, Some(some("done")))]);
}

/// A replica with an enum column the authority doesn't have takes the
/// authority's rows and keeps its own write across a reopen.
#[test]
fn a_replica_with_its_own_enum_column_keeps_local_writes() {
    let dir = tempfile::tempdir().unwrap();
    let sp = dir.path().join("server.sqlite");
    let e = || enumeration(&["todo", "doing", "done"]);
    let server = authority_at(
        small_schema(ColumnType::Boolean, Some(ColumnType::Text)),
        &sp,
    );
    try_insert(
        &server,
        9,
        &[
            ("title", name("t")),
            ("status", Value::Nullable(Some(Box::new(Value::Bool(true))))),
        ],
    )
    .unwrap();
    drop(server);
    let server = authority_at(small_schema(e(), Some(ColumnType::Text)), &sp);
    try_insert(
        &server,
        1,
        &[("title", name("t")), ("status", some("doing"))],
    )
    .unwrap();
    let cp = dir.path().join("client.sqlite");
    let cs = || small_schema(e(), Some(enumeration(&["x"])));
    let client = open_at(cs(), 1, &cp);
    let _link = link(&client, &server);
    let q = client.prepare_query(&Query::from("tasks")).unwrap();
    let _sub = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    try_insert(
        &client,
        2,
        &[("title", name("t")), ("status", some("done"))],
    )
    .unwrap();
    drop(_sub);
    drop(_link);
    drop(client);
    let client = open_at(cs(), 1, &cp);
    let got = column(&client, Query::from("tasks"), "status");
    assert_eq!(
        got,
        [
            (1, Some(some("doing"))),
            (2, Some(some("done"))),
            (9, Some(Value::Nullable(None))),
        ]
    );
}

/// A copy lens's column sorts its variants like its source, removed
/// variants included.
#[test]
fn a_copy_sorts_like_its_source() {
    let dir = tempfile::tempdir().unwrap();
    let sp = dir.path().join("server.sqlite");
    let server = authority_at(v1(), &sp);
    insert(&server, 1, "done");
    insert(&server, 2, "todo");
    block_on(server.activate_schema_with_lenses(v2(), &[lens(&v1(), &v2(), vec![])])).unwrap();
    insert(&server, 3, "blocked");
    let vars = ["blocked", "todo", "doing", "done"];
    let copied = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("tasks")
                    .column("title", ColumnType::Text)
                    .column("status", enumeration(&vars))
                    .column("copy", enumeration(&vars))
                    .nullable_column(
                        "labels",
                        ColumnType::Array {
                            element: Box::new(enumeration(&["bug", "ui"])),
                        },
                    )
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    block_on(server.activate_schema_with_lenses(
        copied.clone(),
        &[lens(
            &v2(),
            &copied,
            vec![LensOp::CopyColumn {
                from: "status".into(),
                to: "copy".into(),
            }],
        )],
    ))
    .unwrap();
    let by = |c: &str| {
        column(
            &server,
            Query::from("tasks").order_by(c, OrderDirection::Asc),
            "title",
        )
        .into_iter()
        .map(|(n, _)| n)
        .collect::<Vec<_>>()
    };
    let (s, c) = (by("status"), by("copy"));
    assert_eq!(s, c);
}
