//! Enum columns are stored as INTEGER tags, which each store keeps for
//! itself in `_jazz_enum_tags`, numbering each stored column's variants in
//! the current schema's declaration order. Everything above SQLite, sync
//! included, uses variant names; a column sorts in its query schema's
//! declaration order.

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
use jazz::query::{Aggregate, Operand, OrderDirection, Predicate, Query};
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
    link_as(client, server, AuthorSubject::SYSTEM)
}

/// `client` connected to `server`, which serves it as `author`.
fn link_as(client: &Db, server: &Db, author: AuthorSubject) -> jazz::db::Upstream {
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
        author,
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
/// filters take names, and ORDER BY follows declaration order, not the
/// alphabet.
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
fn variants_sort_in_the_current_declaration_order_and_removed_ones_last() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("authority.sqlite");
    let db = authority_at(v1(), &path);
    insert(&db, 1, "done");
    insert(&db, 2, "todo");
    // v2 declares `blocked` first: activating it renumbers the tags.
    block_on(db.activate_schema_with_lenses(v2(), &[lens(&v1(), &v2(), vec![])])).unwrap();
    insert(&db, 3, "blocked");
    assert_eq!(
        statuses(&db, by_status()),
        [
            (3, "blocked".into()),
            (2, "todo".into()),
            (1, "done".into())
        ]
    );

    // v3 drops `doing` and adds `review`.
    let v3 = tasks(&["blocked", "todo", "review", "done"]);
    block_on(db.activate_schema_with_lenses(v3.clone(), &[lens(&v2(), &v3, vec![])])).unwrap();
    insert(&db, 4, "review");
    // v3's variants are tags 0-3, and the removed `doing` follows them.
    assert_eq!(
        raw_tags(&path),
        [
            (0, "blocked".into()),
            (1, "todo".into()),
            (2, "review".into()),
            (3, "done".into()),
            (4, "doing".into())
        ]
    );
    assert_eq!(raw_cells(&path), integers(&[0, 1, 2, 3]));
    // `doing` is no variant of v3 any more.
    let refused =
        try_insert(&db, 5, &[("title", name("t")), ("status", name("doing"))]).unwrap_err();
    assert!(
        refused.to_string().contains("not one of its variants"),
        "{refused}"
    );
}

/// A replica tags variants itself, as it admits versions: a version it
/// admits before the authority does tags its new variant there at once,
/// and rows travel by variant name.
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

    // A version only the client has admitted yet: it tags `blocked` itself.
    block_on(client.admit_schema_with_lenses(v2(), &[lens(&v1(), &v2(), vec![])])).unwrap();
    let ahead = block_on(client.register_schema_view(v2())).unwrap();
    let tags = |path: &Path| {
        rusqlite::Connection::open(path)
            .unwrap()
            .query_row(
                "SELECT tag FROM _jazz_enum_tags WHERE name = 'blocked'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .ok()
    };
    assert_eq!(tags(&client_path), Some(3));
    assert_eq!(tags(&dir.path().join("server.sqlite")), None);

    // The authority admits v2 too, and the app updates to it: a row
    // written under it travels by name.
    block_on(server.activate_schema_with_lenses(v2(), &[lens(&v1(), &v2(), vec![])])).unwrap();
    drop((ahead, _sub, _link, client));
    let client = open_at(v2(), 1, &client_path);
    let _link = link(&client, &server);
    let q = client.prepare_query(&Query::from("tasks")).unwrap();
    let _sub = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    let ahead = &client;
    try_insert(
        ahead,
        3,
        &[("title", name("t")), ("status", name("blocked"))],
    )
    .unwrap();
    pump(&server, &[&client]);
    // Opened under v2, the client numbers v2's variants in its order.
    assert_eq!(raw_cells(&client_path), integers(&[0, 1, 3]));
    let declared = [
        (3, "blocked".into()),
        (2, "todo".into()),
        (1, "done".into()),
    ];
    assert_eq!(statuses(ahead, by_status()), declared);
    assert_eq!(statuses(&server, by_status()), declared);
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

/// Tags are never read as text or text as tags: changing a required column
/// between enum and text in place is refused, re-adding it as text after a
/// drop gives it a stored column of its own, the store keeps reading its
/// enum after a reopen, and an app opened with the column as nullable text
/// gets a stored column of its own.
#[test]
fn enum_and_text_never_share_a_stored_column() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("authority.sqlite");
    let db = authority_at(v1(), &path);
    insert(&db, 1, "doing");

    let text = tasks_with("status", ColumnType::Text);
    let refused = block_on(db.activate_schema(text.clone())).unwrap_err();
    assert!(refused.message.contains("changing the type"), "{refused}");

    // Dropped, then re-added as text: a stored column of its own.
    let dropped_dir = tempfile::tempdir().unwrap();
    let dropped_path = dropped_dir.path().join("dropped.sqlite");
    let dropped = authority_at(v1(), &dropped_path);
    insert(&dropped, 1, "doing");
    let without = untracked();
    block_on(dropped.activate_schema(without.clone())).unwrap();
    let readd = lens(
        &without,
        &text,
        vec![LensOp::AddColumn {
            column: "status".into(),
            default: name("unknown"),
        }],
    );
    block_on(dropped.activate_schema_with_lenses(text.clone(), &[readd])).unwrap();
    assert_eq!(raw_cells(&dropped_path), integers(&[1]));
    assert_eq!(
        column(&dropped, Query::from("tasks"), "status"),
        [(1, Some(name("unknown")))]
    );

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
    let (stored, status) = stored_column(&client_path, "tasks", "status");
    let conn = rusqlite::Connection::open(&client_path).unwrap();
    let todo: Option<i64> = conn
        .query_row(
            &format!(
                "SELECT \"{status}\" FROM \"{stored}\" WHERE typeof(\"{status}\") = 'integer' \
                 AND title = 't' AND \"{status}\" = 0"
            ),
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
    client.become_replica();
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
    // The writes reach the server by name.
    assert_eq!(statuses(&server, by_status()), declared);
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

/// Where enum column `table.column` is stored, as the most recently
/// admitted version storing it as an enum names it: (stored table, stored
/// column).
fn stored_column(path: &Path, table: &str, column: &str) -> (String, String) {
    rusqlite::Connection::open(path)
        .unwrap()
        .query_row(
            "SELECT stored_tbl, stored_col FROM _jazz_names n WHERE tbl = ?1 AND col = ?2 \
             AND EXISTS (SELECT 1 FROM _jazz_enum_tags t \
             WHERE t.tbl = n.stored_tbl AND t.col = n.stored_col) \
             ORDER BY n DESC LIMIT 1",
            [table, column],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
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
    let rank = stored_column(&cp, "tasks", "rank").1;
    assert!(before.contains(&(rank, 0, "x".to_owned())));
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
    // Opened under a version declaring `y`, the client numbers it first;
    // `x`, which only it ever stored, follows.
    let rank = stored_column(&sp2, "tasks", "rank").1;
    assert_eq!(stored_column(&cp, "tasks", "rank").1, rank);
    let client_tags = raw_tags_named(&cp);
    assert!(
        client_tags.contains(&(rank.clone(), 0, "y".to_owned())),
        "{client_tags:?}"
    );
    assert!(
        client_tags.contains(&(rank.clone(), 1, "x".to_owned())),
        "{client_tags:?}"
    );
    let server_tags = raw_tags_named(&sp2);
    assert!(server_tags.contains(&(rank, 0, "y".to_owned())));
    assert!(!server_tags.iter().any(|(_, _, name)| name == "x"));
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

/// `MIN` and `MAX` of `status`, as names.
fn min_max(db: &Db) -> (Option<Value>, Option<Value>) {
    let query = Query::from("tasks").aggregate([
        Aggregate::min("status").alias("lo"),
        Aggregate::max("status").alias("hi"),
    ]);
    let prepared = db.prepare_query(&query).unwrap();
    let rows = block_on(db.all(&prepared, ReadOpts::default())).unwrap();
    let cell = |name: &str| {
        rows[0]
            .cells()
            .find(|(n, _)| *n == name)
            .and_then(|(_, value)| value.cloned())
    };
    (cell("lo"), cell("hi"))
}

fn status_after(value: &str) -> Predicate {
    Predicate::Gt(
        Operand::Column("status".into()),
        Operand::Literal(name(value)),
    )
}

/// Making a reordering version current renumbers the tags to its order,
/// rewriting the cells, so its sorts use the raw cell. A version declaring
/// another order still sorts, compares and takes `MIN`/`MAX` in its own.
#[test]
fn a_reorder_renumbers_the_tags_and_older_versions_keep_their_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("authority.sqlite");
    let db = authority_at(v1(), &path);
    insert(&db, 1, "done");
    insert(&db, 2, "todo");
    insert(&db, 3, "doing");
    let reversed = tasks(&["done", "doing", "todo"]);
    block_on(db.activate_schema_with_lenses(reversed.clone(), &[lens(&v1(), &reversed, vec![])]))
        .unwrap();
    assert_eq!(
        raw_tags(&path),
        [(0, "done".into()), (1, "doing".into()), (2, "todo".into())]
    );
    // done 0, todo 2, doing 1.
    assert_eq!(raw_cells(&path), integers(&[0, 1, 2]));
    let current = [(1, "done".into()), (3, "doing".into()), (2, "todo".into())];
    assert_eq!(statuses(&db, by_status()), current);
    assert_eq!(
        ids(&db, Query::from("tasks").filter(status_after("done"))),
        [2, 3]
    );
    assert_eq!(min_max(&db), (Some(some("done")), Some(some("todo"))));

    let old = block_on(db.register_schema_view(v1())).unwrap();
    assert_eq!(
        statuses(&old, by_status()),
        [(2, "todo".into()), (3, "doing".into()), (1, "done".into())]
    );
    assert_eq!(
        ids(&old, Query::from("tasks").filter(status_after("todo"))),
        [1, 3]
    );
    assert_eq!(min_max(&old), (Some(some("todo")), Some(some("done"))));
    // Equality still matches by tag in both.
    assert_eq!(
        ids(&old, Query::from("tasks").filter(status_is("doing"))),
        [3]
    );
    assert_eq!(
        ids(&db, Query::from("tasks").filter(status_is("doing"))),
        [3]
    );

    // Reopened under the current version, the tags stay as they are.
    drop((old, db));
    let db = authority_at(reversed, &path);
    assert_eq!(statuses(&db, by_status()), current);
    assert_eq!(raw_cells(&path), integers(&[0, 1, 2]));
}

/// `tasks` as [`v1`] plus `priority: enum(variants) = low`.
fn prioritized(variants: &[&str]) -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("tasks")
                    .column("title", ColumnType::Text)
                    .column("status", enumeration(&["todo", "doing", "done"]))
                    .nullable_column(
                        "labels",
                        ColumnType::Array {
                            element: Box::new(enumeration(&["bug", "ui"])),
                        },
                    )
                    .column_with_default(
                        "priority",
                        enumeration(variants),
                        jazz::tools::Value::Text("low".into()),
                    )
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

/// A column added with a default keeps SQLite's `DEFAULT` tag. Once a
/// reorder gives that tag to another variant, a version not naming the
/// column still inserts the default variant.
#[test]
fn a_reorder_keeps_the_default_of_a_column_added_later() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("authority.sqlite");
    let db = authority_at(v1(), &path);
    insert(&db, 1, "todo");
    let low_first = prioritized(&["low", "high"]);
    block_on(db.activate_schema_with_lenses(low_first.clone(), &[lens(&v1(), &low_first, vec![])]))
        .unwrap();
    let high_first = prioritized(&["high", "low"]);
    block_on(
        db.activate_schema_with_lenses(
            high_first.clone(),
            &[lens(&low_first, &high_first, vec![])],
        ),
    )
    .unwrap();
    // SQLite's default is `low`'s old tag, now `high`'s.
    let defaults: Vec<(String, String)> = {
        let conn = rusqlite::Connection::open(&path).unwrap();
        let mut stmt = conn
            .prepare("SELECT col, name FROM _jazz_enum_defaults")
            .unwrap();
        stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    };
    let priority = stored_column(&path, "tasks", "priority").1;
    assert_eq!(defaults, [(priority, "low".to_owned())]);
    let old = block_on(db.register_schema_view(v1())).unwrap();
    insert(&old, 2, "done");
    let priorities = |db: &Db| column(db, Query::from("tasks"), "priority");
    let mut read = priorities(&db);
    read.sort_by_key(|(n, _)| *n);
    assert_eq!(read, [(1, Some(name("low"))), (2, Some(name("low")))]);
    drop((old, db));
    let db = authority_at(high_first, &path);
    let old = block_on(db.register_schema_view(v1())).unwrap();
    insert(&old, 3, "done");
    let mut read = priorities(&db);
    read.sort_by_key(|(n, _)| *n);
    assert_eq!(
        read,
        [
            (1, Some(name("low"))),
            (2, Some(name("low"))),
            (3, Some(name("low")))
        ]
    );
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

/// A replica on a version the authority hasn't admitted (an enum column
/// where the authority has text) keeps its own write across a reopen.
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
    assert_eq!(got, [(2, Some(some("done")))]);
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

/// A schema's variants, in declared order, are part of its identity:
/// adding or reordering one makes a new schema version, and so does
/// changing a column between enum and text.
#[test]
fn adding_or_reordering_variants_makes_a_new_schema_version() {
    let id = |schema: JazzSchema| schema.version_id();
    assert_eq!(id(v1()), id(v1()));
    assert_ne!(id(v1()), id(v2()), "a variant added");
    assert_ne!(
        id(tasks(&["todo", "doing", "done"])),
        id(tasks(&["doing", "todo", "done"])),
        "variants reordered"
    );
    assert_ne!(
        id(v1()),
        id(tasks_with("status", ColumnType::Text)),
        "enum to text"
    );
    // Pinned: the id addresses every catalogue record, so it never drifts.
    assert_eq!(
        id(v1()).0.to_string(),
        "463e4b11-caaa-5f66-8bc7-8372381bc982"
    );
}

/// `state` (row bytes in order) with a subscription `event` applied.
fn apply(state: &mut Vec<u8>, event: SubscriptionEvent) {
    if let SubscriptionEvent::Delta {
        reset,
        added,
        updated,
        removed,
        ..
    } = event
    {
        if reset {
            state.clear();
        }
        let mut gone: Vec<u8> = removed.iter().map(|r| r.row_uuid.as_bytes()[0]).collect();
        gone.extend(updated.iter().map(|r| r.row.row_uuid().as_bytes()[0]));
        state.retain(|id| !gone.contains(id));
        let mut placed: Vec<(usize, u8)> = added
            .iter()
            .chain(updated.iter())
            .map(|r| (r.index, r.row.row_uuid().as_bytes()[0]))
            .collect();
        placed.sort();
        for (index, id) in placed {
            let at = index.min(state.len());
            state.insert(at, id);
        }
    }
}

/// The row bytes `query` returns, in its order.
fn in_order(db: &Db, query: &Query) -> Vec<u8> {
    column(db, query.clone(), "title")
        .into_iter()
        .map(|(n, _)| n)
        .collect()
}

/// A live subscription under a version whose declaration order is not tag
/// order (an older client during a rollout) keeps its rows, and its
/// window, in that version's order as writes arrive.
#[test]
fn a_subscription_under_an_older_version_keeps_its_order() {
    for limit in [None, Some(2usize)] {
        let dir = tempfile::tempdir().unwrap();
        let db = authority_at(v1(), &dir.path().join("authority.sqlite"));
        insert(&db, 1, "done");
        insert(&db, 3, "doing");
        insert(&db, 6, "doing");
        let reversed = tasks(&["done", "doing", "todo"]);
        block_on(
            db.activate_schema_with_lenses(reversed.clone(), &[lens(&v1(), &reversed, vec![])]),
        )
        .unwrap();
        let old = block_on(db.register_schema_view(v1())).unwrap();
        let mut query = by_status();
        if let Some(limit) = limit {
            query = query.limit(limit);
        }
        let prepared = old.prepare_query(&query).unwrap();
        let mut stream = block_on(old.subscribe(&prepared, ReadOpts::default())).unwrap();
        block_on(old.tick()).unwrap();
        let mut state = Vec::new();
        while let Some(event) = stream.try_next_event() {
            apply(&mut state, event);
        }
        assert_eq!(state, in_order(&old, &query), "{limit:?}: initial");
        // Through the current version, then through the old view.
        insert(&db, 2, "todo");
        insert(&old, 4, "todo");
        insert(&old, 5, "done");
        block_on(db.tick()).unwrap();
        block_on(old.tick()).unwrap();
        while let Some(event) = stream.try_next_event() {
            apply(&mut state, event);
        }
        let expected = match limit {
            None => vec![2, 4, 3, 6, 1, 5],
            Some(_) => vec![2, 4],
        };
        assert_eq!(in_order(&old, &query), expected, "{limit:?}: fresh");
        assert_eq!(state, expected, "{limit:?}: subscription");
    }
}

/// A range compared with a name the column never held has no place in the
/// order: it is refused, not answered with every row or none. Equality
/// with one matches nothing.
#[test]
fn a_range_with_an_unknown_variant_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = authority_at(v1(), &dir.path().join("authority.sqlite"));
    insert(&db, 1, "todo");
    insert(&db, 2, "done");
    let read = |predicate: Predicate| {
        let query = Query::from("tasks").filter(predicate);
        db.prepare_query(&query)
            .and_then(|prepared| block_on(db.all(&prepared, ReadOpts::default())))
            .map(|rows| rows.len())
    };
    let cmp = |op: fn(Operand, Operand) -> Predicate, value: &str| {
        op(
            Operand::Column("status".into()),
            Operand::Literal(name(value)),
        )
    };
    for predicate in [
        cmp(Predicate::Lt, "nope"),
        cmp(Predicate::Gt, "aaa"),
        cmp(Predicate::Gte, "aaa"),
    ] {
        let refused = read(predicate).unwrap_err();
        assert!(
            refused.to_string().contains("not one of its variants"),
            "{refused}"
        );
    }
    assert_eq!(read(status_is("nope")).unwrap(), 0);
    assert_eq!(read(cmp(Predicate::Gt, "todo")).unwrap(), 1);
}

/// A peer declaring the same variants in another order is another schema
/// version (an alpha.59 peer's ids never count variants at all): until its
/// server deploys that version, its writes are refused with a typed reason.
#[test]
fn a_peer_on_another_variant_order_is_refused_until_deployed() {
    use std::pin::pin;
    use std::task::{Context, Poll};
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(v1(), &dir.path().join("server.sqlite"));
    let client = open_at(
        tasks(&["done", "doing", "todo"]),
        1,
        &dir.path().join("client.sqlite"),
    );
    client.become_replica();
    let _link = link(&client, &server);
    let q = client.prepare_query(&Query::from("tasks")).unwrap();
    let _sub = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    let tx = block_on(client.insert(
        "tasks",
        BTreeMap::from([
            ("title".to_owned(), name("t")),
            ("status".to_owned(), name("done")),
        ]),
        InsertOptions {
            row_id: Some(RowUuid::from_bytes([2; 16])),
            ..Default::default()
        },
    ))
    .unwrap()
    .mergeable_tx_id();
    let mut wait = pin!(client.wait_for_transaction(tx, jazz::tx::DurabilityTier::Global));
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut fate = None;
    for _ in 0..20 {
        if let Poll::Ready(result) = wait.as_mut().poll(&mut cx) {
            fate = Some(result);
            break;
        }
        pump(&server, &[&client]);
    }
    let error = fate.expect("a fate").expect_err("refused by the server");
    assert_eq!(
        error.reason,
        Some(jazz::db::Reason::SchemaVersionNotDeployed),
        "{error:?}"
    );
    assert!(ids(&server, Query::from("tasks")).is_empty());
}

/// `tasks` under `variants`, readable only where `status` is `done`.
fn done_only(variants: &[&str]) -> JazzSchema {
    use jazz::tools::{CmpOp, PolicyExpr, PolicyValue, TablePolicies};
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("tasks")
                    .column("title", ColumnType::Text)
                    .column("status", enumeration(variants))
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::Cmp {
                                column: "status".into(),
                                op: CmpOp::Eq,
                                value: PolicyValue::Literal(jazz::tools::Value::Text(
                                    "done".into(),
                                )),
                            })
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                            .with_delete(PolicyExpr::True),
                    ),
            )
            .build(),
    )
}

/// A read policy naming a variant binds its tag as the store numbers it
/// now: after a reorder renumbers the authority's tags, a user session
/// still sees exactly the policy's rows, on the older version and on the
/// reordered one.
#[test]
fn a_policy_on_a_variant_keeps_its_rows_across_a_reorder() {
    let dir = tempfile::tempdir().unwrap();
    let v1 = done_only(&["todo", "doing", "done"]);
    let server = authority_at(v1.clone(), &dir.path().join("server.sqlite"));
    insert(&server, 1, "todo");
    insert(&server, 2, "doing");
    insert(&server, 3, "done");
    let user = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_path = dir.path().join("client.sqlite");
    let sees = |schema: JazzSchema| {
        let client = block_on(Db::open_at(
            DbConfig::new(
                schema,
                DbIdentity {
                    node: NodeUuid::from_bytes([1; 16]),
                    author: user,
                },
            ),
            &client_path,
            FileOptions::default(),
        ))
        .unwrap();
        let _link = link_as(&client, &server, user);
        let q = client.prepare_query(&Query::from("tasks")).unwrap();
        let _sub = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
        pump(&server, &[&client]);
        ids(&client, Query::from("tasks"))
    };
    assert_eq!(sees(v1.clone()), [3]);
    let reversed = done_only(&["done", "doing", "todo"]);
    block_on(server.activate_schema_with_lenses(reversed.clone(), &[lens(&v1, &reversed, vec![])]))
        .unwrap();
    insert(&server, 4, "done");
    insert(&server, 5, "todo");
    assert_eq!(sees(v1), [3, 4]);
    assert_eq!(sees(reversed), [3, 4]);
}

/// Groups of an enum column carry variant names and order by the query
/// schema's declaration order, under the current version and an older one.
#[test]
fn grouping_by_an_enum_follows_the_declaration_order() {
    let dir = tempfile::tempdir().unwrap();
    let db = authority_at(v1(), &dir.path().join("authority.sqlite"));
    insert(&db, 1, "done");
    insert(&db, 2, "todo");
    insert(&db, 3, "done");
    insert(&db, 4, "doing");
    let groups = |db: &Db| -> Vec<(String, Value, RowUuid)> {
        let query = Query::from("tasks")
            .aggregate([Aggregate::count().alias("n")])
            .group_by("status")
            .order_by("status", OrderDirection::Asc);
        let prepared = db.prepare_query(&query).unwrap();
        block_on(db.all(&prepared, ReadOpts::default()))
            .unwrap()
            .iter()
            .map(|row| {
                let cell = |name: &str| {
                    row.cells()
                        .find(|(n, _)| *n == name)
                        .and_then(|(_, value)| value.cloned())
                        .unwrap()
                };
                match cell("status") {
                    Value::String(status) => (status, cell("n"), row.row_uuid()),
                    other => panic!("status {other:?}"),
                }
            })
            .collect()
    };
    let in_v1_order = groups(&db);
    let names: Vec<&str> = in_v1_order.iter().map(|(s, ..)| s.as_str()).collect();
    assert_eq!(names, ["todo", "doing", "done"]);
    let reversed = tasks(&["done", "doing", "todo"]);
    block_on(db.activate_schema_with_lenses(reversed.clone(), &[lens(&v1(), &reversed, vec![])]))
        .unwrap();
    let mut reversed_order = groups(&db);
    let names: Vec<&str> = reversed_order.iter().map(|(s, ..)| s.as_str()).collect();
    assert_eq!(names, ["done", "doing", "todo"]);
    // The same groups, each keeping its identity across the renumbering.
    reversed_order.reverse();
    assert_eq!(reversed_order, in_v1_order);
    let old = block_on(db.register_schema_view(v1())).unwrap();
    assert_eq!(groups(&old), in_v1_order);
}
