//! Renaming a column or table with a migration lens. The stored table or
//! column keeps the name it was created with; each schema version reads and
//! writes it under its own name, so old and new versions share the same rows,
//! history and indexes, a reopened file finds them again, and a client on the
//! old schema syncs with a server on the new one. No data is copied.
//!
//! The lens ops that do write values (INV-LENS-13, INV-LENS-17): a copy is a
//! stored column of its own, filled from its source at activation and in
//! rows older versions insert; a dropped column gets the lens's backwards
//! default in rows newer versions insert; the identity transform changes no
//! value.

use crate::common;

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, ReadTier, SubscriptionEvent,
    SyncMessage, Transport, TransportError, UpdateOptions, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{LensOp, MigrationLens, TableLens};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TablePolicies, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema, read_and_allow_all_writes, session_eq};

fn todos(table: &str, title: &str) -> TableSchemaBuilder {
    TableSchemaBuilder::new(table)
        .column(title, ColumnType::Text)
        .column_with_default("rank", ColumnType::BigInt, jazz::tools::Value::BigInt(0))
        .index_only([title])
        .policies(allow_all_policies())
}

fn schema(tables: Vec<TableSchemaBuilder>) -> JazzSchema {
    let mut builder = SchemaBuilder::new();
    for table in tables {
        builder = builder.table(table);
    }
    compile_schema(&builder.build())
}

/// `todos(title, rank)`.
fn v1() -> JazzSchema {
    schema(vec![todos("todos", "title")])
}

/// v1 with `title` renamed to `name`.
fn v2() -> JazzSchema {
    schema(vec![todos("todos", "name")])
}

/// v2 with `todos` renamed to `tasks`.
fn v3() -> JazzSchema {
    schema(vec![todos("tasks", "name")])
}

fn lens(
    source: &JazzSchema,
    target: &JazzSchema,
    from: &str,
    to: &str,
    ops: Vec<LensOp>,
) -> MigrationLens {
    MigrationLens::new(
        source.version_id(),
        target.version_id(),
        vec![TableLens {
            source_table: from.into(),
            target_table: to.into(),
            ops,
        }],
    )
    .unwrap()
}

fn rename_title() -> MigrationLens {
    lens(
        &v1(),
        &v2(),
        "todos",
        "todos",
        vec![LensOp::RenameColumn {
            from: "title".into(),
            to: "name".into(),
        }],
    )
}

fn rename_todos() -> MigrationLens {
    lens(&v2(), &v3(), "todos", "tasks", vec![])
}

fn config(schema: JazzSchema, node: u8, author: AuthorSubject) -> DbConfig {
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )
}

fn open_at(schema: JazzSchema, path: &Path) -> Db {
    block_on(Db::open_at(
        config(schema, 1, AuthorSubject::SYSTEM),
        path,
        FileOptions::default(),
    ))
    .unwrap()
}

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("jazz-rename-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("store.sqlite")
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn text(value: &str) -> Value {
    Value::String(value.to_owned())
}

fn insert(db: &Db, table: &str, n: u8, column: &str, value: &str) {
    block_on(db.insert(
        table,
        BTreeMap::from([(column.to_owned(), text(value))]),
        InsertOptions {
            row_id: Some(row(n)),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn update(db: &Db, table: &str, n: u8, column: &str, value: &str) {
    block_on(db.update(
        table,
        row(n),
        BTreeMap::from([(column.to_owned(), text(value))]),
        UpdateOptions::default(),
    ))
    .unwrap();
}

/// `column` of every row of `table`, in row order.
fn read(db: &Db, table: &str, column: &str) -> Vec<Value> {
    read_query(db, Query::from(table), column, ReadOpts::default())
}

fn read_query(db: &Db, query: Query, column: &str, opts: ReadOpts) -> Vec<Value> {
    let prepared = db.prepare_query(&query).unwrap();
    let mut rows: Vec<_> = block_on(db.all(&prepared, opts))
        .unwrap()
        .iter()
        .map(|row| {
            let value = row
                .cells()
                .find(|(name, _)| *name == column)
                .and_then(|(_, value)| value.cloned())
                .unwrap_or_else(|| panic!("no column {column}"));
            (row.row_uuid(), value)
        })
        .collect();
    rows.sort_by_key(|(id, _)| *id);
    rows.into_iter().map(|(_, value)| value).collect()
}

fn texts(values: &[&str]) -> Vec<Value> {
    values.iter().map(|value| text(value)).collect()
}

/// The stored tables and their value columns, without Jazz bookkeeping
/// (row ids, provenance, history tables).
fn stored(path: &Path) -> BTreeMap<String, Vec<String>> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let tables: Vec<String> = conn
        .prepare(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '\\_%' ESCAPE '\\' \
             AND name NOT LIKE '%\\_\\_h' ESCAPE '\\' AND name NOT LIKE 'sqlite%'",
        )
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    tables
        .into_iter()
        .map(|table| {
            let columns: Vec<String> = conn
                .prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))
                .unwrap()
                .query_map([], |row| row.get(0))
                .unwrap()
                .collect::<Result<Vec<String>, _>>()
                .unwrap()
                .into_iter()
                .filter(|column| {
                    !column.starts_with(['_', '$']) && column != "id" && column != "rid"
                })
                .collect();
            (table, columns)
        })
        .collect()
}

fn indexes(path: &Path) -> Vec<String> {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.prepare("SELECT name FROM sqlite_master WHERE type = 'index' AND name LIKE '%\\_\\_ix(%' ESCAPE '\\' ORDER BY name")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

#[test]
fn a_renamed_column_is_read_and_written_by_both_versions() {
    let path = scratch("column");
    let db = open_at(v1(), &path);
    insert(&db, "todos", 1, "title", "a");
    let before = stored(&path);
    block_on(db.activate_schema_with_lenses(v2(), &[rename_title()])).unwrap();
    assert_eq!(stored(&path), before, "no column is added or copied");

    let old = block_on(db.register_schema_view(v1())).unwrap();
    assert_eq!(read(&db, "todos", "name"), texts(&["a"]));
    insert(&db, "todos", 2, "name", "b");
    assert_eq!(read(&old, "todos", "title"), texts(&["a", "b"]));

    update(&old, "todos", 1, "title", "a2");
    update(&db, "todos", 2, "name", "b2");
    assert_eq!(read(&db, "todos", "name"), texts(&["a2", "b2"]));
    assert_eq!(read(&old, "todos", "title"), texts(&["a2", "b2"]));
    assert_eq!(
        read(&db, "todos", "rank"),
        vec![Value::I64(0), Value::I64(0)]
    );

    // Filters and the index use the stored column under either name.
    let by_name = Query::from("todos").filter(eq(col("name"), lit(text("b2"))));
    assert_eq!(
        read_query(&db, by_name, "name", ReadOpts::default()),
        texts(&["b2"])
    );
    let by_title = Query::from("todos").filter(eq(col("title"), lit(text("a2"))));
    assert_eq!(
        read_query(&old, by_title, "title", ReadOpts::default()),
        texts(&["a2"])
    );
    assert_eq!(indexes(&path).len(), 1, "one index serves both versions");
}

#[test]
fn a_subscription_on_the_old_version_sees_writes_under_the_new_name() {
    let db = block_on(Db::open(config(v1(), 1, AuthorSubject::SYSTEM))).unwrap();
    insert(&db, "todos", 1, "title", "a");
    block_on(db.activate_schema_with_lenses(v2(), &[rename_title()])).unwrap();
    let old = block_on(db.register_schema_view(v1())).unwrap();
    let prepared = old.prepare_query(&Query::from("todos")).unwrap();
    let mut stream = block_on(old.subscribe(&prepared, ReadOpts::default())).unwrap();
    let title = |row: &jazz::db::SubscriptionOutputRow| {
        row.cells()
            .find(|(name, _)| *name == "title")
            .and_then(|(_, value)| value.cloned())
    };
    match stream.try_next_event().expect("initial rows") {
        SubscriptionEvent::Delta { added, .. } => {
            assert_eq!(
                added.iter().map(title).collect::<Vec<_>>(),
                [Some(text("a"))]
            );
        }
        event => panic!("expected the initial rows, got {event:?}"),
    }

    update(&db, "todos", 1, "name", "a2");
    insert(&db, "todos", 2, "name", "b");
    let mut seen = Vec::new();
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Delta { added, updated, .. } = event {
            seen.extend(added.iter().chain(&updated).map(title));
        }
    }
    assert!(seen.contains(&Some(text("a2"))), "{seen:?}");
    assert!(seen.contains(&Some(text("b"))), "{seen:?}");
}

#[test]
fn a_renamed_table_keeps_its_stored_table() {
    let path = scratch("table");
    let db = open_at(v1(), &path);
    insert(&db, "todos", 1, "title", "a");
    block_on(db.activate_schema_with_lenses(v2(), &[rename_title()])).unwrap();
    block_on(db.activate_schema_with_lenses(v3(), &[rename_todos()])).unwrap();
    assert_eq!(
        stored(&path),
        BTreeMap::from([(
            "todos".to_owned(),
            vec!["title".to_owned(), "rank".to_owned()]
        )]),
        "the stored table and column keep their first names"
    );

    insert(&db, "tasks", 2, "name", "b");
    let v1_view = block_on(db.register_schema_view(v1())).unwrap();
    let v2_view = block_on(db.register_schema_view(v2())).unwrap();
    insert(&v1_view, "todos", 3, "title", "c");
    update(&v2_view, "todos", 1, "name", "a2");
    assert_eq!(read(&db, "tasks", "name"), texts(&["a2", "b", "c"]));
    assert_eq!(read(&v1_view, "todos", "title"), texts(&["a2", "b", "c"]));
    assert!(db.prepare_query(&Query::from("todos")).is_err());
}

#[test]
fn reopening_finds_the_renamed_columns_again() {
    let path = scratch("reopen");
    {
        let db = open_at(v1(), &path);
        insert(&db, "todos", 1, "title", "a");
        block_on(db.activate_schema_with_lenses(v2(), &[rename_title()])).unwrap();
        insert(&db, "todos", 2, "name", "b");
    }
    let after_rename = stored(&path);
    {
        // The file remembers where v2 keeps `name`; no lens is needed.
        let db = open_at(v2(), &path);
        assert_eq!(read(&db, "todos", "name"), texts(&["a", "b"]));
        let old = block_on(db.register_schema_view(v1())).unwrap();
        insert(&old, "todos", 3, "title", "c");
        assert_eq!(read(&db, "todos", "name"), texts(&["a", "b", "c"]));
    }
    {
        let db = open_at(v1(), &path);
        assert_eq!(read(&db, "todos", "title"), texts(&["a", "b", "c"]));
        update(&db, "todos", 2, "title", "b2");
    }
    let db = open_at(v2(), &path);
    assert_eq!(read(&db, "todos", "name"), texts(&["a", "b2", "c"]));
    assert_eq!(stored(&path), after_rename, "reopening adds nothing");
}

/// `table(body?)`: a nullable text column under the name `table`.
fn documents(table: &str) -> JazzSchema {
    schema(vec![
        TableSchemaBuilder::new(table)
            .nullable_column("body", ColumnType::Text)
            .policies(allow_all_policies()),
    ])
}

fn bodies(values: &[Option<&str>]) -> Vec<Value> {
    values
        .iter()
        .map(|value| Value::Nullable(value.map(|v| Box::new(text(v)))))
        .collect()
}

/// #3005: across a table rename, an owner reopened on either version reads
/// and writes the same rows, and a fresh receiver on the renamed table reads
/// the nullable text an old version wrote.
#[test]
fn a_table_rename_holds_across_reopen_and_for_a_fresh_receiver() {
    let (before, after) = (documents("notes"), documents("documents"));
    let rename = lens(&before, &after, "notes", "documents", vec![]);
    let path = scratch("table-reopen");
    {
        let db = open_at(before.clone(), &path);
        insert(&db, "notes", 1, "body", "a");
        block_on(db.activate_schema_with_lenses(after.clone(), &[rename])).unwrap();
        let old = block_on(db.register_schema_view(before.clone())).unwrap();
        insert(&old, "notes", 2, "body", "b");
    }
    {
        let db = open_at(after.clone(), &path);
        assert_eq!(
            read(&db, "documents", "body"),
            bodies(&[Some("a"), Some("b")])
        );
        let old = block_on(db.register_schema_view(before.clone())).unwrap();
        insert(&old, "notes", 3, "body", "c");
        block_on(old.insert(
            "notes",
            [("body".to_owned(), Value::Nullable(None))].into(),
            InsertOptions {
                row_id: Some(row(4)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    {
        let db = open_at(before.clone(), &path);
        update(&db, "notes", 1, "body", "a2");
        assert_eq!(read(&db, "notes", "body").len(), 4);
    }

    let server = open_at(after.clone(), &path);
    server.set_authority();
    let fresh = block_on(Db::open(config(after, 2, user(2)))).unwrap();
    let (client_side, server_side) = pipe();
    let _link = block_on(fresh.connect_upstream(client_side));
    server.accept_subscriber(server_side, user(2));
    let query = fresh.prepare_query(&Query::from("documents")).unwrap();
    let mut stream = block_on(fresh.subscribe(&query, ReadOpts::default())).unwrap();
    pump(&server, &[&fresh]);
    while let Some(event) = stream.try_next_event() {
        assert!(
            !matches!(event, SubscriptionEvent::Rejected { .. }),
            "{event:?}"
        );
    }
    assert_eq!(
        read_query(&fresh, Query::from("documents"), "body", local()),
        bodies(&[Some("a2"), Some("b"), Some("c"), None])
    );
}

#[test]
fn a_name_freed_by_a_rename_gets_its_own_stored_column() {
    let path = scratch("freed");
    let db = open_at(v1(), &path);
    insert(&db, "todos", 1, "title", "old");
    block_on(db.activate_schema_with_lenses(v2(), &[rename_title()])).unwrap();
    // v4 adds a new, unrelated `title` next to the renamed `name`.
    let v4 = schema(vec![
        todos("todos", "name").nullable_column("title", ColumnType::Text),
    ]);
    let add = lens(&v2(), &v4, "todos", "todos", vec![]);
    block_on(db.activate_schema_with_lenses(v4.clone(), &[add])).unwrap();

    assert_eq!(read(&db, "todos", "name"), texts(&["old"]));
    assert_eq!(read(&db, "todos", "title"), vec![Value::Nullable(None)]);
    let old = block_on(db.register_schema_view(v1())).unwrap();
    assert_eq!(read(&old, "todos", "title"), texts(&["old"]));
    let columns = &stored(&path)["todos"];
    assert_eq!(columns.len(), 3, "{columns:?}");
    assert!(columns.contains(&"title~2".to_owned()), "{columns:?}");
}

#[test]
fn renaming_a_missing_column_or_table_is_rejected() {
    let db = block_on(Db::open(config(v1(), 1, AuthorSubject::SYSTEM))).unwrap();
    let wrong = lens(
        &v1(),
        &v2(),
        "todos",
        "todos",
        vec![LensOp::RenameColumn {
            from: "missing".into(),
            to: "name".into(),
        }],
    );
    let error = block_on(db.activate_schema_with_lenses(v2(), &[wrong])).unwrap_err();
    assert!(error.to_string().contains("missing"), "{error}");

    let renamed = schema(vec![todos("tasks", "title")]);
    let wrong = lens(&v1(), &renamed, "nothing", "tasks", vec![]);
    let error = block_on(db.activate_schema_with_lenses(renamed, &[wrong])).unwrap_err();
    assert!(error.to_string().contains("nothing"), "{error}");
}

#[test]
fn renaming_a_branch_column_is_not_supported_yet() {
    let branched = |column: &str| {
        schema(vec![
            TableSchemaBuilder::new("docs")
                .column(column, ColumnType::Text)
                .column("body", ColumnType::Text)
                .branch_by(column)
                .policies(allow_all_policies()),
        ])
    };
    let (from, to) = (branched("space"), branched("area"));
    let db = block_on(Db::open(config(from.clone(), 1, AuthorSubject::SYSTEM))).unwrap();
    let rename = lens(
        &from,
        &to,
        "docs",
        "docs",
        vec![LensOp::RenameColumn {
            from: "space".into(),
            to: "area".into(),
        }],
    );
    let error = block_on(db.activate_schema_with_lenses(to, &[rename])).unwrap_err();
    assert!(error.to_string().contains("not supported"), "{error}");
}

// ---- sync between versions -------------------------------------------------

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

/// (client side, server side)
fn pipe() -> (Box<dyn Transport>, Box<dyn Transport>) {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    (
        Box::new(Pipe {
            out: up.clone(),
            inbound: down.clone(),
        }),
        Box::new(Pipe {
            out: down,
            inbound: up,
        }),
    )
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

fn local() -> ReadOpts {
    ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    }
}

fn user(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([n; 16])
}

/// A server that started on `first` and then activated each of `steps`
/// with the lens into it.
fn server(first: JazzSchema, steps: Vec<(JazzSchema, MigrationLens)>) -> Db {
    let db = block_on(Db::open_history_complete(config(
        first,
        0,
        AuthorSubject::SYSTEM,
    )))
    .unwrap();
    for (schema, lens) in steps {
        block_on(db.activate_schema_with_lenses(schema, &[lens])).unwrap();
    }
    db
}

fn seed(server: &Db, table: &str, n: u8, cells: &[(&str, Value)]) {
    server
        .seed_settled_mergeable_for_bootstrap(
            table,
            row(n),
            AuthorSubject::SYSTEM,
            cells
                .iter()
                .map(|(column, value)| ((*column).to_owned(), value.clone()))
                .collect(),
        )
        .unwrap();
}

#[test]
fn a_client_on_the_old_schema_syncs_with_a_server_on_the_new_one() {
    let server = server(v1(), vec![(v2(), rename_title()), (v3(), rename_todos())]);
    seed(&server, "tasks", 1, &[("name", text("from server"))]);

    let old = block_on(Db::open(config(v1(), 1, user(1)))).unwrap();
    let new = block_on(Db::open(config(v3(), 2, user(2)))).unwrap();
    let (old_client, old_server) = pipe();
    let (new_client, new_server) = pipe();
    let _old_link = block_on(old.connect_upstream(old_client));
    let _new_link = block_on(new.connect_upstream(new_client));
    server.accept_subscriber(old_server, user(1));
    server.accept_subscriber(new_server, user(2));
    let old_query = old.prepare_query(&Query::from("todos")).unwrap();
    let new_query = new.prepare_query(&Query::from("tasks")).unwrap();
    let _old_stream = block_on(old.subscribe(&old_query, ReadOpts::default())).unwrap();
    let _new_stream = block_on(new.subscribe(&new_query, ReadOpts::default())).unwrap();
    pump(&server, &[&old, &new]);
    assert_eq!(
        read_query(&old, Query::from("todos"), "title", local()),
        texts(&["from server"])
    );

    // The old client writes under its names; the server and the new client
    // read the same rows under theirs.
    insert(&old, "todos", 2, "title", "from old");
    update(&old, "todos", 1, "title", "edited by old");
    pump(&server, &[&old, &new]);
    assert_eq!(
        read(&server, "tasks", "name"),
        texts(&["edited by old", "from old"])
    );
    assert_eq!(
        read_query(&new, Query::from("tasks"), "name", local()),
        texts(&["edited by old", "from old"])
    );

    insert(&new, "tasks", 3, "name", "from new");
    pump(&server, &[&old, &new]);
    assert_eq!(
        read_query(&old, Query::from("todos"), "title", local()),
        texts(&["edited by old", "from old", "from new"])
    );
}

/// `notes(owner, body)`, readable by its owner; `owner_column` names `owner`.
fn notes(owner_column: &str) -> JazzSchema {
    let policies: TablePolicies =
        read_and_allow_all_writes(session_eq(owner_column, &["user", "identity", "subject"]));
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column(owner_column, ColumnType::Text)
                    .column("body", ColumnType::Text)
                    .policies(policies),
            )
            .build(),
    )
}

#[test]
fn an_old_client_is_served_under_the_current_policies_by_the_stored_column() {
    let (old_schema, new_schema) = (notes("owner"), notes("holder"));
    let rename = lens(
        &old_schema,
        &new_schema,
        "notes",
        "notes",
        vec![LensOp::RenameColumn {
            from: "owner".into(),
            to: "holder".into(),
        }],
    );
    let server = server(old_schema.clone(), vec![(new_schema, rename)]);
    for (n, who) in [(1, user(1)), (2, user(2))] {
        seed(
            &server,
            "notes",
            n,
            &[
                ("holder", text(&who.principal_parts().1)),
                ("body", text(&format!("note {n}"))),
            ],
        );
    }

    let client = block_on(Db::open(config(old_schema, 1, user(1)))).unwrap();
    let (client_side, server_side) = pipe();
    let _link = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, user(1));
    let query = client.prepare_query(&Query::from("notes")).unwrap();
    let _stream = block_on(client.subscribe(&query, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    assert_eq!(
        read_query(&client, Query::from("notes"), "body", local()),
        texts(&["note 1"]),
        "only the note the renamed column says user 1 holds"
    );
    assert_eq!(
        read_query(&client, Query::from("notes"), "owner", local()),
        vec![text(&user(1).principal_parts().1)]
    );
}

/// `docs(branch_id, title)`, branched, and the same table renamed `pages`.
fn branched(table: &str) -> JazzSchema {
    schema(vec![
        TableSchemaBuilder::new(table)
            .column("branch_id", ColumnType::Uuid)
            .column("title", ColumnType::Text)
            .branch_by("branch_id")
            .policies(allow_all_policies()),
    ])
}

fn branch(byte: u8) -> jazz::protocol::BranchSelector {
    jazz::protocol::BranchSelector::new([(
        "branch_id",
        Value::Uuid(uuid::Uuid::from_bytes([byte; 16])),
    )])
}

/// A snapshot taken before a table rename still freezes the base: a draft
/// written under the new name over that frozen base copies the row as it was
/// at the capture, read from the stored table's history.
#[test]
fn a_frozen_base_from_before_a_table_rename_still_copies_the_frozen_row() {
    use jazz::db::{ExactWriteTarget, WriteTarget};
    use jazz::protocol::BranchViewBase;
    let db = block_on(Db::open(config(branched("docs"), 1, AuthorSubject::SYSTEM))).unwrap();
    let (main, draft) = (branch(1), branch(2));
    block_on(db.insert(
        "docs",
        BTreeMap::from([("title".to_owned(), text("frozen"))]),
        InsertOptions {
            row_id: Some(row(1)),
            target: ExactWriteTarget::Branch(main.clone()),
            ..Default::default()
        },
    ))
    .unwrap();
    let snapshot = block_on(db.capture_snapshot()).unwrap();
    let on = |head: &jazz::protocol::BranchSelector, base| UpdateOptions {
        target: WriteTarget::BranchView {
            head: head.clone(),
            base,
        },
        ..Default::default()
    };
    block_on(db.update(
        "docs",
        row(1),
        BTreeMap::from([("title".to_owned(), text("later"))]),
        on(&main, None),
    ))
    .unwrap();

    let pages = branched("pages");
    let rename = lens(&branched("docs"), &pages, "docs", "pages", vec![]);
    block_on(db.activate_schema_with_lenses(pages, &[rename])).unwrap();

    let frozen = Some(BranchViewBase::snapshot(main.clone(), snapshot));
    block_on(db.update("pages", row(1), BTreeMap::new(), on(&draft, frozen.clone()))).unwrap();
    let titles = |opts| read_query(&db, Query::from("pages"), "title", opts);
    assert_eq!(
        titles(ReadOpts::default().branch_view(draft.clone(), None)),
        texts(&["frozen"]),
        "the draft copied the frozen version"
    );
    assert_eq!(
        titles(ReadOpts::default().branch_view(main, None)),
        texts(&["later"])
    );
}

/// `todos(title, rank)` plus `search_title`, a copy of `title`.
fn with_copy() -> JazzSchema {
    schema(vec![
        todos("todos", "title").column("search_title", ColumnType::Text),
    ])
}

fn copy_title() -> MigrationLens {
    lens(
        &v1(),
        &with_copy(),
        "todos",
        "todos",
        vec![LensOp::CopyColumn {
            from: "title".into(),
            to: "search_title".into(),
        }],
    )
}

/// `column` of every row version `table`'s history holds, sorted.
fn history(path: &Path, table: &str, column: &str) -> Vec<String> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut values: Vec<String> = conn
        .prepare(&format!("SELECT \"{column}\" FROM \"{table}__h\""))
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    values.sort();
    values
}

#[test]
fn a_copied_column_is_filled_from_its_source_in_current_rows_and_history() {
    let path = scratch("copy-fill");
    let db = open_at(v1(), &path);
    insert(&db, "todos", 1, "title", "a");
    update(&db, "todos", 1, "title", "a2");
    insert(&db, "todos", 2, "title", "b");
    block_on(db.activate_schema_with_lenses(with_copy(), &[copy_title()])).unwrap();

    assert_eq!(read(&db, "todos", "search_title"), texts(&["a2", "b"]));
    assert_eq!(read(&db, "todos", "title"), texts(&["a2", "b"]));
    assert_eq!(
        history(&path, "todos", "search_title"),
        vec!["a".to_owned()],
        "superseded versions hold the copy too"
    );
}

#[test]
fn a_row_an_older_version_inserts_gets_the_copy_of_its_source() {
    let path = scratch("copy-mirror");
    let db = open_at(v1(), &path);
    insert(&db, "todos", 1, "title", "a");
    // A handle opened before the copy existed keeps it in step too.
    let old = block_on(db.register_schema_view(v1())).unwrap();
    block_on(db.activate_schema_with_lenses(with_copy(), &[copy_title()])).unwrap();

    insert(&old, "todos", 2, "title", "b");
    assert_eq!(read(&db, "todos", "search_title"), texts(&["a", "b"]));

    // The copy is a column of its own (SPEC 4 §4.6): a write of the source,
    // by either version, leaves it, and the new version writes it alone.
    update(&old, "todos", 1, "title", "a2");
    update(&db, "todos", 2, "title", "b2");
    assert_eq!(read(&db, "todos", "search_title"), texts(&["a", "b"]));
    update(&db, "todos", 1, "search_title", "mine");
    assert_eq!(read(&db, "todos", "title"), texts(&["a2", "b2"]));
    assert_eq!(read(&db, "todos", "search_title"), texts(&["mine", "b"]));

    // A reopened store keeps the rule.
    drop((old, db));
    let db = open_at(with_copy(), &path);
    let old = block_on(db.register_schema_view(v1())).unwrap();
    insert(&old, "todos", 3, "title", "c");
    assert_eq!(
        read(&db, "todos", "search_title"),
        texts(&["mine", "b", "c"])
    );
}

#[test]
fn a_row_a_client_on_the_old_schema_inserts_gets_the_copy() {
    let server = server(v1(), vec![(with_copy(), copy_title())]);
    let old = block_on(Db::open(config(v1(), 1, user(1)))).unwrap();
    let new = block_on(Db::open(config(with_copy(), 2, user(2)))).unwrap();
    let (old_client, old_server) = pipe();
    let (new_client, new_server) = pipe();
    let _old_link = block_on(old.connect_upstream(old_client));
    let _new_link = block_on(new.connect_upstream(new_client));
    server.accept_subscriber(old_server, user(1));
    server.accept_subscriber(new_server, user(2));
    let new_query = new.prepare_query(&Query::from("todos")).unwrap();
    let _new_stream = block_on(new.subscribe(&new_query, ReadOpts::default())).unwrap();

    insert(&old, "todos", 1, "title", "from old");
    pump(&server, &[&old, &new]);
    assert_eq!(read(&server, "todos", "search_title"), texts(&["from old"]));
    assert_eq!(
        read_query(&new, Query::from("todos"), "search_title", local()),
        texts(&["from old"])
    );
}

#[test]
fn a_copy_must_name_a_new_column_of_the_source_type() {
    let db = block_on(Db::open(config(v1(), 1, AuthorSubject::SYSTEM))).unwrap();
    let missing = lens(
        &v1(),
        &with_copy(),
        "todos",
        "todos",
        vec![LensOp::CopyColumn {
            from: "nope".into(),
            to: "search_title".into(),
        }],
    );
    let error = block_on(db.activate_schema_with_lenses(with_copy(), &[missing])).unwrap_err();
    assert!(error.to_string().contains("no `nope`"), "{error}");

    let typed = schema(vec![
        todos("todos", "title").column("search_title", ColumnType::Integer),
    ]);
    let retyped = lens(
        &v1(),
        &typed,
        "todos",
        "todos",
        vec![LensOp::CopyColumn {
            from: "title".into(),
            to: "search_title".into(),
        }],
    );
    let error = block_on(db.activate_schema_with_lenses(typed, &[retyped])).unwrap_err();
    assert!(error.to_string().contains("another type"), "{error}");
}

#[test]
fn the_identity_transform_is_accepted_and_changes_no_value() {
    let db = block_on(Db::open(config(v1(), 1, AuthorSubject::SYSTEM))).unwrap();
    insert(&db, "todos", 1, "title", "a");
    let identity = lens(
        &v1(),
        &v2(),
        "todos",
        "todos",
        vec![
            LensOp::RenameColumn {
                from: "title".into(),
                to: "name".into(),
            },
            LensOp::TransformColumn {
                column: "name".into(),
                transform: "jazz.identity".into(),
            },
        ],
    );
    block_on(db.activate_schema_with_lenses(v2(), &[identity])).unwrap();
    let old = block_on(db.register_schema_view(v1())).unwrap();
    assert_eq!(read(&db, "todos", "name"), texts(&["a"]));
    update(&old, "todos", 1, "title", "b");
    assert_eq!(read(&db, "todos", "name"), texts(&["b"]));
}

#[test]
fn an_unregistered_transform_is_rejected() {
    let db = block_on(Db::open(config(v1(), 1, AuthorSubject::SYSTEM))).unwrap();
    let upper = lens(
        &v1(),
        &v2(),
        "todos",
        "todos",
        vec![
            LensOp::RenameColumn {
                from: "title".into(),
                to: "name".into(),
            },
            LensOp::TransformColumn {
                column: "name".into(),
                transform: "app.uppercase".into(),
            },
        ],
    );
    let error = block_on(db.activate_schema_with_lenses(v2(), &[upper])).unwrap_err();
    assert_eq!(error.code, jazz::db::ErrorCode::Schema);
    assert!(
        error.to_string().contains("unregistered transform"),
        "{error}"
    );
}

/// v1 without `rank`.
fn without_rank() -> JazzSchema {
    schema(vec![
        TableSchemaBuilder::new("todos")
            .column("title", ColumnType::Text)
            .index_only(["title"])
            .policies(allow_all_policies()),
    ])
}

#[test]
fn a_row_a_newer_version_inserts_gives_a_dropped_column_its_backwards_default() {
    let db = block_on(Db::open(config(v1(), 1, AuthorSubject::SYSTEM))).unwrap();
    block_on(db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), text("old")),
            ("rank".to_owned(), Value::I64(3)),
        ]),
        InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap();
    let drop_rank = lens(
        &v1(),
        &without_rank(),
        "todos",
        "todos",
        vec![LensOp::DropColumn {
            column: "rank".into(),
            backwards_default: Value::I64(7),
        }],
    );
    block_on(db.activate_schema_with_lenses(without_rank(), &[drop_rank])).unwrap();
    let old = block_on(db.register_schema_view(v1())).unwrap();

    insert(&db, "todos", 2, "title", "new");
    update(&db, "todos", 1, "title", "old2");
    assert_eq!(
        read(&old, "todos", "rank"),
        vec![Value::I64(3), Value::I64(7)],
        "an update keeps the stored value; an insert takes the backwards default"
    );
}

/// Nico's review, point 6: a replica stores rows as its upstream does and
/// learns each version's names from it. A client store reopened under a
/// newer version that renamed a column reads the rows it cached by the new
/// name, offline (it learned the version's names while connected) and
/// after reconnecting.
#[test]
fn a_client_store_upgraded_across_a_rename_reads_its_cached_rows_by_the_new_name() {
    let server = server(v1(), vec![(v2(), rename_title())]);
    seed(&server, "todos", 1, &[("name", text("from server"))]);
    let path = scratch("upgrade-rename");
    {
        let client = block_on(Db::open_at(
            config(v1(), 1, user(1)),
            &path,
            FileOptions::default(),
        ))
        .unwrap();
        let (c, s) = pipe();
        let _link = block_on(client.connect_upstream(c));
        server.accept_subscriber(s, user(1));
        let q = client.prepare_query(&Query::from("todos")).unwrap();
        let _stream = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
        pump(&server, &[&client]);
        assert_eq!(column(&client, "title"), [Some(text("from server"))]);
    }
    // The app ships v2 (title renamed to name); the store reopens under it.
    let client = block_on(Db::open_at(
        config(v2(), 1, user(1)),
        &path,
        FileOptions::default(),
    ))
    .unwrap();
    assert_eq!(
        column(&client, "name"),
        [Some(text("from server"))],
        "offline, by the names the upstream gave v2"
    );
    let (c, s) = pipe();
    let _link = block_on(client.connect_upstream(c));
    server.accept_subscriber(s, user(1));
    let q = client.prepare_query(&Query::from("todos")).unwrap();
    let _stream = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    assert_eq!(column(&client, "name"), [Some(text("from server"))]);
}

/// A client opened under a version it never learned the upstream's names
/// for names its renamed column on its own; once connected it takes the
/// upstream's names, and its pending write, staged under the version
/// before, uploads under that version and reads by the new name.
#[test]
fn a_pending_write_staged_before_a_client_upgrade_uploads_and_reads_by_the_new_name() {
    let server = server(v1(), vec![(v2(), rename_title())]);
    let path = scratch("upgrade-pending");
    {
        let client = block_on(Db::open_at(
            config(v1(), 1, user(1)),
            &path,
            FileOptions::default(),
        ))
        .unwrap();
        let (c, _s) = pipe();
        let _link = block_on(client.connect_upstream(c));
        insert(&client, "todos", 7, "title", "offline v1 write");
        block_on(client.tick()).unwrap();
    }
    let client = block_on(Db::open_at(
        config(v2(), 1, user(1)),
        &path,
        FileOptions::default(),
    ))
    .unwrap();
    let (c, s) = pipe();
    let _link = block_on(client.connect_upstream(c));
    server.accept_subscriber(s, user(1));
    pump(&server, &[&client]);
    assert!(client.take_upstream_rejections().is_empty());
    assert_eq!(read(&server, "todos", "name"), [text("offline v1 write")]);
    assert_eq!(column(&client, "name"), [Some(text("offline v1 write"))]);
}

fn column(db: &Db, name: &str) -> Vec<Option<Value>> {
    let prepared = db.prepare_query(&Query::from("todos")).unwrap();
    block_on(db.all(&prepared, local()))
        .unwrap()
        .iter()
        .map(|row| {
            row.cells()
                .find(|(n, _)| *n == name)
                .and_then(|(_, v)| v.cloned())
        })
        .collect()
}

/// A worker opened under v2 before it learned the authority's names; a
/// tab connects to it first; then the worker connects and mirrors the
/// authority's catalogue. The tab is sent the worker's new names, so it
/// still reads `name`.
#[test]
fn a_tab_behind_a_worker_reads_a_renamed_column_after_the_worker_mirrors() {
    use jazz::db::RemoteLinkHint;
    let server = server(v1(), vec![(v2(), rename_title())]);
    seed(&server, "todos", 1, &[("name", text("from server"))]);
    let worker = block_on(Db::open(config(v2(), 0xa0, user(0xa0)))).unwrap();
    let tab = block_on(Db::open(config(v2(), 0xa1, user(0xa0)))).unwrap();
    tab.set_non_durable_client();
    tab.set_remote_link_hint(RemoteLinkHint::Live);
    let (tc, ts) = pipe();
    let _tab_link = block_on(tab.connect_upstream(tc));
    worker.accept_subscriber(ts, user(0xa0));
    pump(&worker, &[&tab]);

    let (wc, ws) = pipe();
    let _worker_link = block_on(worker.connect_upstream(wc));
    server.accept_subscriber(ws, user(0xa0));
    worker.set_remote_link_hint(RemoteLinkHint::Live);
    let q = tab.prepare_query(&Query::from("todos")).unwrap();
    let _stream = block_on(tab.subscribe(&q, ReadOpts::default())).unwrap();
    for _ in 0..6 {
        block_on(tab.tick()).unwrap();
        block_on(worker.tick()).unwrap();
        block_on(server.tick()).unwrap();
        block_on(worker.tick()).unwrap();
        block_on(tab.tick()).unwrap();
    }
    assert_eq!(
        column(&worker, "name"),
        [Some(text("from server"))],
        "worker"
    );
    assert_eq!(column(&tab, "name"), [Some(text("from server"))], "tab");
}

/// A store that keeps a table under a name the upstream stores another
/// table as cannot mirror it yet (their rows would have to merge): it
/// stops syncing with that upstream and says why on its remote reads,
/// while local reads keep being served and nothing is half renamed.
#[test]
fn a_catalogue_the_store_cannot_mirror_refuses_remote_reads_and_keeps_local_ones() {
    let server = server(v1(), vec![(v2(), rename_title()), (v3(), rename_todos())]);
    seed(&server, "tasks", 1, &[("name", text("from server"))]);
    let path = scratch("unmirrorable");
    {
        // Offline under v1, then v3 without the lenses between: `tasks` is
        // stored here as itself, beside v1's `todos`.
        let client = open_at(v1(), &path);
        insert(&client, "todos", 2, "title", "v1 row");
    }
    let client = open_at(v3(), &path);
    insert(&client, "tasks", 3, "name", "local task");
    let (c, s) = pipe();
    let _link = block_on(client.connect_upstream(c));
    server.accept_subscriber(s, AuthorSubject::SYSTEM);
    let q = client.prepare_query(&Query::from("tasks")).unwrap();
    let mut stream = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    let mut rejected = None;
    while let Some(event) = stream.try_next_event() {
        if let jazz::db::SubscriptionEvent::Rejected { code } = event {
            rejected = Some(code);
        }
    }
    let code = rejected.expect("the remote read says why");
    // Unsupported catalogue mirroring is a Query error on main. Preserve
    // that classification when exposing the client-safe failure category.
    assert_eq!(
        code,
        jazz::db::SubscriptionServerFailureCode::QueryValidation
    );
    assert_eq!(
        read_query(&client, Query::from("tasks"), "name", local()),
        [text("local task")]
    );
    let stored = stored(&path);
    assert!(
        stored.contains_key("tasks") && stored.contains_key("todos"),
        "{stored:?}"
    );
}
