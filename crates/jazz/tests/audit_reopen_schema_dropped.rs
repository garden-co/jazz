//! Audit (state-space, reopen-schema): what a store rebuilds on reopen must
//! equal what it built incrementally while running. A reopened store
//! rebuilds its physical tables from `PRAGMA table_info` (names only, every
//! column a placeholder `Nullable(U64)`, see `Table::stored_only`) and
//! unions only the schema it opens under. Stored columns and tables that
//! only *other* admitted versions name keep the placeholder type, yet the
//! stored view (`other_versions_column`) and the dropped-column backwards
//! defaults (`stored_op`) read their types from it.
//!
//! Each test runs the same history twice, once on a live store and once
//! across a reopen, and asserts the two agree.
//!
//! ```text
//! v1 todos(title, note) ──lens: drop note (backwards "n/a")──► v2 todos(title)
//!   live:    open v1 ─ activate v2 ─ write ─ read          (ok)
//!   reopen:  open v1 ─ activate v2 ─ close ─ open v2 ─ write ─ read   (?)
//! ```

use std::cell::RefCell;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, ReadTier, SyncMessage,
    Transport, TransportError, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{LensOp, MigrationLens, TableLens};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use crate::common::{allow_all_policies, compile_schema};

fn todos(with_note: bool) -> TableSchemaBuilder {
    let t = TableSchemaBuilder::new("todos").column("title", ColumnType::Text);
    let t = if with_note {
        t.column("note", ColumnType::Text)
    } else {
        t
    };
    t.policies(allow_all_policies())
}

fn tags() -> TableSchemaBuilder {
    TableSchemaBuilder::new("tags")
        .column("label", ColumnType::Text)
        .policies(allow_all_policies())
}

/// `todos(title, note)` and `tags(label)`.
fn v1() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(todos(true))
            .table(tags())
            .build(),
    )
}

/// v1 without `todos.note`; `tags` kept.
fn v2() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(todos(false))
            .table(tags())
            .build(),
    )
}

/// v1 without the `tags` table (a dropTables migration).
fn v2_no_tags() -> JazzSchema {
    compile_schema(&SchemaBuilder::new().table(todos(true)).build())
}

fn drop_note() -> MigrationLens {
    MigrationLens::new(
        v1().version_id(),
        v2().version_id(),
        vec![
            TableLens {
                source_table: "todos".into(),
                target_table: "todos".into(),
                ops: vec![LensOp::DropColumn {
                    column: "note".into(),
                    backwards_default: Value::String("n/a".into()),
                }],
            },
            TableLens {
                source_table: "tags".into(),
                target_table: "tags".into(),
                ops: vec![],
            },
        ],
    )
    .unwrap()
}

fn drop_tags() -> MigrationLens {
    MigrationLens::new(
        v1().version_id(),
        v2_no_tags().version_id(),
        vec![TableLens {
            source_table: "todos".into(),
            target_table: "todos".into(),
            ops: vec![],
        }],
    )
    .unwrap()
}

fn config(schema: JazzSchema, target: &JazzSchema, lens: &MigrationLens, node: u8) -> DbConfig {
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
    .with_migrations(vec![v1(), target.clone()], vec![lens.clone()])
}

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "jazz-audit-reopen-schema-{}-{name}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("store.sqlite")
}

fn text(value: &str) -> Value {
    Value::String(value.to_owned())
}

fn insert(db: &Db, table: &str, n: u8, cells: &[(&str, Value)]) -> Result<(), String> {
    block_on(
        db.insert(
            table,
            cells
                .iter()
                .map(|(c, v)| ((*c).to_owned(), v.clone()))
                .collect(),
            InsertOptions {
                row_id: Some(RowUuid::from_bytes([n; 16])),
                ..Default::default()
            },
        ),
    )
    .map(|_| ())
    .map_err(|e| e.to_string())
}

fn read(db: &Db, table: &str, column: &str) -> Result<Vec<Option<Value>>, String> {
    let prepared = db
        .prepare_query(&Query::from(table))
        .map_err(|e| e.to_string())?;
    let mut rows: Vec<_> = block_on(db.all(
        &prepared,
        ReadOpts {
            tier: ReadTier::LocalOnly,
            ..ReadOpts::default()
        },
    ))
    .map_err(|e| e.to_string())?
    .iter()
    .map(|row| {
        (
            row.row_uuid(),
            row.cells()
                .find(|(name, _)| *name == column)
                .and_then(|(_, v)| v.cloned()),
        )
    })
    .collect();
    rows.sort_by_key(|(id, _)| *id);
    Ok(rows.into_iter().map(|(_, v)| v).collect())
}

/// Open a store on v1, write a row, activate `target` with `lens`; with
/// `reopen`, close it and open it again under `target`.
fn history(name: &str, target: &JazzSchema, lens: &MigrationLens, reopen: bool) -> (Db, PathBuf) {
    let path = scratch(name);
    let db = block_on(Db::open_at(
        config(v1(), target, lens, 1),
        &path,
        FileOptions::default(),
    ))
    .unwrap();
    db.set_authority();
    insert(
        &db,
        "todos",
        1,
        &[("title", text("t1")), ("note", text("n1"))],
    )
    .unwrap();
    insert(&db, "tags", 1, &[("label", text("l1"))]).unwrap();
    block_on(db.activate_schema_with_lenses(target.clone(), &[lens.clone()])).unwrap();
    if !reopen {
        return (db, path);
    }
    drop(db);
    let db = block_on(Db::open_at(
        config(target.clone(), target, lens, 1),
        &path,
        FileOptions::default(),
    ))
    .unwrap();
    db.set_authority();
    (db, path)
}

/// The current version's own inserts into a table with a dropped column
/// (which name the dropped column's backwards default, SPEC 8.9) behave the
/// same on a live and on a reopened store.
#[test]
fn audit_reopen_schema_new_version_insert_with_dropped_column() {
    let mut outcomes = Vec::new();
    for reopen in [false, true] {
        let (db, _) = history(&format!("own-{reopen}"), &v2(), &drop_note(), reopen);
        let written = insert(&db, "todos", 2, &[("title", text("t2"))]);
        let old = block_on(db.register_schema_view(v1())).unwrap();
        outcomes.push((reopen, written, read(&old, "todos", "note")));
    }
    eprintln!("OUTCOMES {outcomes:?}");
    assert_eq!(
        outcomes[0].1, outcomes[1].1,
        "live vs reopened: {outcomes:?}"
    );
    assert_eq!(
        outcomes[0].2, outcomes[1].2,
        "live vs reopened: {outcomes:?}"
    );
}

/// A version that still names the dropped column (an older bundle's
/// session, a view) writes it the same on a live and a reopened store.
#[test]
fn audit_reopen_schema_old_version_writes_a_dropped_column() {
    let mut outcomes = Vec::new();
    for reopen in [false, true] {
        let (db, _) = history(&format!("old-{reopen}"), &v2(), &drop_note(), reopen);
        let old = block_on(db.register_schema_view(v1())).unwrap();
        let written = insert(
            &old,
            "todos",
            3,
            &[("title", text("t3")), ("note", text("n3"))],
        );
        outcomes.push((reopen, written, read(&old, "todos", "note")));
    }
    eprintln!("OUTCOMES {outcomes:?}");
    assert_eq!(
        outcomes[0].1, outcomes[1].1,
        "live vs reopened: {outcomes:?}"
    );
    assert_eq!(
        outcomes[0].2, outcomes[1].2,
        "live vs reopened: {outcomes:?}"
    );
}

/// A table the current version dropped (dropTables), still written by a
/// version that names it, behaves the same live and reopened.
#[test]
fn audit_reopen_schema_old_version_writes_a_dropped_table() {
    let mut outcomes = Vec::new();
    for reopen in [false, true] {
        let (db, _) = history(
            &format!("tbl-{reopen}"),
            &v2_no_tags(),
            &drop_tags(),
            reopen,
        );
        let old = block_on(db.register_schema_view(v1())).unwrap();
        let written = insert(&old, "tags", 2, &[("label", text("l2"))]);
        outcomes.push((reopen, written, read(&old, "tags", "label")));
    }
    eprintln!("OUTCOMES {outcomes:?}");
    assert_eq!(
        outcomes[0].1, outcomes[1].1,
        "live vs reopened: {outcomes:?}"
    );
    assert_eq!(
        outcomes[0].2, outcomes[1].2,
        "live vs reopened: {outcomes:?}"
    );
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

fn pump(server: &Db, client: &Db) {
    for _ in 0..8 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
}

/// A client still on v1 (offline across the publish, or pinned to the
/// older bundle) uploads a write naming the column v2 dropped. The server
/// accepts it the same whether or not it restarted since the migration.
///
/// ```text
/// alice(v1) ──insert {title, note}──► server(v2, live | reopened)
/// ```
#[test]
fn audit_reopen_schema_old_client_write_to_restarted_server() {
    let mut outcomes = Vec::new();
    for reopen in [false, true] {
        let (server, _) = history(&format!("sync-{reopen}"), &v2(), &drop_note(), reopen);
        let alice = block_on(Db::open(config(v1(), &v2(), &drop_note(), 7))).unwrap();
        alice.track_upstream_rejections();
        let (c, s) = pipe();
        let _link = block_on(alice.connect_upstream(c));
        server
            .accept_subscriber(s, AuthorSubject::for_test_bytes([7; 16]))
            .unwrap();
        pump(&server, &alice);
        let written = insert(
            &alice,
            "todos",
            4,
            &[("title", text("t4")), ("note", text("n4"))],
        );
        pump(&server, &alice);
        let old = block_on(server.register_schema_view(v1())).unwrap();
        outcomes.push((
            reopen,
            written,
            read(&server, "todos", "title"),
            read(&old, "todos", "note"),
            alice
                .take_upstream_rejections()
                .into_iter()
                .map(|r| format!("{:?}", r.error))
                .collect::<Vec<_>>(),
        ));
    }
    eprintln!("OUTCOMES {outcomes:?}");
    assert_eq!(
        (&outcomes[0].2, &outcomes[0].3, &outcomes[0].4),
        (&outcomes[1].2, &outcomes[1].3, &outcomes[1].4),
        "live vs reopened: {outcomes:?}"
    );
}
