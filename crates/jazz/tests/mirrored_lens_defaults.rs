//! A column a migration lens adds with a default reads that default in the
//! rows written before it, on every store: the authority adds it with the
//! lens's default, and a replica that mirrors the version from its upstream
//! (so never sees the lens) adds it with the same one (g4, rc.6 gate).
//!
//! The adopter path: a local-first browser store holds rows written under
//! v1; the app redeploys with v2, which adds `currency` with
//! `m.add.string({ default: "EUR" })`; the still-open v1 tab hears of v2
//! from the server before it reloads onto the v2 bundle. Its rows used to
//! read `currency: null` forever, because mirroring v2 added the column
//! with no default and reopening with the lens found it already stored:
//! the catalogue now carries each stored column's `ADD COLUMN` default.

use crate::common;

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, SyncMessage, Transport,
    TransportError, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{LensOp, MigrationLens, TableLens};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn expenses(extra: impl FnOnce(TableSchemaBuilder) -> TableSchemaBuilder) -> SchemaBuilder {
    SchemaBuilder::new().table(
        extra(TableSchemaBuilder::new("expenses").column("title", ColumnType::Text))
            .policies(allow_all_policies()),
    )
}

/// `expenses(title)`.
fn v1() -> JazzSchema {
    compile_schema(&expenses(|t| t).build())
}

fn status() -> ColumnType {
    ColumnType::Enum {
        variants: vec!["open".to_owned(), "paid".to_owned()],
    }
}

/// v1 plus a required `currency: text` and `status: enum`.
fn v2() -> JazzSchema {
    compile_schema(
        &expenses(|t| {
            t.column("currency", ColumnType::Text)
                .column("status", status())
        })
        .build(),
    )
}

/// `currency: m.add.string({ default: "EUR" })`, and
/// `status: m.add.enum(..., { default: "paid" })`.
fn v1_to_v2() -> MigrationLens {
    MigrationLens::new(
        v1().version_id(),
        v2().version_id(),
        vec![TableLens {
            source_table: "expenses".into(),
            target_table: "expenses".into(),
            ops: vec![
                LensOp::AddColumn {
                    column: "currency".into(),
                    default: Value::String("EUR".into()),
                },
                LensOp::AddColumn {
                    column: "status".into(),
                    default: Value::String("paid".into()),
                },
            ],
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

fn authority_at(path: &Path) -> Db {
    let db = block_on(Db::open_at(config(v1(), 0), path, FileOptions::default())).unwrap();
    db.set_authority();
    db
}

/// The browser's file, opened by the v1 bundle (which ships no lens).
fn v1_tab(path: &Path) -> Db {
    block_on(Db::open_at(config(v1(), 1), path, FileOptions::default())).unwrap()
}

/// The same file, opened by the v2 bundle with the app's migrations.
fn v2_tab(path: &Path) -> Db {
    block_on(Db::open_at(
        config(v2(), 1).with_migrations(vec![v1()], vec![v1_to_v2()]),
        path,
        FileOptions::default(),
    ))
    .unwrap()
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

fn link(client: &Db, server: &Db) -> jazz::db::Upstream {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let link = block_on(client.connect_upstream(Box::new(Pipe {
        out: up.clone(),
        inbound: down.clone(),
    })));
    server
        .accept_subscriber(
            Box::new(Pipe {
                out: down,
                inbound: up,
            }),
            AuthorSubject::SYSTEM,
        )
        .unwrap();
    link
}

fn pump(server: &Db, clients: &[&Db]) {
    for _ in 0..8 {
        for client in clients {
            block_on(client.tick()).unwrap();
        }
        block_on(server.tick()).unwrap();
    }
    for client in clients {
        block_on(client.tick()).unwrap();
    }
}

fn insert(db: &Db, n: u8, columns: &[(&str, &str)]) {
    block_on(
        db.insert(
            "expenses",
            columns
                .iter()
                .map(|(column, value)| (column.to_string(), Value::String(value.to_string())))
                .collect::<BTreeMap<_, _>>(),
            InsertOptions {
                row_id: Some(RowUuid::from_bytes([n; 16])),
                ..Default::default()
            },
        ),
    )
    .unwrap();
}

/// Each local row as (title, currency, status), by row id.
fn local_rows(db: &Db) -> Vec<(Option<Value>, Option<Value>, Option<Value>)> {
    let prepared = db.prepare_query(&Query::from("expenses")).unwrap();
    let mut rows: Vec<_> = block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|row| {
            let cell = |name: &str| {
                row.cells()
                    .find(|(column, _)| *column == name)
                    .and_then(|(_, value)| value.cloned())
            };
            (
                row.row_uuid(),
                (cell("title"), cell("currency"), cell("status")),
            )
        })
        .collect();
    rows.sort_by_key(|(id, _)| *id);
    rows.into_iter().map(|(_, row)| row).collect()
}

/// The `ADD COLUMN` default (as SQL) of `currency` in the file at `path`,
/// in the table and its history table.
fn currency_defaults(path: &Path) -> Vec<(String, Option<String>)> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let tables: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '\\_%' ESCAPE '\\'")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let mut out = Vec::new();
    for table in tables {
        let mut info = conn
            .prepare(&format!("PRAGMA table_info(\"{table}\")"))
            .unwrap();
        let columns = info
            .query_map([], |row| {
                Ok((row.get::<_, String>(1)?, row.get::<_, Option<String>>(4)?))
            })
            .unwrap();
        for column in columns {
            let (column, default) = column.unwrap();
            if column.starts_with("currency") {
                out.push((table.clone(), default));
            }
        }
    }
    out
}

fn row(title: &str, currency: &str, status: &str) -> (Option<Value>, Option<Value>, Option<Value>) {
    (
        Some(Value::String(title.into())),
        Some(Value::String(currency.into())),
        Some(Value::String(status.into())),
    )
}

/// `status` is an enum: its default is stored as a tag, and mirrored as
/// one.
///
/// ```
/// life 1: server(v1)              ── tab(v1) ── insert a, b ──► synced
/// life 2: server(v1 + v2, lens)   ── tab(v1) still open: mirrors v2, inserts c
///         reload: tab(v2, lens)   ── reads a, b, c ──► currency "EUR" (was: null)
/// ```
#[test]
fn a_local_first_store_reloaded_onto_v2_reads_the_lens_default_in_its_v1_rows() {
    let dir = tempfile::tempdir().unwrap();
    let (core, browser) = (
        dir.path().join("core.sqlite"),
        dir.path().join("tab.sqlite"),
    );
    {
        let server = authority_at(&core);
        let tab = v1_tab(&browser);
        let _link = link(&tab, &server);
        pump(&server, &[&tab]);
        insert(&tab, 1, &[("title", "a")]);
        insert(&tab, 2, &[("title", "b")]);
        pump(&server, &[&tab]);
        drop(tab);
    }
    // The v2 backend: the server admits v2 with its lens, as a published
    // bundle does, while the v1 tab stays open and writes once more.
    let server = authority_at(&core);
    block_on(server.admit_schema_with_lenses(v2(), &[v1_to_v2()])).unwrap();
    {
        let tab = v1_tab(&browser);
        let _link = link(&tab, &server);
        pump(&server, &[&tab]);
        insert(&tab, 3, &[("title", "c")]);
        pump(&server, &[&tab]);
    }
    // It mirrored v2 from the server, adding `currency` as the server did.
    let eur = Some("'EUR'".to_owned());
    assert_eq!(
        currency_defaults(&browser),
        vec![
            ("expenses".to_owned(), eur.clone()),
            ("expenses__h".to_owned(), eur.clone())
        ]
    );
    let tab = v2_tab(&browser);
    let expected = vec![
        row("a", "EUR", "paid"),
        row("b", "EUR", "paid"),
        row("c", "EUR", "paid"),
    ];
    assert_eq!(local_rows(&tab), expected, "reloaded, before connecting");
    let _link = link(&tab, &server);
    pump(&server, &[&tab]);
    assert_eq!(local_rows(&tab), expected, "reloaded and connected");
    insert(
        &tab,
        4,
        &[("title", "d"), ("currency", "USD"), ("status", "open")],
    );
    pump(&server, &[&tab]);

    // A store that never saw v1 reads the same.
    let fresh = block_on(Db::open(
        config(v2(), 2).with_migrations(vec![v1()], vec![v1_to_v2()]),
    ))
    .unwrap();
    let _fresh_link = link(&fresh, &server);
    let all = fresh.prepare_query(&Query::from("expenses")).unwrap();
    let _held = block_on(fresh.subscribe(&all, ReadOpts::default())).unwrap();
    pump(&server, &[&fresh]);
    let mut everyone = expected.clone();
    everyone.push(row("d", "USD", "open"));
    assert_eq!(local_rows(&fresh), everyone, "a fresh store");
}
