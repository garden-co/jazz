//! Audit (state-space, reopen-schema): siblings of #4769. A store that
//! admitted v1 and then v2 (a createTables / added-column migration) must
//! behave the same live and after a reopen, whichever version it reopens
//! under (the current one, or an older one: a rolled-back backend, an
//! older bundle's process), and for writes through either version.
//!
//! ```text
//! v1 parents(title)
//! v2 parents(title, due: timestamp = 5, ref: uuid?) + children(parentId -> parents, name)
//!   live:    open v1 ─ activate v2 ─ write via X ─ read
//!   reopen:  open v1 ─ activate v2 ─ close ─ open Y ─ write via X ─ read
//! ```

use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, ReadTier, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{LensOp, MigrationLens, TableLens};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use crate::common::{allow_all_policies, compile_schema};

fn parents(v2: bool) -> TableSchemaBuilder {
    let t = TableSchemaBuilder::new("parents").column("title", ColumnType::Text);
    let t = if v2 {
        t.column_with_default(
            "due",
            ColumnType::Timestamp,
            jazz::tools::Value::Timestamp(5),
        )
        .nullable_column("ref", ColumnType::Uuid)
        .column_with_default(
            "state",
            ColumnType::Enum {
                variants: vec!["open".into(), "done".into()],
            },
            jazz::tools::Value::Text("open".into()),
        )
    } else {
        t
    };
    t.policies(allow_all_policies())
}

fn v1() -> JazzSchema {
    compile_schema(&SchemaBuilder::new().table(parents(false)).build())
}

fn v2() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(parents(true))
            .table(
                TableSchemaBuilder::new("children")
                    .fk_column("parentId", "parents")
                    .column("name", ColumnType::Text)
                    .index_only(["parentId"])
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn lens() -> MigrationLens {
    MigrationLens::new(
        v1().version_id(),
        v2().version_id(),
        vec![TableLens {
            source_table: "parents".into(),
            target_table: "parents".into(),
            ops: vec![
                LensOp::AddColumn {
                    column: "due".into(),
                    default: Value::U64(5),
                },
                LensOp::AddColumn {
                    column: "ref".into(),
                    default: Value::Nullable(None),
                },
                LensOp::AddColumn {
                    column: "state".into(),
                    default: Value::String("open".into()),
                },
            ],
        }],
    )
    .unwrap()
}

fn config(schema: JazzSchema) -> DbConfig {
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
    .with_migrations(vec![v1(), v2()], vec![lens()])
}

fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "jazz-audit-reopen-added-{}-{name}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("store.sqlite")
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn insert(db: &Db, table: &str, n: u8, cells: Vec<(&str, Value)>) -> Result<(), String> {
    block_on(db.insert(
        table,
        cells.into_iter().map(|(c, v)| (c.to_owned(), v)).collect(),
        InsertOptions {
            row_id: Some(row(n)),
            ..Default::default()
        },
    ))
    .map(|_| ())
    .map_err(|e| e.to_string())
}

fn dump(db: &Db, table: &str) -> Result<Vec<Vec<(String, Option<Value>)>>, String> {
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
    .map(|r| {
        (
            r.row_uuid(),
            r.cells()
                .map(|(n, v)| (n.to_owned(), v.cloned()))
                .collect::<Vec<_>>(),
        )
    })
    .collect();
    rows.sort_by_key(|(id, _)| *id);
    Ok(rows.into_iter().map(|(_, c)| c).collect())
}

#[derive(Clone, Copy, Debug)]
enum Reopen {
    Live,
    UnderV2,
    UnderV1,
}

/// Open v1, insert a parent, activate v2, insert a child; then reopen as
/// asked. Returns the store (with v2 current or as `under`).
fn history(name: &str, reopen: Reopen) -> Db {
    let path = scratch(name);
    let db = block_on(Db::open_at(config(v1()), &path, FileOptions::default())).unwrap();
    db.set_authority();
    insert(
        &db,
        "parents",
        1,
        vec![("title", Value::String("p1".into()))],
    )
    .unwrap();
    block_on(db.activate_schema_with_lenses(v2(), &[lens()])).unwrap();
    insert(
        &db,
        "children",
        1,
        vec![
            ("parentId", Value::Uuid(uuid::Uuid::from_bytes([1; 16]))),
            ("name", Value::String("c1".into())),
        ],
    )
    .unwrap();
    let under = match reopen {
        Reopen::Live => return db,
        Reopen::UnderV2 => v2(),
        Reopen::UnderV1 => v1(),
    };
    drop(db);
    let db = block_on(Db::open_at(config(under), &path, FileOptions::default())).unwrap();
    db.set_authority();
    db
}

fn scenario(db: &Db) -> Vec<String> {
    let v2view = block_on(db.register_schema_view(v2())).unwrap();
    let v1view = block_on(db.register_schema_view(v1())).unwrap();
    let parent = Value::Uuid(uuid::Uuid::from_bytes([1; 16]));
    vec![
        format!(
            "v2 insert child: {:?}",
            insert(
                &v2view,
                "children",
                2,
                vec![
                    ("parentId", parent.clone()),
                    ("name", Value::String("c2".into()))
                ]
            )
        ),
        format!(
            "v2 insert parent with new cols: {:?}",
            insert(
                &v2view,
                "parents",
                2,
                vec![
                    ("title", Value::String("p2".into())),
                    ("due", Value::U64(9)),
                    ("ref", Value::Nullable(Some(Box::new(parent.clone())))),
                    ("state", Value::String("done".into())),
                ]
            )
        ),
        format!(
            "v1 insert parent: {:?}",
            insert(
                &v1view,
                "parents",
                3,
                vec![("title", Value::String("p3".into()))]
            )
        ),
        format!("v2 parents: {:?}", dump(&v2view, "parents")),
        format!("v2 children: {:?}", dump(&v2view, "children")),
        format!("v1 parents: {:?}", dump(&v1view, "parents")),
    ]
}

fn compare(reopen: Reopen) {
    let live = scenario(&history(&format!("live-{reopen:?}"), Reopen::Live));
    let reopened = scenario(&history(&format!("re-{reopen:?}"), reopen));
    for (l, r) in live.iter().zip(&reopened) {
        eprintln!("LIVE     {l}\nREOPENED {r}");
    }
    assert_eq!(live, reopened, "live vs reopened ({reopen:?})");
}

/// Reopened under the current version (a dev-server restart, a Sandbox
/// core waking): writes through both versions behave as before.
#[test]
fn audit_reopen_schema_added_reopen_under_current() {
    compare(Reopen::UnderV2);
}

/// Reopened under the older version (a rollback, an older bundle's
/// process opening the store first): writes through both versions behave as
/// before.
#[test]
fn audit_reopen_schema_added_reopen_under_older() {
    compare(Reopen::UnderV1);
}

struct Pipe {
    out: std::rc::Rc<std::cell::RefCell<std::collections::VecDeque<jazz::db::SyncMessage>>>,
    inbound: std::rc::Rc<std::cell::RefCell<std::collections::VecDeque<jazz::db::SyncMessage>>>,
}

impl jazz::db::Transport for Pipe {
    fn send(&mut self, message: jazz::db::SyncMessage) -> Result<(), jazz::db::TransportError> {
        self.out.borrow_mut().push_back(message);
        Ok(())
    }
    fn try_recv(&mut self) -> Option<jazz::db::SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

fn link(client: &Db, server: &Db, who: u8) -> jazz::db::Upstream {
    let up = std::rc::Rc::default();
    let down = std::rc::Rc::default();
    let upstream = block_on(client.connect_upstream(Box::new(Pipe {
        out: std::rc::Rc::clone(&up),
        inbound: std::rc::Rc::clone(&down),
    })));
    server
        .accept_subscriber(
            Box::new(Pipe {
                out: down,
                inbound: up,
            }),
            AuthorSubject::for_test_bytes([who; 16]),
        )
        .unwrap();
    upstream
}

fn pump(server: &Db, client: &Db) {
    for _ in 0..8 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
}

fn subscribe_all(db: &Db, tables: &[&str]) -> Vec<jazz::db::SubscriptionStream> {
    tables
        .iter()
        .map(|t| {
            let q = db.prepare_query(&Query::from(*t)).unwrap();
            block_on(db.subscribe(&q, ReadOpts::default())).unwrap()
        })
        .collect()
}

/// A browser store written under v2, opened by an older tab or app build
/// still on v1 (one session, while the server keeps writing v2 rows), then
/// opened under v2 again: the v2 cells the v1 session received and the rows
/// it wrote read back as they would have without the v1 interlude.
///
/// ```text
/// server(v2) ──p1 {due 9, done}──► alice store (v2)   close
/// server(v2) ──p2 {due 11, done}─► alice store (v1)   insert p3  close
///                                  alice store (v2)   read p1, p2, p3 offline
/// ```
#[test]
fn audit_reopen_schema_added_replica_interlude_under_older_version() {
    let server = block_on(Db::open_history_complete(config(v1()))).unwrap();
    block_on(server.activate_schema_with_lenses(v2(), &[lens()])).unwrap();
    let parent = |n: u8, due: u64| {
        vec![
            ("title", Value::String(format!("p{n}"))),
            ("due", Value::U64(due)),
            ("state", Value::String("done".into())),
        ]
    };
    insert(&server, "parents", 1, parent(1, 9)).unwrap();
    let path = scratch("replica-interlude");
    let open = |schema: JazzSchema| {
        let mut config = config(schema);
        config.identity.node = NodeUuid::from_bytes([7; 16]);
        block_on(Db::open_at(config, &path, FileOptions::default())).unwrap()
    };
    {
        let alice = open(v2());
        let _link = link(&alice, &server, 7);
        let _subs = subscribe_all(&alice, &["parents", "children"]);
        pump(&server, &alice);
        eprintln!("ALICE v2 first: {:?}", dump(&alice, "parents"));
    }
    insert(&server, "parents", 2, parent(2, 11)).unwrap();
    {
        let alice = open(v1());
        let _link = link(&alice, &server, 7);
        let _subs = subscribe_all(&alice, &["parents"]);
        pump(&server, &alice);
        insert(
            &alice,
            "parents",
            3,
            vec![("title", Value::String("p3".into()))],
        )
        .unwrap();
        pump(&server, &alice);
        eprintln!("ALICE v1 interlude: {:?}", dump(&alice, "parents"));
    }
    let alice = open(v2());
    let got = dump(&alice, "parents").unwrap();
    eprintln!("ALICE v2 after: {got:?}");
    eprintln!("SERVER: {:?}", dump(&server, "parents"));
    let cell = |row: &Vec<(String, Option<Value>)>, c: &str| {
        row.iter()
            .find(|(n, _)| n == c)
            .and_then(|(_, v)| v.clone())
    };
    assert_eq!(got.len(), 3, "{got:?}");
    assert_eq!(cell(&got[0], "due"), Some(Value::U64(9)), "{got:?}");
    assert_eq!(cell(&got[1], "due"), Some(Value::U64(11)), "{got:?}");
    assert_eq!(
        cell(&got[1], "state"),
        Some(Value::String("done".into())),
        "{got:?}"
    );
    assert_eq!(cell(&got[2], "due"), Some(Value::U64(5)), "{got:?}");
    assert_eq!(
        cell(&got[2], "state"),
        Some(Value::String("open".into())),
        "{got:?}"
    );
}
