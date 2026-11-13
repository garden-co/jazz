//! A reopened store knows every stored column by the type the version that
//! stores it declared, not only the columns of the schema it reopens under
//! (#4769). A server core reopens under its genesis schema (the bundles
//! createJazz publishes activate nothing), so the tables and columns later
//! versions added are stored there but are not the opening schema's: their
//! writes and reads must still find each stored column's type.

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

fn parents(extra: impl FnOnce(TableSchemaBuilder) -> TableSchemaBuilder) -> TableSchemaBuilder {
    extra(TableSchemaBuilder::new("parents").column("title", ColumnType::Text))
        .policies(allow_all_policies())
}

fn kind() -> ColumnType {
    ColumnType::Enum {
        variants: vec!["file".to_owned(), "link".to_owned()],
    }
}

/// `parents(title)`: the genesis version.
fn v1() -> JazzSchema {
    compile_schema(&SchemaBuilder::new().table(parents(|t| t)).build())
}

/// v1 plus `parents.rank: bigint?` and a new table
/// `children(parentId: uuid, name: text, size: bigint, kind: enum, done: bool?)`,
/// as a generated migration with `createTables: { children: true }` adds it.
fn v2() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(parents(|t| t.nullable_column("rank", ColumnType::BigInt)))
            .table(
                TableSchemaBuilder::new("children")
                    .column("parentId", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .column("size", ColumnType::BigInt)
                    .column("kind", kind())
                    .nullable_column("done", ColumnType::Boolean)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

/// The lens a generated migration lowers to: `parents` gains `rank`;
/// `children` has no source table, so no table lens.
fn v1_to_v2() -> MigrationLens {
    MigrationLens::new(
        v1().version_id(),
        v2().version_id(),
        vec![TableLens {
            source_table: "parents".into(),
            target_table: "parents".into(),
            ops: vec![LensOp::AddColumn {
                column: "rank".into(),
                default: Value::Nullable(None),
            }],
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

/// The authority's file, opened under `schema` as a server core opens it.
fn authority_at(schema: JazzSchema, path: &Path) -> Db {
    let db = block_on(Db::open_at(config(schema, 0), path, FileOptions::default())).unwrap();
    db.set_authority();
    db
}

/// A replica on v2 that ships the app's migrations, as a backend client or
/// browser does, keeping the uploads its authority refuses.
fn replica(node: u8) -> Db {
    let db = block_on(Db::open(
        config(v2(), node).with_migrations(vec![v1()], vec![v1_to_v2()]),
    ))
    .unwrap();
    db.track_upstream_rejections();
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

fn parent_id() -> uuid::Uuid {
    uuid::Uuid::from_bytes([7; 16])
}

fn insert_child(db: &Db, n: u8, name: &str) {
    block_on(db.insert(
        "children",
        BTreeMap::from([
            ("parentId".to_owned(), Value::Uuid(parent_id())),
            ("name".to_owned(), Value::String(name.to_owned())),
            ("size".to_owned(), Value::I64(1000 + n as i64)),
            ("kind".to_owned(), Value::String("link".to_owned())),
            (
                "done".to_owned(),
                Value::Nullable(Some(Box::new(Value::Bool(true)))),
            ),
        ]),
        InsertOptions {
            row_id: Some(RowUuid::from_bytes([n; 16])),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn insert_parent(db: &Db, n: u8, rank: i64) {
    block_on(db.insert(
        "parents",
        BTreeMap::from([
            ("title".to_owned(), Value::String(format!("p{n}"))),
            (
                "rank".to_owned(),
                Value::Nullable(Some(Box::new(Value::I64(rank)))),
            ),
        ]),
        InsertOptions {
            row_id: Some(RowUuid::from_bytes([n; 16])),
            ..Default::default()
        },
    ))
    .unwrap();
}

/// Each row of `table` (by row id) as its `columns`' values.
fn rows(db: &Db, table: &str, columns: &[&str]) -> Vec<Vec<Option<Value>>> {
    let prepared = db.prepare_query(&Query::from(table)).unwrap();
    let mut rows: Vec<_> = block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|row| {
            let cells = columns
                .iter()
                .map(|column| {
                    row.cells()
                        .find(|(name, _)| name == column)
                        .and_then(|(_, value)| value.cloned())
                })
                .collect::<Vec<_>>();
            (row.row_uuid(), cells)
        })
        .collect();
    rows.sort_by_key(|(id, _)| *id);
    rows.into_iter().map(|(_, cells)| cells).collect()
}

fn child(n: u8, name: &str) -> Vec<Option<Value>> {
    vec![
        Some(Value::Uuid(parent_id())),
        Some(Value::String(name.to_owned())),
        Some(Value::I64(1000 + n as i64)),
        Some(Value::String("link".to_owned())),
        Some(Value::Nullable(Some(Box::new(Value::Bool(true))))),
    ]
}

const CHILD: &[&str] = &["parentId", "name", "size", "kind", "done"];

/// The authority admits v2 (a table and a column added by the app's
/// migration) without making it current, as a published bundle does, and
/// takes alice's v2 writes. Reopened under its genesis v1, as a server
/// core restarts, it still takes bob's v2 writes to the added table and
/// column, and a fresh replica reads every row back with its declared
/// types.
///
/// ```
/// life 1: authority(v1) ── admit v2 ── alice(v2) ──insert──► ok
/// life 2: authority(v1, reopened)    ── bob(v2)   ──insert──► ok (was: "does not match type U64")
///                                     ── carol(v2) ──read───► every row, typed
/// ```
#[test]
fn an_authority_reopened_under_its_genesis_takes_writes_to_what_later_versions_added() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("core.sqlite");
    {
        let server = authority_at(v1(), &path);
        block_on(server.admit_schema_with_lenses(v2(), &[v1_to_v2()])).unwrap();
        let alice = replica(1);
        let _link = link(&alice, &server);
        pump(&server, &[&alice]);
        insert_parent(&alice, 1, 10);
        insert_child(&alice, 2, "first life");
        pump(&server, &[&alice]);
        assert!(alice.take_upstream_rejections().is_empty());
    }

    let server = authority_at(v1(), &path);
    let bob = replica(2);
    let _bob_link = link(&bob, &server);
    pump(&server, &[&bob]);
    insert_parent(&bob, 3, 30);
    insert_child(&bob, 4, "after reopen");
    pump(&server, &[&bob]);
    let rejections = bob.take_upstream_rejections();
    assert!(rejections.is_empty(), "{rejections:?}");

    let carol = replica(3);
    let _carol_link = link(&carol, &server);
    let all = carol.prepare_query(&Query::from("children")).unwrap();
    let _held = block_on(carol.subscribe(&all, ReadOpts::default())).unwrap();
    let parents = carol.prepare_query(&Query::from("parents")).unwrap();
    let _held_parents = block_on(carol.subscribe(&parents, ReadOpts::default())).unwrap();
    pump(&server, &[&carol]);
    assert_eq!(
        rows(&carol, "children", CHILD),
        vec![child(2, "first life"), child(4, "after reopen")]
    );
    let rank = |r: i64| Some(Value::Nullable(Some(Box::new(Value::I64(r)))));
    assert_eq!(
        rows(&carol, "parents", &["title", "rank"]),
        vec![
            vec![Some(Value::String("p1".into())), rank(10)],
            vec![Some(Value::String("p3".into())), rank(30)],
        ]
    );
}

/// Reopening twice, under v1 then under v2, changes nothing: the second
/// life's writes stand and the newest version reads every row the same
/// way, whichever version the store reopened under.
#[test]
fn reopening_under_either_version_reads_and_writes_alike() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("core.sqlite");
    {
        let server = authority_at(v1(), &path);
        block_on(server.admit_schema_with_lenses(v2(), &[v1_to_v2()])).unwrap();
        let alice = replica(1);
        let _link = link(&alice, &server);
        pump(&server, &[&alice]);
        insert_child(&alice, 1, "one");
        pump(&server, &[&alice]);
        assert!(alice.take_upstream_rejections().is_empty());
    }
    for (life, schema) in [(2u8, v1()), (3, v2()), (4, v1())] {
        let server = authority_at(schema, &path);
        let writer = replica(10 + life);
        let _link = link(&writer, &server);
        pump(&server, &[&writer]);
        insert_child(&writer, life, "again");
        pump(&server, &[&writer]);
        let rejections = writer.take_upstream_rejections();
        assert!(rejections.is_empty(), "life {life}: {rejections:?}");
    }
    let server = authority_at(v2(), &path);
    assert_eq!(
        rows(&server, "children", CHILD),
        vec![
            child(1, "one"),
            child(2, "again"),
            child(3, "again"),
            child(4, "again")
        ]
    );
}

/// The backend's own database, a replica file reopened under v2 with the
/// app's migrations, keeps taking its own writes to the added table after
/// any number of reopens (the shape in #4769's reproduction).
#[test]
fn a_replica_file_reopened_with_the_apps_migrations_keeps_writing_the_added_table() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("backend.sqlite");
    for n in 1..=3u8 {
        let db = block_on(Db::open_at(
            config(v2(), 1).with_migrations(vec![v1()], vec![v1_to_v2()]),
            &path,
            FileOptions::default(),
        ))
        .unwrap();
        db.set_authority();
        insert_child(&db, n, "local");
        assert_eq!(rows(&db, "children", CHILD).len(), n as usize);
    }
}

/// A replica opened without the app's migrations (React Native's relay
/// before alpha.61) minted its own names for v2's columns, which no
/// authority ever confirmed. The authority's catalogue is then refused with
/// the typed `store_opened_without_migrations`, which a host can act on (a
/// rebuild, a reset), not a free-form "not supported yet". A replica opened
/// with the migrations mirrors the same catalogue.
///
/// ```text
/// authority (v1 + lens → v2) ──catalogue──► replica (v2 alone)      ✗ store_opened_without_migrations
///                            ──catalogue──► replica (v2 + migrations) ✓ mirrors
/// ```
#[test]
fn a_replica_opened_without_the_apps_migrations_is_refused_by_name() {
    let authority = block_on(Db::open_history_complete(
        config(v2(), 0).with_migrations(vec![v1()], vec![v1_to_v2()]),
    ))
    .unwrap();
    authority.set_authority();
    insert_parent(&authority, 1, 1);

    let head_only = block_on(Db::open(config(v2(), 7))).unwrap();
    head_only.track_upstream_rejections();
    let refused = link(&head_only, &authority);
    pump(&authority, &[&head_only]);
    let refusal = head_only
        .upstream_refusal(&refused)
        .expect("the authority's catalogue is refused");
    assert_eq!(
        refusal.code,
        jazz::db::ErrorCode::StoreOpenedWithoutMigrations,
        "{refusal}"
    );

    let shipped = replica(8);
    let mirrored = link(&shipped, &authority);
    pump(&authority, &[&shipped]);
    assert!(
        shipped.upstream_refusal(&mirrored).is_none(),
        "a replica with the app's migrations mirrors the catalogue"
    );
}
