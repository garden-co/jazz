//! Table and column identities: a UUID per stored table and stored column,
//! derived from the authority's namespace and the stored names. A rename
//! keeps the stored name, so it keeps the identity; replicas answer with the
//! authority's identities once they mirror its catalogue, and never mint
//! their own. A column dropped and added again keeps its stored column, and
//! so its identity, only when its type is the one stored there.
//!
//! The authority and replica guards follow Joe's authority catalogue tests
//! from #4366, carried onto the one catalogue (`_jazz_names`).

use crate::common;

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, FileOptions, InsertOptions, ReadOpts, RemoteLinkHint,
    SubscriptionEvent, SyncMessage, Transport, TransportError, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{LensOp, MigrationLens, TableLens};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

/// `table(column: text, rank: bigint = 0)`.
fn notes(table: &str, column: &str) -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(table)
                    .column(column, ColumnType::Text)
                    .column_with_default("rank", ColumnType::BigInt, jazz::tools::Value::BigInt(0))
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

/// `notes(text)`: `rank` dropped.
fn without_rank() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

/// `notes(text, rank: bigint = 5, tag: text?)`: `rank` added again with
/// its type, in a version of its own (a default is not part of a version's
/// identity, so the new `tag` makes it one).
fn bigint_rank() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .column_with_default("rank", ColumnType::BigInt, jazz::tools::Value::BigInt(5))
                    .nullable_column("tag", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

/// `notes(text, rank: text?)`: `rank` added again with another type.
fn text_rank() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .nullable_column("rank", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn rename(source: &JazzSchema, target: &JazzSchema, ops: Vec<LensOp>) -> MigrationLens {
    MigrationLens::new(
        source.version_id(),
        target.version_id(),
        vec![TableLens {
            source_table: source.tables()[0].name.clone(),
            target_table: target.tables()[0].name.clone(),
            ops,
        }],
    )
    .unwrap()
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

fn authority(schema: JazzSchema, node: u8) -> Db {
    let db = block_on(Db::open_history_complete(config(
        schema,
        node,
        AuthorSubject::SYSTEM,
    )))
    .unwrap();
    db.set_authority();
    db
}

fn open_at(schema: JazzSchema, node: u8, path: &Path) -> Db {
    block_on(Db::open_at(
        config(schema, node, AuthorSubject::SYSTEM),
        path,
        FileOptions::default(),
    ))
    .unwrap()
}

fn not_observed<T: std::fmt::Debug>(result: Result<T, jazz::db::Error>) {
    assert_eq!(result.unwrap_err().code, ErrorCode::NotObserved);
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

/// `client` connected to `server`, which serves it as SYSTEM.
fn link(client: &Db, server: &Db) -> jazz::db::Upstream {
    let (c, s) = pipe();
    let link = block_on(client.connect_upstream(c));
    server.accept_subscriber(s, AuthorSubject::SYSTEM);
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

fn insert(db: &Db, n: u8, cells: &[(&str, Value)]) {
    block_on(
        db.insert(
            "notes",
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
    .unwrap();
}

fn ranks(db: &Db) -> Vec<Option<Value>> {
    let prepared = db.prepare_query(&Query::from("notes")).unwrap();
    let mut rows: Vec<_> = block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|row| {
            let rank = row
                .cells()
                .find(|(name, _)| *name == "rank")
                .and_then(|(_, value)| value.cloned());
            (row.row_uuid(), rank)
        })
        .collect();
    rows.sort_by_key(|(id, _)| *id);
    rows.into_iter().map(|(_, rank)| rank).collect()
}

/// Two apps on the same schema never share an identity; a lookup of a
/// table or column the schema lacks is `None`; and a store that is no
/// authority's, nor mirrors one, has none to give.
#[test]
fn identities_are_the_authoritys_and_differ_between_apps() {
    let alice = authority(notes("notes", "text"), 1);
    let bob = authority(notes("notes", "text"), 2);
    let table = alice.table_identity("notes").unwrap().expect("notes");
    assert_ne!(Some(table), bob.table_identity("notes").unwrap());
    let column = alice
        .column_identity("notes", "text")
        .unwrap()
        .expect("text");
    assert_ne!(table.0, column.0);
    assert_ne!(
        Some(column),
        alice.column_identity("notes", "rank").unwrap()
    );
    assert_eq!(alice.table_identity("unknown").unwrap(), None);
    assert_eq!(alice.column_identity("notes", "unknown").unwrap(), None);
    assert_eq!(alice.table_identity("notes").unwrap(), Some(table));

    let local = block_on(Db::open(config(
        notes("notes", "text"),
        3,
        AuthorSubject::SYSTEM,
    )))
    .unwrap();
    not_observed(local.table_identity("notes"));
    not_observed(local.column_identity("notes", "text"));
}

/// An authority's identities survive its restart: the namespace is minted
/// once and kept in the file.
#[test]
fn a_reopened_authority_keeps_its_identities() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("authority.sqlite");
    let db = open_at(notes("notes", "text"), 1, &path);
    db.set_authority();
    let table = db.table_identity("notes").unwrap();
    let column = db.column_identity("notes", "text").unwrap();
    drop(db);
    let db = open_at(notes("notes", "text"), 1, &path);
    db.set_authority();
    assert_eq!(db.table_identity("notes").unwrap(), table);
    assert_eq!(db.column_identity("notes", "text").unwrap(), column);
}

/// A rename keeps the stored table and column, so it keeps their
/// identities: the current schema reads them by the new names, and a view
/// of the old version by the old ones.
#[test]
fn a_rename_keeps_the_identities() {
    let v1 = notes("notes", "text");
    let v2 = notes("tasks", "body");
    let db = authority(v1.clone(), 1);
    let table = db.table_identity("notes").unwrap();
    let column = db.column_identity("notes", "text").unwrap();
    let lens = rename(
        &v1,
        &v2,
        vec![LensOp::RenameColumn {
            from: "text".into(),
            to: "body".into(),
        }],
    );
    block_on(db.activate_schema_with_lenses(v2, &[lens])).unwrap();
    assert_eq!(db.table_identity("tasks").unwrap(), table);
    assert_eq!(db.column_identity("tasks", "body").unwrap(), column);
    assert_eq!(db.table_identity("notes").unwrap(), None);
    let old = block_on(db.register_schema_view(v1)).unwrap();
    assert_eq!(old.table_identity("notes").unwrap(), table);
    assert_eq!(old.column_identity("notes", "text").unwrap(), column);
    assert_eq!(old.table_identity("tasks").unwrap(), None);
}

/// A replica has no identities until it mirrors the authority's catalogue,
/// then the authority's, also offline after a restart.
#[test]
fn a_replica_answers_with_the_authoritys_identities_once_it_mirrors_them() {
    let server = authority(notes("notes", "text"), 0);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("client.sqlite");
    {
        let client = open_at(notes("notes", "text"), 1, &path);
        not_observed(client.table_identity("notes"));
        let _link = link(&client, &server);
        pump(&server, &[&client]);
        assert_eq!(
            client.table_identity("notes").unwrap(),
            server.table_identity("notes").unwrap()
        );
        assert_eq!(
            client.column_identity("notes", "rank").unwrap(),
            server.column_identity("notes", "rank").unwrap()
        );
    }
    let offline = open_at(notes("notes", "text"), 1, &path);
    assert_eq!(
        offline.table_identity("notes").unwrap(),
        server.table_identity("notes").unwrap()
    );
}

/// A version only the replica admitted has no identity yet: its names
/// are the replica's guess until the authority admits it too.
#[test]
fn a_version_only_the_replica_admitted_has_no_identity_until_the_authority_admits_it() {
    let v1 = notes("notes", "text");
    let v2 = notes("tasks", "body");
    let lens = rename(
        &v1,
        &v2,
        vec![LensOp::RenameColumn {
            from: "text".into(),
            to: "body".into(),
        }],
    );
    let server = authority(v1.clone(), 0);
    let client = block_on(Db::open(config(v1.clone(), 1, AuthorSubject::SYSTEM))).unwrap();
    let _link = link(&client, &server);
    pump(&server, &[&client]);
    let table = server.table_identity("notes").unwrap();
    assert_eq!(client.table_identity("notes").unwrap(), table);

    block_on(client.admit_schema_with_lenses(v2.clone(), &[lens.clone()])).unwrap();
    let ahead = block_on(client.register_schema_view(v2.clone())).unwrap();
    not_observed(ahead.table_identity("tasks"));

    block_on(server.activate_schema_with_lenses(v2, &[lens])).unwrap();
    pump(&server, &[&client]);
    assert_eq!(ahead.table_identity("tasks").unwrap(), table);
}

/// A store marked authority while it has an upstream is that upstream's
/// replica: it mints no namespace of its own.
#[test]
fn an_attached_store_marked_authority_mints_nothing() {
    let client = block_on(Db::open(config(
        notes("notes", "text"),
        1,
        AuthorSubject::SYSTEM,
    )))
    .unwrap();
    let (c, _s) = pipe();
    let _link = block_on(client.connect_upstream(c));
    client.set_authority();
    not_observed(client.table_identity("notes"));
}

/// A replica of one app connected to another app's authority refuses it:
/// it keeps its identities and its remote reads say why.
#[test]
fn an_upstream_of_another_app_is_refused() {
    let alice = authority(notes("notes", "text"), 0);
    let bob = authority(notes("notes", "text"), 9);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("client.sqlite");
    {
        let client = open_at(notes("notes", "text"), 1, &path);
        let _link = link(&client, &alice);
        pump(&alice, &[&client]);
    }
    let client = open_at(notes("notes", "text"), 1, &path);
    let _link = link(&client, &bob);
    let q = client.prepare_query(&Query::from("notes")).unwrap();
    let mut stream = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&bob, &[&client]);
    let mut rejected = None;
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Rejected { reason } = event {
            rejected = Some(reason);
        }
    }
    let reason = rejected.expect("the remote read says why");
    assert!(reason.contains("another app"), "{reason}");
    assert_eq!(
        client.table_identity("notes").unwrap(),
        alice.table_identity("notes").unwrap()
    );
}

/// A tab behind a worker learns the identities through the worker.
#[test]
fn a_tab_behind_a_worker_answers_with_the_authoritys_identities() {
    let server = authority(notes("notes", "text"), 0);
    let worker = block_on(Db::open(config(
        notes("notes", "text"),
        0xa0,
        AuthorSubject::SYSTEM,
    )))
    .unwrap();
    let tab = block_on(Db::open(config(
        notes("notes", "text"),
        0xa1,
        AuthorSubject::SYSTEM,
    )))
    .unwrap();
    tab.set_non_durable_client();
    tab.set_remote_link_hint(RemoteLinkHint::Live);
    let _tab_link = link(&tab, &worker);
    pump(&worker, &[&tab]);
    not_observed(tab.table_identity("notes"));

    let _worker_link = link(&worker, &server);
    worker.set_remote_link_hint(RemoteLinkHint::Live);
    for _ in 0..6 {
        block_on(tab.tick()).unwrap();
        block_on(worker.tick()).unwrap();
        block_on(server.tick()).unwrap();
        block_on(worker.tick()).unwrap();
        block_on(tab.tick()).unwrap();
    }
    assert_eq!(
        tab.table_identity("notes").unwrap(),
        server.table_identity("notes").unwrap()
    );
}

/// `rank` dropped and added again with its type continues its stored
/// column: the identity and the values written before the drop are back.
#[test]
fn a_column_added_again_with_its_type_keeps_its_identity_and_values() {
    let db = authority(notes("notes", "text"), 1);
    insert(
        &db,
        1,
        &[("text", Value::String("a".into())), ("rank", Value::I64(7))],
    );
    let rank = db.column_identity("notes", "rank").unwrap();
    block_on(db.activate_schema(without_rank())).unwrap();
    block_on(db.activate_schema(bigint_rank())).unwrap();
    assert_eq!(db.column_identity("notes", "rank").unwrap(), rank);
    assert_eq!(ranks(&db), [Some(Value::I64(7))]);
}

/// `rank` added again as text is a new column: a new identity and none of
/// the integers stored before, which stay readable by the old version.
#[test]
fn a_column_added_again_with_another_type_is_a_new_column() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let db = open_at(notes("notes", "text"), 1, &path);
    db.set_authority();
    insert(
        &db,
        1,
        &[("text", Value::String("a".into())), ("rank", Value::I64(7))],
    );
    let rank = db.column_identity("notes", "rank").unwrap();
    block_on(db.activate_schema(without_rank())).unwrap();
    drop(db);
    // Reopened, so only the file knows `rank` held integers.
    let db = open_at(without_rank(), 1, &path);
    db.set_authority();
    block_on(db.activate_schema(text_rank())).unwrap();
    let text = db.column_identity("notes", "rank").unwrap();
    assert!(text.is_some());
    assert_ne!(text, rank);
    assert_eq!(ranks(&db), [Some(Value::Nullable(None))]);
    let old = block_on(db.register_schema_view(notes("notes", "text"))).unwrap();
    assert_eq!(old.column_identity("notes", "rank").unwrap(), rank);
    assert_eq!(ranks(&old), [Some(Value::I64(7))]);
}

/// A column whose type changes in place is a new column, also when only
/// the file knows the type it was stored with: a store opened on such a
/// schema (an app shipped ahead of its server's deploy) gives it a stored
/// column of its own, and the old version still reads the values before.
#[test]
fn a_type_changed_in_place_is_a_new_column() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let db = open_at(notes("notes", "text"), 1, &path);
    db.set_authority();
    insert(
        &db,
        1,
        &[("text", Value::String("a".into())), ("rank", Value::I64(7))],
    );
    let rank = db.column_identity("notes", "rank").unwrap();
    drop(db);
    let db = open_at(text_rank(), 1, &path);
    db.set_authority();
    let text = db.column_identity("notes", "rank").unwrap();
    assert!(text.is_some());
    assert_ne!(text, rank);
    assert_eq!(ranks(&db), [Some(Value::Nullable(None))]);
    let old = block_on(db.register_schema_view(notes("notes", "text"))).unwrap();
    assert_eq!(old.column_identity("notes", "rank").unwrap(), rank);
    assert_eq!(ranks(&old), [Some(Value::I64(7))]);
}

/// `notes(text, rank: boolean?)`: `rank` added again with a third type.
fn bool_rank() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .nullable_column("rank", ColumnType::Boolean)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

/// The rejection reason of a remote read of `notes` on `client`, after
/// pumping it with `server`.
fn remote_read_rejection(client: &Db, server: &Db) -> Option<String> {
    let q = client.prepare_query(&Query::from("notes")).unwrap();
    let mut stream = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(server, &[client]);
    let mut rejected = None;
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Rejected { reason } = event {
            rejected = Some(reason);
        }
    }
    rejected
}

/// A replica and its authority each add `rank` again while apart, as text
/// and as a boolean: both store it under the same fresh name. The replica
/// refuses the authority's catalogue rather than read text as booleans: it
/// stays as it was, its remote reads say why, and its local reads go on.
#[test]
fn a_replica_refuses_a_catalogue_storing_its_column_with_another_type() {
    let server = authority(notes("notes", "text"), 0);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("client.sqlite");
    {
        let client = open_at(notes("notes", "text"), 1, &path);
        let _link = link(&client, &server);
        block_on(server.activate_schema(without_rank())).unwrap();
        pump(&server, &[&client]);
    }
    let client = open_at(without_rank(), 1, &path);
    block_on(client.activate_schema(text_rank())).unwrap();
    insert(
        &client,
        1,
        &[
            ("text", Value::String("a".into())),
            (
                "rank",
                Value::Nullable(Some(Box::new(Value::String("high".into())))),
            ),
        ],
    );
    block_on(server.activate_schema(bool_rank())).unwrap();

    let _link = link(&client, &server);
    let reason = remote_read_rejection(&client, &server).expect("the remote read says why");
    assert!(reason.contains("changing the type"), "{reason}");
    assert_eq!(
        ranks(&client),
        [Some(Value::Nullable(Some(Box::new(Value::String(
            "high".into()
        )))))],
        "local reads go on"
    );
    drop(client);
    let reopened = open_at(text_rank(), 1, &path);
    assert_eq!(
        ranks(&reopened),
        [Some(Value::Nullable(Some(Box::new(Value::String(
            "high".into()
        )))))],
        "nothing of the authority's catalogue was stored"
    );
}

/// `table(text)`, with `rank` of `ty` defaulting to `default` when given.
fn ranked(table: &str, rank: Option<(ColumnType, jazz::tools::Value)>) -> JazzSchema {
    let mut builder = TableSchemaBuilder::new(table).column("text", ColumnType::Text);
    if let Some((ty, default)) = rank {
        builder = builder.column_with_default("rank", ty, default);
    }
    compile_schema(
        &SchemaBuilder::new()
            .table(builder.policies(allow_all_policies()))
            .build(),
    )
}

/// `rank` added again through a lens from the version that dropped it is
/// a new column, with the lens's default: the lens says the source lacks
/// it, so the integers stored before are not its values.
#[test]
fn a_column_a_lens_adds_again_is_a_new_column() {
    let db = authority(notes("notes", "text"), 1);
    insert(
        &db,
        1,
        &[("text", Value::String("a".into())), ("rank", Value::I64(7))],
    );
    let rank = db.column_identity("notes", "rank").unwrap();
    block_on(db.activate_schema(without_rank())).unwrap();
    let lens = rename(
        &without_rank(),
        &bigint_rank(),
        vec![LensOp::AddColumn {
            column: "rank".into(),
            default: Value::I64(5),
        }],
    );
    block_on(db.activate_schema_with_lenses(bigint_rank(), &[lens])).unwrap();
    let added = db.column_identity("notes", "rank").unwrap();
    assert!(added.is_some());
    assert_ne!(added, rank);
    assert_eq!(ranks(&db), [Some(Value::I64(5))]);
}

/// Dropped, the table renamed, then added again with its type: `rank`
/// continues the stored column it had under the table's old name.
#[test]
fn a_column_added_again_after_its_table_is_renamed_keeps_its_identity_and_values() {
    let db = authority(notes("notes", "text"), 1);
    insert(
        &db,
        1,
        &[("text", Value::String("a".into())), ("rank", Value::I64(7))],
    );
    let rank = db.column_identity("notes", "rank").unwrap();
    block_on(db.activate_schema(without_rank())).unwrap();
    let tasks = ranked("tasks", None);
    block_on(
        db.activate_schema_with_lenses(tasks.clone(), &[rename(&without_rank(), &tasks, vec![])]),
    )
    .unwrap();
    block_on(db.activate_schema(ranked(
        "tasks",
        Some((ColumnType::BigInt, jazz::tools::Value::BigInt(5))),
    )))
    .unwrap();
    assert_eq!(db.column_identity("tasks", "rank").unwrap(), rank);
    let prepared = db.prepare_query(&Query::from("tasks")).unwrap();
    let rows = block_on(db.all(&prepared, ReadOpts::default())).unwrap();
    let ranks: Vec<_> = rows
        .iter()
        .map(|row| {
            row.cells()
                .find(|(name, _)| *name == "rank")
                .and_then(|(_, value)| value.cloned())
        })
        .collect();
    assert_eq!(ranks, [Some(Value::I64(7))]);
}

/// A replica mirrors a column its authority dropped and added again: the
/// same stored column, so the same identity and the values from before.
#[test]
fn a_replica_mirrors_a_column_added_again() {
    let server = authority(notes("notes", "text"), 0);
    insert(
        &server,
        1,
        &[("text", Value::String("a".into())), ("rank", Value::I64(7))],
    );
    let client = block_on(Db::open(config(
        notes("notes", "text"),
        1,
        AuthorSubject::SYSTEM,
    )))
    .unwrap();
    let _link = link(&client, &server);
    let all = client.prepare_query(&Query::from("notes")).unwrap();
    let _held = block_on(client.subscribe(&all, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    let rank = server.column_identity("notes", "rank").unwrap();
    block_on(server.activate_schema(without_rank())).unwrap();
    block_on(server.activate_schema(bigint_rank())).unwrap();
    pump(&server, &[&client]);
    let view = block_on(client.register_schema_view(bigint_rank())).unwrap();
    assert_eq!(view.column_identity("notes", "rank").unwrap(), rank);
    assert_eq!(ranks(&view), [Some(Value::I64(7))]);
}

/// A store written before column types were recorded knows none: a column
/// added there again starts a new stored column rather than guess the
/// type of the one it had, and the types it stores from then on are
/// recorded.
#[test]
fn a_store_without_recorded_types_adds_a_column_again_as_a_new_one() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let db = open_at(notes("notes", "text"), 1, &path);
    db.set_authority();
    insert(
        &db,
        1,
        &[("text", Value::String("a".into())), ("rank", Value::I64(7))],
    );
    let rank = db.column_identity("notes", "rank").unwrap();
    block_on(db.activate_schema(without_rank())).unwrap();
    drop(db);
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("DROP TABLE _jazz_column_types")
        .unwrap();
    let db = open_at(without_rank(), 1, &path);
    db.set_authority();
    block_on(db.activate_schema(bigint_rank())).unwrap();
    let added = db.column_identity("notes", "rank").unwrap();
    assert!(added.is_some());
    assert_ne!(added, rank);
    assert_eq!(ranks(&db), [Some(Value::I64(5))]);
    drop(db);
    let recorded: i64 = rusqlite::Connection::open(&path)
        .unwrap()
        .query_row(
            "SELECT count(*) FROM _jazz_column_types WHERE tbl = 'notes'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(recorded > 0, "its types are recorded");
}
