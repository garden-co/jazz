//! Writes made before the server can take them are never silently lost:
//! a store that writes before it first connects, and a client writing
//! under a schema version its server has not admitted yet, either upload
//! those writes once they can, or are refused with a typed error.

use crate::common;

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use jazz::db::{
    Db, DbConfig, DbIdentity, Error, FileOptions, InsertOptions, ReadOpts, ReadTier, Reason,
    RemoteLinkHint, SubscriptionEvent, SyncMessage, Transport, TransportError, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{MigrationLens, TableLens};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

use common::{allow_all_policies, compile_schema};

/// `notes(title: text)`.
fn v1() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

/// v1 plus `tag: text?`.
fn v2() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .nullable_column("tag", ColumnType::Text)
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
            source_table: "notes".into(),
            target_table: "notes".into(),
            ops: vec![],
        }],
    )
    .unwrap()
}

fn open_at(schema: JazzSchema, node: u8, path: &Path) -> Db {
    block_on(Db::open_at(
        DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author: AuthorSubject::SYSTEM,
            },
        ),
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

/// Insert row `n`; the transaction it committed.
fn insert(db: &Db, n: u8, cells: &[(&str, &str)]) -> u64 {
    block_on(
        db.insert(
            "notes",
            cells
                .iter()
                .map(|(column, value)| ((*column).to_owned(), Value::String((*value).to_owned())))
                .collect::<BTreeMap<_, _>>(),
            InsertOptions {
                row_id: Some(RowUuid::from_bytes([n; 16])),
                ..Default::default()
            },
        ),
    )
    .unwrap()
    .mergeable_tx_id()
}

/// The row bytes of every note `db` reads, sorted.
fn ids(db: &Db) -> Vec<u8> {
    let prepared = db.prepare_query(&Query::from("notes")).unwrap();
    let mut ids: Vec<u8> = block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|row| row.row_uuid().as_bytes()[0])
        .collect();
    ids.sort();
    ids
}

/// Row `n`'s `tag` as `db` reads it.
fn tag(db: &Db, n: u8) -> Option<Value> {
    let prepared = db.prepare_query(&Query::from("notes")).unwrap();
    block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .iter()
        .find(|row| row.row_uuid().as_bytes()[0] == n)
        .and_then(|row| {
            row.cells()
                .find(|(name, _)| *name == "tag")
                .and_then(|(_, value)| value.cloned())
        })
}

fn some(value: &str) -> Option<Value> {
    Some(Value::Nullable(Some(Box::new(Value::String(
        value.to_owned(),
    )))))
}

/// The reason the first `Rejected` event `stream` holds gives, if any.
fn rejection(stream: &mut jazz::db::SubscriptionStream) -> Option<String> {
    let mut rejected = None;
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Rejected { reason } = event {
            rejected.get_or_insert(reason);
        }
    }
    rejected
}

/// #4443: a store that committed writes before it became a replica keeps
/// them and refuses its upstream, saying why on its remote reads: the
/// upstream's rows would otherwise overwrite them, and they never upload.
/// It stays so across a reopen.
#[test]
fn a_store_that_wrote_before_becoming_a_replica_refuses_its_upstream() {
    for server_rows in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let server = authority_at(v1(), &dir.path().join("server.sqlite"));
        if server_rows {
            insert(&server, 1, &[("title", "on the server")]);
        }
        let client_path = dir.path().join("client.sqlite");
        let client = open_at(v1(), 1, &client_path);
        insert(&client, 2, &[("title", "written standalone")]);
        let on_server = if server_rows { vec![1] } else { vec![] };

        for _ in 0..2 {
            let _link = link(&client, &server);
            let q = client.prepare_query(&Query::from("notes")).unwrap();
            let mut stream = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
            pump(&server, &[&client]);
            let reason = rejection(&mut stream).expect("the remote read says why");
            assert!(reason.contains("before it became a replica"), "{reason}");
            assert_eq!(ids(&client), vec![2]);
            assert_eq!(ids(&server), on_server);
        }
        drop(client);
        let client = open_at(v1(), 1, &client_path);
        let _link = link(&client, &server);
        let q = client.prepare_query(&Query::from("notes")).unwrap();
        let mut stream = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
        pump(&server, &[&client]);
        assert!(rejection(&mut stream).is_some());
        assert_eq!(ids(&client), vec![2]);
        assert_eq!(ids(&server), on_server);
    }
}

/// A store that becomes a replica before it writes uploads what it wrote
/// offline on its first connect, beside the rows the server holds.
#[test]
fn a_store_that_became_a_replica_before_writing_uploads_on_first_connect() {
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(v1(), &dir.path().join("server.sqlite"));
    insert(&server, 1, &[("title", "on the server")]);
    let client = open_at(v1(), 1, &dir.path().join("client.sqlite"));
    client.become_replica();
    insert(&client, 2, &[("title", "written offline")]);

    let _link = link(&client, &server);
    let q = client.prepare_query(&Query::from("notes")).unwrap();
    let mut stream = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);

    assert_eq!(rejection(&mut stream), None);
    assert_eq!(ids(&server), vec![1, 2]);
    assert_eq!(ids(&client), vec![1, 2]);
}

/// A store that is a replica from the start and has written nothing yet
/// connects as any replica does.
#[test]
fn an_empty_store_connects_whenever_it_becomes_a_replica() {
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(v1(), &dir.path().join("server.sqlite"));
    insert(&server, 1, &[("title", "on the server")]);
    let client = open_at(v1(), 1, &dir.path().join("client.sqlite"));
    let _link = link(&client, &server);
    let q = client.prepare_query(&Query::from("notes")).unwrap();
    let mut stream = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    insert(&client, 2, &[("title", "online")]);
    pump(&server, &[&client]);

    assert_eq!(rejection(&mut stream), None);
    assert_eq!(ids(&server), vec![1, 2]);
    assert_eq!(ids(&client), vec![1, 2]);
}

/// #4450: an offline client on v1 admits v2 ahead of its server and
/// writes through a v2 view. The write stays pending, its v2 column
/// included; once the server has v2 when the client syncs, it uploads.
#[test]
fn an_offline_view_write_uploads_once_the_server_has_its_version() {
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(v1(), &dir.path().join("server.sqlite"));
    insert(&server, 1, &[("title", "on the server")]);
    let client = open_at(v1(), 1, &dir.path().join("client.sqlite"));
    client.become_replica();
    block_on(client.admit_schema_with_lenses(v2(), &[lens()])).unwrap();
    let ahead = block_on(client.register_schema_view(v2())).unwrap();
    insert(&ahead, 2, &[("title", "ahead"), ("tag", "new")]);
    assert_eq!(tag(&ahead, 2), some("new"));

    block_on(server.activate_schema_with_lenses(v2(), &[lens()])).unwrap();
    let _link = link(&client, &server);
    let q = client.prepare_query(&Query::from("notes")).unwrap();
    let _sub = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);

    assert_eq!(ids(&server), vec![1, 2]);
    assert_eq!(tag(&server, 2), some("new"));
    assert_eq!(ids(&client), vec![1, 2]);
    assert_eq!(tag(&ahead, 2), some("new"));
}

/// #4450: a write through a v2 view syncs to a server that still lacks
/// v2. The server refuses it loudly, with a typed reason (the app was
/// released ahead of its schema); the client drops it, and the session
/// goes on: reads are answered and writes after it upload.
#[test]
fn a_view_write_the_server_lacks_at_sync_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(v1(), &dir.path().join("server.sqlite"));
    insert(&server, 1, &[("title", "on the server")]);
    let client = open_at(v1(), 1, &dir.path().join("client.sqlite"));
    client.become_replica();
    block_on(client.admit_schema_with_lenses(v2(), &[lens()])).unwrap();
    let ahead = block_on(client.register_schema_view(v2())).unwrap();
    let refused = insert(&ahead, 2, &[("title", "ahead"), ("tag", "new")]);
    let after = insert(&client, 3, &[("title", "a plain v1 write after it")]);

    let _link = link(&client, &server);
    let q = client.prepare_query(&Query::from("notes")).unwrap();
    let _sub = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    let error = fate(&server, &client, refused).expect_err("refused by the server");
    assert_eq!(
        error.reason,
        Some(Reason::SchemaVersionNotDeployed),
        "{error:?}"
    );
    fate(&server, &client, after).expect("uploads");
    insert(&server, 4, &[("title", "meanwhile")]);
    pump(&server, &[&client]);

    assert_eq!(ids(&server), vec![1, 3, 4]);
    assert_eq!(ids(&client), vec![1, 3, 4]);
    assert_eq!(remote_read(&server, &client), Some(Ok(vec![1, 3, 4])));
}

/// #4450: a client opened on v2 while its server knows only v1: a write
/// it syncs then is refused with a typed reason, never kept in silence.
#[test]
fn a_write_from_a_client_opened_ahead_of_its_server_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(v1(), &dir.path().join("server.sqlite"));
    insert(&server, 1, &[("title", "on the server")]);
    let client = open_at(v2(), 1, &dir.path().join("client.sqlite"));
    let _link = link(&client, &server);
    let q = client.prepare_query(&Query::from("notes")).unwrap();
    let _sub = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    let tx = insert(&client, 2, &[("title", "ahead"), ("tag", "new")]);
    let error = fate(&server, &client, tx).expect_err("refused by the server");
    assert_eq!(
        error.reason,
        Some(Reason::SchemaVersionNotDeployed),
        "{error:?}"
    );
    assert_eq!(ids(&server), vec![1]);
    assert!(!ids(&client).contains(&2));
}

/// A client opened on v2 that writes offline uploads on its first sync
/// once its server has v2.
#[test]
fn a_client_opened_ahead_uploads_once_its_server_has_the_version() {
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(v1(), &dir.path().join("server.sqlite"));
    insert(&server, 1, &[("title", "on the server")]);
    // The app ships its versions and lenses, so the client names v2's
    // columns as its server will (SPEC 10.7).
    let client = block_on(Db::open_at(
        DbConfig::new(
            v2(),
            DbIdentity {
                node: NodeUuid::from_bytes([1; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
        .with_migrations(vec![v1(), v2()], vec![lens()]),
        &dir.path().join("client.sqlite"),
        FileOptions::default(),
    ))
    .unwrap();
    client.become_replica();
    insert(&client, 2, &[("title", "ahead"), ("tag", "new")]);
    block_on(server.activate_schema_with_lenses(v2(), &[lens()])).unwrap();
    let _link = link(&client, &server);
    let q = client.prepare_query(&Query::from("notes")).unwrap();
    let _sub = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);

    assert_eq!(ids(&server), vec![1, 2]);
    assert_eq!(tag(&server, 2), some("new"));
    assert_eq!(ids(&client), vec![1, 2]);
}

/// A commit names tables and columns by stored names, so one transaction
/// may write through views of different versions: a replica uploads it as
/// one commit, which lands whole once the server deploys the versions.
#[test]
fn a_replica_uploads_a_transaction_through_views_of_different_versions() {
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(v1(), &dir.path().join("server.sqlite"));
    let client = open_at(v1(), 1, &dir.path().join("client.sqlite"));
    let _link = link(&client, &server);
    pump(&server, &[&client]);
    block_on(client.admit_schema_with_lenses(v2(), &[lens()])).unwrap();
    let ahead = block_on(client.register_schema_view(v2())).unwrap();
    let cells = |cells: &[(&str, &str)]| {
        cells
            .iter()
            .map(|(column, value)| ((*column).to_owned(), Value::String((*value).to_owned())))
            .collect::<BTreeMap<_, _>>()
    };
    let row = |n: u8| InsertOptions {
        row_id: Some(RowUuid::from_bytes([n; 16])),
        ..Default::default()
    };

    let mixed = OpenTransactionId::new();
    block_on(client.begin_mergeable(mixed)).unwrap();
    block_on(
        client
            .tx_ref(mixed)
            .insert("notes", cells(&[("title", "v1")]), row(4)),
    )
    .unwrap();
    block_on(ahead.tx_ref(mixed).insert(
        "notes",
        cells(&[("title", "v2"), ("tag", "new")]),
        row(5),
    ))
    .unwrap();
    let tx = block_on(client.commit_mergeable_handle(mixed)).unwrap().0;
    assert_eq!(ids(&client), vec![4, 5]);
    block_on(server.activate_schema_with_lenses(v2(), &[lens()])).unwrap();
    fate(&server, &client, tx).expect("the mixed transaction lands whole");
    assert_eq!(ids(&server), vec![4, 5]);
    assert_eq!(tag(&server, 5), some("new"));
}

fn cells(cells: &[(&str, &str)]) -> BTreeMap<String, Value> {
    cells
        .iter()
        .map(|(column, value)| ((*column).to_owned(), Value::String((*value).to_owned())))
        .collect()
}

fn row(n: u8) -> InsertOptions {
    InsertOptions {
        row_id: Some(RowUuid::from_bytes([n; 16])),
        ..Default::default()
    }
}

/// A `Remote` one-shot read of `notes` on `client`, pumping between
/// polls: the row bytes it answers, or why it failed; `None` while it
/// still waits.
fn remote_read(server: &Db, client: &Db) -> Option<Result<Vec<u8>, String>> {
    let prepared = client.prepare_query(&Query::from("notes")).unwrap();
    let opts = ReadOpts {
        tier: ReadTier::Remote,
        ..Default::default()
    };
    let mut read = pin!(client.all(&prepared, opts));
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    for _ in 0..20 {
        if let Poll::Ready(result) = read.as_mut().poll(&mut cx) {
            return Some(
                result
                    .map(|rows| rows.iter().map(|r| r.row_uuid().as_bytes()[0]).collect())
                    .map_err(|e| e.message),
            );
        }
        pump(server, &[client]);
    }
    None
}

/// Transaction `tx`'s fate at the server, pumping `client` and `server`
/// until it is known.
fn fate(server: &Db, client: &Db, tx: u64) -> Result<(), Error> {
    fate_by(client, tx, || pump(server, &[client]))
}

/// Transaction `tx`'s fate at the authority, calling `pump` until it is
/// known.
fn fate_by(client: &Db, tx: u64, mut pump: impl FnMut()) -> Result<(), Error> {
    let mut wait = pin!(client.wait_for_transaction(tx, DurabilityTier::Global));
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    for _ in 0..20 {
        if let Poll::Ready(result) = wait.as_mut().poll(&mut cx) {
            return result.map(drop);
        }
        pump();
    }
    panic!("transaction {tx} has no fate yet");
}

/// A `transaction` opened on the owner whose writes go through a view
/// uploads them by stored names, as does one mixing versions.
#[test]
fn a_transaction_uploads_under_the_version_its_writes_name() {
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(v1(), &dir.path().join("server.sqlite"));
    let client = open_at(v1(), 1, &dir.path().join("client.sqlite"));
    let _link = link(&client, &server);
    pump(&server, &[&client]);
    block_on(client.admit_schema_with_lenses(v2(), &[lens()])).unwrap();
    let ahead = block_on(client.register_schema_view(v2())).unwrap();

    let mixed = block_on(client.transaction(async |tx| {
        tx.insert("notes", cells(&[("title", "v1")]), row(4))
            .await?;
        ahead
            .insert("notes", cells(&[("title", "v2"), ("tag", "new")]), row(5))
            .await?;
        Ok(())
    }));
    mixed.unwrap();
    assert_eq!(ids(&client), vec![4, 5]);

    block_on(client.transaction(async |_| {
        ahead
            .insert("notes", cells(&[("title", "v2"), ("tag", "new")]), row(6))
            .await?;
        Ok(())
    }))
    .unwrap();
    block_on(server.activate_schema_with_lenses(v2(), &[lens()])).unwrap();
    pump(&server, &[&client]);
    assert_eq!(ids(&client), vec![4, 5, 6]);
    assert_eq!(ids(&server), vec![4, 5, 6]);
    assert_eq!(tag(&server, 5), some("new"));
    assert_eq!(tag(&server, 6), some("new"));
}

/// A stranded store answers no `Remote` read: its rows were never the
/// server's to confirm.
#[test]
fn a_stranded_store_refuses_remote_reads() {
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(v1(), &dir.path().join("server.sqlite"));
    insert(&server, 1, &[("title", "on the server")]);
    let client = open_at(v1(), 1, &dir.path().join("client.sqlite"));
    insert(&client, 2, &[("title", "standalone")]);
    let _link = link(&client, &server);
    pump(&server, &[&client]);
    let read = remote_read(&server, &client);
    assert!(
        matches!(&read, Some(Err(m)) if m.contains("before it became a replica")),
        "{read:?}"
    );
}

/// Schema work before a store becomes a replica writes no rows: the
/// store is not stranded.
#[test]
fn schema_work_before_becoming_a_replica_does_not_strand_a_store() {
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(v1(), &dir.path().join("server.sqlite"));
    insert(&server, 1, &[("title", "on the server")]);
    let client = open_at(v1(), 1, &dir.path().join("client.sqlite"));
    block_on(client.admit_schema_with_lenses(v2(), &[lens()])).unwrap();
    block_on(client.activate_schema_with_lenses(v2(), &[lens()])).unwrap();
    block_on(server.activate_schema_with_lenses(v2(), &[lens()])).unwrap();
    let _link = link(&client, &server);
    let q = client.prepare_query(&Query::from("notes")).unwrap();
    let mut stream = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    assert_eq!(rejection(&mut stream), None);
    assert_eq!(remote_read(&server, &client), Some(Ok(vec![1])));
}

/// An offline write through a v2 view survives a reopen under v1
/// (without the view), and lands once the server has v2, `tag` included.
#[test]
fn a_pending_view_write_survives_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(v1(), &dir.path().join("server.sqlite"));
    insert(&server, 1, &[("title", "on the server")]);
    let path = dir.path().join("client.sqlite");
    {
        let client = open_at(v1(), 1, &path);
        client.become_replica();
        block_on(client.admit_schema_with_lenses(v2(), &[lens()])).unwrap();
        let ahead = block_on(client.register_schema_view(v2())).unwrap();
        insert(&ahead, 2, &[("title", "ahead"), ("tag", "new")]);
    }
    let client = open_at(v1(), 1, &path);
    block_on(server.activate_schema_with_lenses(v2(), &[lens()])).unwrap();
    let _link = link(&client, &server);
    let q = client.prepare_query(&Query::from("notes")).unwrap();
    let _sub = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    assert_eq!(ids(&server), vec![1, 2]);
    assert_eq!(tag(&server, 2), some("new"));
    assert_eq!(ids(&client), vec![1, 2]);
}

/// Offline commits under v1, v2, v1, v2 upload in order on the first
/// connect to a server that admitted v2.
#[test]
fn interleaved_versions_upload_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(v1(), &dir.path().join("server.sqlite"));
    let client = open_at(v1(), 1, &dir.path().join("client.sqlite"));
    client.become_replica();
    block_on(client.admit_schema_with_lenses(v2(), &[lens()])).unwrap();
    let ahead = block_on(client.register_schema_view(v2())).unwrap();
    insert(&client, 1, &[("title", "a")]);
    insert(&ahead, 2, &[("title", "b"), ("tag", "t2")]);
    insert(&client, 3, &[("title", "c")]);
    insert(&ahead, 4, &[("title", "d"), ("tag", "t4")]);
    block_on(server.activate_schema_with_lenses(v2(), &[lens()])).unwrap();
    let _link = link(&client, &server);
    let q = client.prepare_query(&Query::from("notes")).unwrap();
    let _sub = block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
    pump(&server, &[&client]);
    assert_eq!(ids(&server), vec![1, 2, 3, 4]);
    assert_eq!(tag(&server, 2), some("t2"));
    assert_eq!(tag(&server, 4), some("t4"));
}

/// A write a stranded store makes after it became a replica never
/// uploads: a durable wait on it says why rather than hanging.
#[test]
fn a_stranded_store_fails_durable_waits() {
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(v1(), &dir.path().join("server.sqlite"));
    let client = open_at(v1(), 1, &dir.path().join("client.sqlite"));
    insert(&client, 2, &[("title", "standalone")]);
    let _link = link(&client, &server);
    pump(&server, &[&client]);
    let tx = insert(&client, 3, &[("title", "after")]);
    let error = fate(&server, &client, tx).expect_err("never uploads");
    assert!(
        error.message.contains("before it became a replica"),
        "{error:?}"
    );
}

/// A v1 write still pending when the client makes v2 (renaming `title`)
/// current uploads under v1, the version it was written under.
#[test]
fn a_pending_write_uploads_under_its_version_after_an_upgrade() {
    use jazz::protocol::LensOp;
    let renamed = || {
        compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("notes")
                        .column("heading", ColumnType::Text)
                        .policies(allow_all_policies()),
                )
                .build(),
        )
    };
    let rename = MigrationLens::new(
        v1().version_id(),
        renamed().version_id(),
        vec![TableLens {
            source_table: "notes".into(),
            target_table: "notes".into(),
            ops: vec![LensOp::RenameColumn {
                from: "title".into(),
                to: "heading".into(),
            }],
        }],
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let server = authority_at(v1(), &dir.path().join("server.sqlite"));
    block_on(server.admit_schema_with_lenses(renamed(), std::slice::from_ref(&rename))).unwrap();
    let client = open_at(v1(), 1, &dir.path().join("client.sqlite"));
    client.become_replica();
    let tx = insert(&client, 7, &[("title", "offline v1 write")]);
    block_on(client.activate_schema_with_lenses(renamed(), &[rename])).unwrap();
    let _link = link(&client, &server);
    fate(&server, &client, tx).expect("uploads under v1");
    assert_eq!(ids(&server), vec![7]);
    assert_eq!(ids(&client), vec![7]);
}

fn open_mem(schema: JazzSchema, node: u8) -> Db {
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

/// Tick tab, worker and core in turn.
fn pump3(core: &Db, worker: &Db, tab: &Db) {
    for _ in 0..6 {
        block_on(tab.tick()).unwrap();
        block_on(worker.tick()).unwrap();
        block_on(core.tick()).unwrap();
        block_on(worker.tick()).unwrap();
        block_on(tab.tick()).unwrap();
    }
}

/// A tab on v1 relayed by `worker`, with a v2 view.
fn tab_on(worker: &Db) -> (Db, Db, jazz::db::Upstream) {
    let tab = open_mem(v1(), 0xa1);
    tab.set_non_durable_client();
    tab.set_remote_link_hint(RemoteLinkHint::Live);
    let link = link(&tab, worker);
    block_on(tab.admit_schema_with_lenses(v2(), &[lens()])).unwrap();
    let ahead = block_on(tab.register_schema_view(v2())).unwrap();
    (tab, ahead, link)
}

/// A worker that mirrored v2 in an earlier page load and reopens under v1
/// holds v2 again from its upstream: a tab's write through a v2 view
/// lands, and so does the plain write after it.
#[test]
fn a_reopened_worker_relays_a_view_write_its_core_has() {
    for reopened in [false, true] {
        relay_view_write(reopened);
    }
}

fn relay_view_write(reopened: bool) {
    let dir = tempfile::tempdir().unwrap();
    let core = authority_at(v1(), &dir.path().join("core.sqlite"));
    block_on(core.admit_schema_with_lenses(v2(), &[lens()])).unwrap();
    insert(&core, 1, &[("title", "core")]);
    let path = dir.path().join("worker.sqlite");
    if reopened {
        let worker = open_at(v1(), 0xa0, &path);
        let _link = link(&worker, &core);
        worker.set_remote_link_hint(RemoteLinkHint::Live);
        let q = worker.prepare_query(&Query::from("notes")).unwrap();
        let _sub = block_on(worker.subscribe(&q, ReadOpts::default())).unwrap();
        pump(&core, &[&worker]);
    }
    let worker = open_at(v1(), 0xa0, &path);
    let _link = link(&worker, &core);
    worker.set_remote_link_hint(RemoteLinkHint::Live);
    let (tab, ahead, _tab_link) = tab_on(&worker);
    let q = tab.prepare_query(&Query::from("notes")).unwrap();
    let _sub = block_on(tab.subscribe(&q, ReadOpts::default())).unwrap();
    pump3(&core, &worker, &tab);
    let view_write = insert(&ahead, 2, &[("title", "a"), ("tag", "t")]);
    let plain = insert(&tab, 3, &[("title", "b")]);
    fate_by(&tab, view_write, || pump3(&core, &worker, &tab)).expect("lands");
    fate_by(&tab, plain, || pump3(&core, &worker, &tab)).expect("lands");
    assert_eq!(ids(&core), vec![1, 2, 3]);
    let core_v2 = block_on(core.register_schema_view(v2())).unwrap();
    assert_eq!(tag(&core_v2, 2), some("t"), "reopened: {reopened}");
}

/// A tab's write through a v2 view, relayed by a worker whose core lacks
/// v2, is refused with the typed reason once the worker has its core's
/// catalogue; the plain write after it lands.
#[test]
fn a_relayed_view_write_its_core_lacks_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let core = authority_at(v1(), &dir.path().join("core.sqlite"));
    insert(&core, 1, &[("title", "core")]);
    let worker = open_at(v1(), 0xa0, &dir.path().join("worker.sqlite"));
    let _link = link(&worker, &core);
    worker.set_remote_link_hint(RemoteLinkHint::Live);
    let (tab, ahead, _tab_link) = tab_on(&worker);
    let q = tab.prepare_query(&Query::from("notes")).unwrap();
    let _sub = block_on(tab.subscribe(&q, ReadOpts::default())).unwrap();
    let view_write = insert(&ahead, 2, &[("title", "a"), ("tag", "t")]);
    let plain = insert(&tab, 3, &[("title", "b")]);
    let error = fate_by(&tab, view_write, || pump3(&core, &worker, &tab)).expect_err("refused");
    assert_eq!(
        error.reason,
        Some(Reason::SchemaVersionNotDeployed),
        "{error:?}"
    );
    fate_by(&tab, plain, || pump3(&core, &worker, &tab)).expect("lands");
    assert_eq!(ids(&core), vec![1, 3]);
    assert_eq!(ids(&tab), vec![1, 3]);
}

/// A worker that learned its core's catalogue, then went offline, does
/// not refuse a tab's write through a version the core deployed meanwhile:
/// it stays pending, and lands once the worker reconnects.
#[test]
fn an_offline_worker_keeps_a_view_write_pending() {
    let dir = tempfile::tempdir().unwrap();
    let core = authority_at(v1(), &dir.path().join("core.sqlite"));
    let worker = open_at(v1(), 0xa0, &dir.path().join("worker.sqlite"));
    worker.become_replica();
    let up = link(&worker, &core);
    worker.set_remote_link_hint(RemoteLinkHint::Live);
    let (tab, ahead, _tab_link) = tab_on(&worker);
    let q = tab.prepare_query(&Query::from("notes")).unwrap();
    let _sub = block_on(tab.subscribe(&q, ReadOpts::default())).unwrap();
    pump3(&core, &worker, &tab);
    assert!(worker.detach_connection(&up));
    block_on(core.activate_schema_with_lenses(v2(), &[lens()])).unwrap();
    let tx = insert(&ahead, 2, &[("title", "a"), ("tag", "t")]);
    {
        let mut wait = pin!(tab.wait_for_transaction(tx, DurabilityTier::Global));
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        for _ in 0..5 {
            assert!(wait.as_mut().poll(&mut cx).is_pending(), "decided offline");
            pump3(&core, &worker, &tab);
        }
    }
    let _up = link(&worker, &core);
    fate_by(&tab, tx, || pump3(&core, &worker, &tab)).expect("lands");
    assert_eq!(ids(&core), vec![2]);
    assert_eq!(tag(&core, 2), some("t"));
}
