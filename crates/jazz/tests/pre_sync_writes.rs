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
    SubscriptionEvent, SyncMessage, Transport, TransportError, block_on,
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
    let client = open_at(v2(), 1, &dir.path().join("client.sqlite"));
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

/// One commit uploads under one version: a replica refuses a transaction
/// staged through views of different versions when it commits, and keeps
/// none of it; one staged through a single view commits.
#[test]
fn a_replica_refuses_a_transaction_through_views_of_different_versions() {
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
    let error = block_on(client.commit_mergeable_handle(mixed)).unwrap_err();
    assert!(error.message.contains("not supported"), "{}", error.message);
    assert_eq!(ids(&client), Vec::<u8>::new());

    let single = OpenTransactionId::new();
    block_on(ahead.begin_mergeable(single)).unwrap();
    block_on(ahead.tx_ref(single).insert(
        "notes",
        cells(&[("title", "v2"), ("tag", "new")]),
        row(5),
    ))
    .unwrap();
    block_on(ahead.commit_mergeable_handle(single)).unwrap();
    block_on(server.activate_schema_with_lenses(v2(), &[lens()])).unwrap();
    pump(&server, &[&client]);
    assert_eq!(ids(&server), vec![5]);
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
    let mut wait = pin!(client.wait_for_transaction(tx, DurabilityTier::Global));
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    for _ in 0..20 {
        if let Poll::Ready(result) = wait.as_mut().poll(&mut cx) {
            return result.map(drop);
        }
        pump(server, &[client]);
    }
    panic!("transaction {tx} has no fate yet");
}

/// A `transaction` opened on the owner whose writes go through a view
/// uploads under the view's version; one mixing versions is refused.
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
    let error = mixed.map(drop).unwrap_err();
    assert!(error.message.contains("not supported"), "{}", error.message);
    assert_eq!(ids(&client), Vec::<u8>::new());

    block_on(client.transaction(async |_| {
        ahead
            .insert("notes", cells(&[("title", "v2"), ("tag", "new")]), row(5))
            .await?;
        Ok(())
    }))
    .unwrap();
    block_on(server.activate_schema_with_lenses(v2(), &[lens()])).unwrap();
    pump(&server, &[&client]);
    assert_eq!(ids(&client), vec![5]);
    assert_eq!(ids(&server), vec![5]);
    assert_eq!(tag(&server, 5), some("new"));
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
